{-# LANGUAGE DeriveGeneric  #-}
{-# LANGUAGE NamedFieldPuns #-}
module DDB.Store where
-- TODO: update this comment now that there are no other storage backends.
--
-- an attempt at a more efficient Store implementation.
--
-- The basic idea:
--
-- We store the blobs in a single, large file. New blobs are simply
-- appended to the end. Separately, we store an index mapping hashes
-- to (offset, size) pairs. During operation we load the index into
-- memory.
--
-- This *should* improve performance substantially over SimpleStore. The only
-- disk IO we're doing during a backup now is either at startup/shutdown, which
-- only happens once per run, or is a write() syscall, outputting a new blob.
-- We can determine if a blob is already present without hitting the disk,
-- since we load the index into main memory on startup.

import DDB.Types

import Codec.Compression.GZip (compress, decompress)
import Codec.Serialise        (Serialise, deserialise, serialise)
import Control.Exception      (catch, throwIO)
import Control.Monad          (unless, void)
import Data.Foldable          (traverse_)
import Data.IORef             (IORef, newIORef, readIORef, writeIORef)
import Data.Word              (Word64)
import GHC.Generics           (Generic)
import System.Directory       (createDirectoryIfMissing, listDirectory)
import System.IO
    ( Handle
    , IOMode(ReadWriteMode)
    , SeekMode(AbsoluteSeek, SeekFromEnd)
    , hClose
    , hSeek
    , openBinaryFile
    )
import System.IO.Error        (isDoesNotExistError)
import System.Posix.Files     (rename)

import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict  as M

type Index = M.HashMap Hash (Word64, Word64)

data MetaData = MetaData
    { offset :: !Word64
    -- ^ The offset of the end of the data. Storing this means that if we
    -- fail in the middle of a run, nothing is changed; we have some garbage at
    -- the end of the blob file, but we just truncate to the appropriate
    -- position on the next run.
    , index  :: Index
    -- ^ Index mapping hashes to ranges in the blob file.
    }
    deriving(Show, Eq, Generic)

instance Serialise MetaData

data Store = Store
    { handle    :: Handle
    -- ^ open handle to the blob file.
    , metadata  :: IORef MetaData
    -- ^ metadata about the store. We keep this in memory until we close
    -- the store, at which point we flush it to disk atomically.
    , storePath :: FilePath
    -- ^ The path to the directory containing the store.
    }


-- | @'saveBlock' index store block@ saves @block@ to the store. If the block
-- is already present, this is a no-op.
--
-- The @index@ parameter is an extra index to check for the blocl. If the block
-- is found in the index, it should not be copied to the store, regardless of
-- whether it is present there. This exists for use with the --third-leg option;
-- otherwise it will just be an empty map.
saveBlock :: Index -> Store -> HashedBlock -> IO ()
saveBlock
        extraIndex
        Store{handle, metadata}
        (HashedBlock digest (Block uncompressedBytes)) = do
    m@MetaData{offset, index} <- readIORef metadata
    let bytes = compress $ LBS.fromStrict uncompressedBytes
    unless (digest `M.member` extraIndex || digest `M.member` index) $ do
        LBS.hPut handle bytes
        writeIORef metadata $!
            m { offset = offset + fromIntegral (LBS.length bytes)
              , index = M.insert
                    digest
                    (offset, fromIntegral $ LBS.length bytes)
                    index
              }

-- @'loadBlock' store digest@ returns the block corresponding to the
-- given sha256 hash from the store.
loadBlock :: Store -> Hash -> IO Block
loadBlock Store{handle, metadata} digest = do
    MetaData{offset, index} <- readIORef metadata
    case M.lookup digest index of
        Nothing -> error "TODO: throw a proper exception"
        Just (blobOffset, blobSize) -> do
            -- TODO: it would be cleaner to just use pread,
            -- but the unix package doesn't wrap it; see:
            --
            -- https://github.com/haskell/unix/issues/105
            hSeek handle AbsoluteSeek (fromIntegral blobOffset)
            compressedBytes <- LBS.hGet handle (fromIntegral blobSize)
            let bytes = LBS.toStrict $ decompress compressedBytes
            hSeek handle AbsoluteSeek (fromIntegral offset)
            return $ Block bytes

-- | @'saveTag' store tagname ref@ saves @ref@ under the given tag.
saveTag :: Store -> String -> FileRef -> IO ()
saveTag Store{storePath} tagname ref =
    LBS.writeFile (tagPath storePath tagname) (serialise ref)

-- | @'loadTag' store tagname@ loads the FileRef for the given tag.
loadTag :: Store -> String -> IO FileRef
loadTag Store{storePath} tagname =
    deserialise <$> LBS.readFile (tagPath storePath tagname)

-- | Close the store, flushing the metadata to disk.
closeStore :: Store -> IO ()
closeStore Store{storePath, handle, metadata} = do
    m <- readIORef metadata
    hClose handle
    atomicWriteFile (metadataPath storePath) (serialise m)

-- | Open the store located at the given path, creating it if needed.
openStore :: FilePath -> IO Store
openStore storePath = do
    createDirectoryIfMissing True $ storePath ++ "/tags"
    handle <- openBinaryFile (blobsPath storePath) ReadWriteMode
    metadata <- loadMetadata
    -- TODO: truncate to indicated size and seek there.
    hSeek handle SeekFromEnd 0
    return Store{handle, metadata, storePath}
  where
    loadMetadata = do
        value <- (deserialise <$> LBS.readFile (metadataPath storePath))
            `catch`
            ((\e -> do
                unless (isDoesNotExistError e) $ throwIO e
                return MetaData
                        { offset = 0
                        , index = M.empty
                        }) :: IOError -> IO MetaData)
        newIORef value


-- | @'mergeStore' src dest@ adds all of the blocks and tags from src to dest.
-- If any of the tags already exist, they are overwritten. TODO: this is not
-- the best behavior; come up with and implement something better.
mergeStore :: Store -> Store -> IO ()
mergeStore src dest = do
    mergeBlocks src dest
    mergeTags src dest

mergeBlocks :: Store -> Store -> IO ()
mergeBlocks src@Store{metadata=srcMeta} dest = do
    srcIndex <- index <$> readIORef srcMeta
    void $ M.traverseWithKey
        (\hash _ -> do
            block <- loadBlock src hash
            saveBlock M.empty dest HashedBlock
                { blockDigest = hash
                , blockBytes = block
                }
        )
        srcIndex

mergeTags :: Store -> Store -> IO ()
mergeTags src dest = do
    tags <- listTags src
    traverse_
        (\tag -> do
            ref <- loadTag src tag
            saveTag dest tag ref)
        tags

listTags :: Store -> IO [String]
listTags Store{storePath} =
    listDirectory (storePath ++ "/tags")

tagPath :: FilePath -> String -> FilePath
tagPath storePath tagname = storePath ++ "/tags/" ++ tagname

metadataPath, blobsPath :: FilePath -> FilePath
metadataPath = (++ "/metadata.cbor")
blobsPath = (++ "/blobs-sha256")

-- | @'atomicWriteFile' path bytes@ writes @bytes@ to the file @path@,
-- atomically. It does so by writing to a different file, then using
-- 'rename' to move it.
atomicWriteFile :: FilePath -> LBS.ByteString -> IO ()
atomicWriteFile path bytes = do
    LBS.writeFile (path ++ ".new") bytes
    rename (path ++ ".new") path

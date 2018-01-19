{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE RecordWildCards #-}
module DDB.NewStore where
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
--
-- TODO: come up with a better name than NewStore.

import DDB.Types

import Codec.Serialise    (Serialise, deserialise, serialise)
import Control.Exception  (catch)
import Control.Monad      (unless)
import Data.IORef         (IORef, newIORef, readIORef, writeIORef)
import Data.Word          (Word64)
import GHC.Generics       (Generic)
import System.IO
    ( Handle
    , IOMode(ReadWriteMode)
    , SeekMode(AbsoluteSeek, SeekFromEnd)
    , hClose
    , hSeek
    , openBinaryFile
    )
import System.Posix.Files (rename)

import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict  as M

data MetaData = MetaData
    { offset :: Word64
    -- ^ The offset of the end of the data. Storing this means that if we
    -- fail in the middle of a run, nothing is changed; we have some garbage at
    -- the end of the blob file, but we just truncate to the appropriate
    -- position on the next run.
    , index  :: M.HashMap Hash (Word64, Word64)
    -- ^ Index mapping hashes to ranges in the blob file.
    }
    deriving(Show, Eq, Generic)

instance Serialise MetaData

data NewStore = NewStore
    { handle    :: Handle
    -- ^ open handle to the blob file.
    , metadata  :: IORef MetaData
    -- ^ metadata about the store. We keep this in memory until we close
    -- the store, at which point we flush it to disk atomically.
    , storePath :: FilePath
    -- ^ The path to the directory containing the store.
    }

instance Store NewStore where
    saveBlock NewStore{..} (HashedBlock digest (Block bytes)) = do
        m@MetaData{..} <- readIORef metadata
        unless (digest `M.member` index) $ do
            B.hPut handle bytes
            writeIORef metadata $
                m { offset = offset + fromIntegral (B.length bytes)
                  , index = M.insert
                        digest
                        (offset, fromIntegral $ B.length bytes)
                        index
                  }

    loadBlock NewStore{..} digest = do
        MetaData{..} <- readIORef metadata
        case M.lookup digest index of
            Nothing -> error "TODO: throw a proper exception"
            Just (blobOffset, blobSize) -> do
                -- TODO: it would be cleaner to just use pread,
                -- but the unix package doesn't wrap it; see:
                --
                -- https://github.com/haskell/unix/issues/105
                hSeek handle AbsoluteSeek (fromIntegral blobOffset)
                bytes <- B.hGet handle (fromIntegral blobSize)
                hSeek handle AbsoluteSeek (fromIntegral offset)
                return $ Block bytes

    saveTag NewStore{..} tagname ref =
        LBS.writeFile (tagPath storePath tagname) (serialise ref)
    loadTag NewStore{..} tagname =
        deserialise <$> LBS.readFile (tagPath storePath tagname)

tagPath :: FilePath -> String -> FilePath
tagPath storePath tagname = storePath ++ "/tags/" ++ tagname

indexPath, blobsPath :: FilePath -> FilePath
indexPath = (++ "/index")
blobsPath = (++ "/blobs-sha256")

openStore :: FilePath -> IO NewStore
openStore storePath = do
    handle <- openBinaryFile (blobsPath storePath) ReadWriteMode
    metadata <- loadMetadata
    -- TODO: truncate to indicated size and seek there.
    hSeek handle SeekFromEnd 0
    return NewStore{..}
  where
    loadMetadata = do
        value <- (deserialise <$> LBS.readFile (indexPath storePath))
            -- TODO: check the type of error:
            `catch`
            ((\_ -> return MetaData
                        { offset = 0
                        , index = M.empty
                        }) :: IOError -> IO MetaData)
        newIORef value

-- | Close the store, flushing the metadata to disk.
closeStore :: NewStore -> IO ()
closeStore NewStore{..} = do
    m <- readIORef metadata
    hClose handle
    atomicWriteFile (indexPath storePath) (serialise m)

-- | @'atomicWriteFile path bytes@ writes @bytes@ to the file @path@,
-- atomically. It does so by writing to a different file, then using
-- 'rename' to move it.
atomicWriteFile :: FilePath -> LBS.ByteString -> IO ()
atomicWriteFile path bytes = do
    LBS.writeFile (path ++ ".new") bytes
    rename (path ++ ".new") path

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

import Control.Monad (unless)
import Data.Word     (Word64)
import System.IO     (Handle, ReadWriteMode, SeekFromEnd, hSeek)

import qualified Data.ByteString     as B
import qualified Data.HashMap.Strict as M

data MetaData = MetaData
    { offset :: Word64
    -- ^ The offset of the end of the data. This storing this means that if we
    -- fail in the middle of a run, nothing is changed; we have some garbage at
    -- the end of the blob file, but we just truncate to the appropriate
    -- position on the next run.
    , index  :: M.Map B.ByteString (Word64, Word64)
    -- ^ Index mapping hashes to ranges in the blob file.
    }

data NewStore = NewStore
    { handle    :: Handle
    -- ^ open handle to the blob file.
    , metadata  :: IORef MetaData
    -- ^ metadata about the store. we keep this in memory until we close
    -- the store, at which point we flush it to disk atomically.
    , storePath :: FilePath
    -- ^ The path to the directory containing the store.
    }

instance Store NewStore where
    saveBlock s@NewStore{..} (HashedBlock digest bytes) = do
        m@MetaData{..} <- readIORef metadata
        unless (index `M.contains` digest) $ do
            B.hPut handle bytes
            writeIORef metadata $
                m { offset = offset + B.length bytes
                  , index = M.insert digest (offset, B.length bytes)
                  }

    loadBlock s@NewStore{..} (Hash digest) = do
        m@MetaData{..} <- readIORef metadata
        case M.lookup digest index of
            Nothing -> error "TODO: throw a proper exception"
            Just (blobOffset, blobSize) -> do
                -- TODO: it would be cleaner to just use pread,
                -- but the unix package doesn't wrap it; see:
                --
                -- https://github.com/haskell/unix/issues/105
                hSeek handle AbsoluteSeek blobOffset
                bytes <- hGet handle blobSize
                hSeek handle AbsoluteSeek offset
                return bytes

    -- TODO: tags. We factor out the implementation from SimpleStore; there's
    -- no reason not to re-use it.


indexPath, blobsPath :: FilePath -> FilePath
indexPath = (++ "/index")
blobsPath = (++ "/blobs-sha256")

openStore :: FilePath -> IO NewStore
openStore storePath = do
    handle <- openBinaryFile (blobsPath storePath) ReadWriteMode
    metadata <- newIORef <$> (deserialise <$> B.readFile (indexPath storePath))
        -- TODO: check the type of error:
        `catch` (\_ -> MetaData { offset = 0, index = M.empty })
    -- TODO: truncate to indicated size and seek there.
    hSeek handle SeekFromEnd 0
    return NewStore{..}

-- | Close the store, flushing the index to disk.
closeStore :: NewStore -> IO ()
closeStore NewStore{..} = do
    hClose handle
    atomicWriteFile (indexPath storePath) (serialise index)

-- | @'atomicWriteFile path bytes@ writes @bytes@ to the file @path@,
-- atomically. It does so by writing to a different file, then using
-- 'rename' to move it.
atomicWriteFile :: FilePath -> B.ByteString -> IO ()
atomicWriteFile path bytes = do
    B.writeFile (path ++ ".new") bytes
    rename (path ++ ".new") path

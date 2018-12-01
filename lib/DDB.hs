-- | Beginnings of a next-gen version of https://github.com/zenhack/dedup-backup.
--
-- The big difference is that this tool does (will do) de-duplication at the
-- block level, to better handle large files with frequent small changes.
--
-- This has the downside that we can no longer just use hard links and have the
-- user explore old backups via usual filesystem access, so we'll have to
-- specifically provide access mecahnisms, as well as worrying about directory
-- representation, and how to aggregate files.
--
-- We don't have a working version of the tool yet, but bits of the code below
-- do what we need.
--
-- Terminology:
--
-- * The @store@ is a directory under which the backup system's data is stored.
-- * A @block@ is a physically contiguous chunk of bytes to be stored in
--   one-piece.
-- * A @blob@ is a logically contiguous sequence of bytes (e.g. the contents of
--   a file), which may be stored in one or more blocks.
--
-- A blob is stored as a tree of blocks, where the leaves are the logical bytes
-- of the blob itself, and the interior nodes are the concatenation of the
-- sha256 hashes of their children.
--
-- The blocks do not store any other metadata; reading back a blob requires
-- knowing the hash of the root block of its tree, and the depth of the tree.
{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE Rank2Types      #-}
{-# LANGUAGE RecordWildCards #-}
module DDB where

import qualified Crypto.Hash.SHA256 as SHA256

import Conduit
import DDB.Store
import DDB.Types

import Codec.Serialise
    (IDecode(..), Serialise, deserialiseIncremental, serialise)
import Control.Monad         (forM_, when)
import Control.Monad.Catch   (bracket, throwM)
import Control.Monad.ST      (stToIO)
import Data.Monoid           ((<>))
import Foreign.C.Types       (CTime(..))
import System.Directory      (createDirectoryIfMissing, listDirectory)
import System.FilePath.Posix (takeFileName)
import System.IO
    (Handle, IOMode(..), hClose, hPutStrLn, openBinaryFile, stderr)
import System.Posix.Types    (CGid(..), CMode(..), CUid(..))

import qualified Data.ByteString         as B
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8   as B8
import qualified Data.ByteString.Lazy    as LBS
import qualified System.Posix.Files      as P

-- | A HashedBlock for the zero-length block.
zeroBlock :: HashedBlock
zeroBlock = hash (Block B.empty)

status2Meta :: P.FileStatus -> Metadata
status2Meta status = Metadata{..} where
    CMode metaMode = P.fileMode status
    CTime metaModTime = P.modificationTime status
    CTime metaAccessTime = P.accessTime status
    CUid metaOwner = P.fileOwner status
    CGid metaGroup = P.fileGroup status

-- | The maximum block size to store.
blockSize :: Integral a => a
blockSize = 2 * 1024 * 1024

-- | The size of a hash in bytes.
hashSize :: Integral a => a
hashSize = 256 `div` 8

-- | Compute the hash of a block.
hash :: Block -> HashedBlock
hash block@(Block bytes) = HashedBlock (Hash (SHA256.hash bytes)) block

decodeStreaming :: Serialise a => ConduitM B.ByteString a IO ()
decodeStreaming = decodeIO >>= go
  where
    decodeIO = liftIO $ stToIO $ deserialiseIncremental
    go (Partial resume) = do
        next <- await
        idecode <- liftIO $ stToIO $ resume next
        go idecode
    go (Done rest _ val) = do
        yield val
        (yield rest >> mapC id) .| (decodeIO >>= go)
    -- Just the end of the stream:
    go (Fail rest 0 _) | B.null rest = return ()
    -- An actual failure:
    go (Fail _ _ exn) = throwM exn

-- | Read bytestrings from the handle in chunks of size 'blockSize'.
--
-- TODO: there's probably a library function that does this; we should
-- look for for that and use it instead.
hBlocks :: Handle -> Source IO Block
hBlocks handle = do
    bytes <- lift $ B.hGet handle blockSize
    when (bytes /= B.empty) $ do
        yield (Block bytes)
        hBlocks handle

-- | @'saveBlob' store handle@ saves all blocks read from the handle
-- to the store.
--
-- Returns a BlobRef referencing the blob.
saveBlob :: Store -> Handle -> IO BlobRef
saveBlob store h = runConduit $ hBlocks h .| buildTree store

-- Load a blob from disk.
loadBlob :: Store -> BlobRef -> Producer IO B.ByteString
loadBlob store (BlobRef 1 digest) = do
    Block bytes <- lift $ loadBlock store digest
    yield bytes
loadBlob store (BlobRef n digest) = do
    Block bytes <- lift $ loadBlock store digest
    forM_ (chunk bytes) $ \digest' -> do
        loadBlob store (BlobRef (n-1) (Hash digest'))
  where
    chunk bs
        | bs == B.empty = []
        | otherwise = B.take hashSize bs : chunk (B.drop hashSize bs)

-- | Pack incoming ByteStrings into as few blocks as possible.
-- Note that this will not split values, only coalesce them.
collectBlocks :: Monad m => ConduitM B.ByteString Block m ()
collectBlocks = go 0 mempty where
    go bufSize buf = do
        chunk <- await
        case chunk of
            Nothing -> yieldBlock buf
            Just bytes ->
                let newSize = B.length bytes + bufSize
                in if newSize <= blockSize
                    then
                        go newSize (buf <> Builder.byteString bytes)
                    else do
                        yieldBlock buf
                        go (B.length bytes) (Builder.byteString bytes)
    yieldBlock = yield . Block . LBS.toStrict . Builder.toLazyByteString

-- | Save all blocks in the stream to the store, and pass them along.
saveBlocks :: Store -> ConduitM HashedBlock HashedBlock IO ()
saveBlocks store = iterMC (saveBlock store)

-- | 'buildTree' builds the tree for a blob consisting of the incoming (leaf)
-- blocks. Returns a BlobRef to the root of the tree.
buildTree :: Store -> Consumer Block IO BlobRef
buildTree store = mapC hash .| saveBlocks store .| go 1
  where
    go n = do
        item1 <- await
        item2 <- await
        case (item1, item2) of
            (Just block, Nothing) ->
                return $ BlobRef n (blockDigest block)
            (Nothing, _) ->
                return $ BlobRef n (blockDigest zeroBlock)
            (Just block1, Just block2) -> do
                (yieldMany [block1, block2] >> mapC id)
                .| buildNodes
                .| saveBlocks store
                .| go (n+1)

-- | Build interior nodes from the incoming stream. The hashes of the incoming
-- blocks are accumulated into interior nodes, and the blocks for the interior
-- nodes are emitted.
--
-- Note that this only generates one layer of nodes; see 'buildTree'.
buildNodes :: Monad m => ConduitM HashedBlock HashedBlock m ()
buildNodes
    = mapC (\(HashedBlock (Hash digest) _) -> digest)
    .| collectBlocks
    .| mapC hash

storeFile :: Store -> FilePath -> IO (Either UnsupportedFileType FileRef)
storeFile store filename = do
    status <- P.getSymbolicLinkStatus filename
    let meta = status2Meta status
    if P.isRegularFile status then
        bracket
            (openBinaryFile filename ReadMode)
            hClose
            (\h -> Right . RegFile meta <$> saveBlob store h)
    else if P.isSymbolicLink status then do
        target <- P.readSymbolicLink filename
        return $ Right $ SymLink (B8.pack target)
    else if P.isDirectory status then do
        files <- listDirectory filename
        blobRef <- runConduit $
            yieldMany files
            .| mapMC (\name -> do
                ret <- storeFile store (filename ++ "/" ++ name)
                case ret of
                    Left (UnsupportedFileType typeName) -> do
                        hPutStrLn stderr $ "Warning: unsupported file type; skipping."
                        return $ Left $ UnsupportedFileType typeName
                    Right fileRef ->
                        return $ Right DirEnt
                            { entName = B8.pack $ takeFileName name
                            , entRef = fileRef
                            })
            .| copyRights
            .| mapC (LBS.toStrict . serialise)
            .| collectBlocks
            .| buildTree store
        return $ Right $ Dir meta blobRef
    else
        -- TODO: separate out individual file types and give each a name.
        return $ Left (UnsupportedFileType "unknown")

copyRights :: Monad m => ConduitM (Either e v) v m ()
copyRights = await >>= \case
    Nothing ->
        pure ()
    Just (Left _) ->
        copyRights
    Just (Right v) -> do
        yield v
        copyRights

makeSnapshot :: Store -> FilePath -> String -> IO ()
makeSnapshot store path tagname =
    storeFile store path >>= \case
        Left (UnsupportedFileType typ) ->
            throwM $ userError $ "Unknown file type: " <> typ
        Right ref ->
            saveTag store tagname ref

restoreSnapshot :: Store -> String -> FilePath -> IO ()
restoreSnapshot store tagname path = do
    ref <- loadTag store tagname
    extractFile store ref path

extractFile :: Store -> FileRef -> FilePath -> IO ()
extractFile store ref path = case ref of
    RegFile meta blobRef -> do
        bracket
            (openBinaryFile path WriteMode)
            hClose
            (\h -> runConduit $ loadBlob store blobRef .| mapM_C (B.hPut h))
        setMeta meta path
    SymLink target ->
        P.createSymbolicLink (B8.unpack target) path
    Dir meta blobRef -> do
        createDirectoryIfMissing False path
        runConduit $
            loadBlob store blobRef
            .| decodeStreaming
            .| mapM_C (extractDirEnt path)
        setMeta meta path
  where
    extractDirEnt dirPath ent = extractFile store
        (entRef ent)
        (dirPath ++ "/" ++ B8.unpack (entName ent))

setMeta :: Metadata -> FilePath -> IO ()
setMeta Metadata{..} path = do
    P.setFileMode path (CMode metaMode)
    P.setFileTimes path
        (CTime metaAccessTime)
        (CTime metaModTime)

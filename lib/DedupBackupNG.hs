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
{-# LANGUAGE Rank2Types      #-}
{-# LANGUAGE RecordWildCards #-}
module DedupBackupNG where

import qualified Crypto.Hash.SHA256 as SHA256

import Conduit


import Codec.Serialise
    (IDecode(..), Serialise, deserialise, deserialiseIncremental, serialise)
import Control.Monad         (forM_, unless, when)
import Control.Monad.Catch   (bracket, catch, throwM)
import Control.Monad.ST      (stToIO)
import Data.Int              (Int64)
import Data.Monoid           ((<>))
import Data.Word             (Word32)
import Foreign.C.Types       (CTime(..))
import GHC.Generics          (Generic)
import System.Directory      (createDirectoryIfMissing, listDirectory)
import System.FilePath.Posix (takeFileName)
import System.IO             (Handle, IOMode(..), hClose, openBinaryFile)
import System.IO.Error       (isAlreadyExistsError)
import System.Posix.Types    (CGid(..), CMode(..), CUid(..))
import Text.Printf           (printf)

import qualified Data.ByteString         as B
import qualified Data.ByteString.Base16  as Base16
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8   as B8
import qualified Data.ByteString.Lazy    as LBS
import qualified System.Posix.Files      as P
import qualified System.Posix.IO         as P

-- | newtype wrapper around a disk/storage block.
newtype Block = Block B.ByteString
    deriving(Show, Eq, Generic)

-- | Wrapper around sha256 digests.
newtype Hash = Hash B.ByteString
    deriving(Show, Eq, Generic)

instance Serialise Hash

-- | Info about a storage location.
newtype Store = Store FilePath
    deriving(Show, Eq)

-- | A reference to a blob. This includes all information necessary to read the
-- blob, not counting the location of the store.
data BlobRef = BlobRef !Int64 !Hash
    deriving(Show, Eq, Generic)

instance Serialise BlobRef

-- | A block together with its hash.
data HashedBlock = HashedBlock
    { blockDigest :: !Hash
    , blockBytes  :: !Block
    }

-- | A HashedBlock for the zero-length block.
zeroBlock :: HashedBlock
zeroBlock = hash (Block B.empty)

-- | A reference to a file in the store.
data FileRef
    = RegFile !Metadata !BlobRef
    | SymLink !B8.ByteString -- target of the link.
    | Dir !Metadata !BlobRef
    deriving(Show, Eq, Generic)

instance Serialise FileRef

data Metadata = Metadata
    { metaMode       :: !Word32
    , metaModTime    :: !Int64
    , metaAccessTime :: !Int64
    , metaOwner      :: !Word32
    , metaGroup      :: !Word32
    } deriving(Show, Eq, Generic)

status2Meta :: P.FileStatus -> Metadata
status2Meta status = Metadata{..} where
    CMode metaMode = P.fileMode status
    CTime metaModTime = P.modificationTime status
    CTime metaAccessTime = P.accessTime status
    CUid metaOwner = P.fileOwner status
    CGid metaGroup = P.fileGroup status

instance Serialise Metadata

-- | A directory entry. The 'Dir' variant of 'FileRef' points to a blob whose
-- contents are a sequence of these.
data DirEnt = DirEnt
    { entName :: !B8.ByteString -- file name
    , entRef  :: !FileRef
    } deriving(Show, Eq, Generic)

instance Serialise DirEnt

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

-- | @'blockPath' store digest@ is the file name in which the block with
-- sha256 hash @digest@ should be saved within the given store.
blockPath :: Store -> Hash -> FilePath
blockPath (Store storePath) (Hash digest) =
    let hashname@(c1:c2:_) = B8.unpack $ Base16.encode digest
    in storePath ++ "/sha256/" ++ [c1,c2] ++ "/" ++ hashname

-- | @'saveBlock' store block@ Saves @block@ to the store. If the block
-- is already present, this is a no-op.
saveBlock :: Store -> HashedBlock -> IO ()
saveBlock store (HashedBlock digest (Block bytes)) =
    saveFile (blockPath store digest) bytes

-- @'loadBlock@ store digest@ returns the block corresponding to the
-- given sha256 hash from @store@.
loadBlock :: Store -> Hash -> IO Block
loadBlock store digest = Block <$> B.readFile (blockPath store digest)

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

-- Save the provided ByteString to the named file, if the file does
-- not already exist. If it does exist, this is a no-op.
saveFile :: FilePath -> B.ByteString -> IO ()
saveFile filename bytes =
    bracket
        createExclusive
        hClose
        (\h -> B.hPut h bytes)
    `catch`
        (\e -> unless (isAlreadyExistsError e) $ throwM e)
  where
    createExclusive = P.fdToHandle =<< P.openFd
            filename
            P.WriteOnly
            (Just 0o600)
            P.defaultFileFlags { P.exclusive = True }


storeFile :: Store -> FilePath -> IO FileRef
storeFile store filename = do
    status <- P.getSymbolicLinkStatus filename
    let meta = status2Meta status
    if P.isRegularFile status then
        bracket
            (openBinaryFile filename ReadMode)
            hClose
            (\h -> RegFile meta <$> saveBlob store h)
    else if P.isSymbolicLink status then do
        target <- P.readSymbolicLink filename
        return (SymLink $ B8.pack target)
    else if P.isDirectory status then do
        files <- listDirectory filename
        blobRef <- runConduit $
            yieldMany files
            .| mapMC (\name -> do
                fileRef <- storeFile store (filename ++ "/" ++ name)
                return $ DirEnt
                    { entName = B8.pack $ takeFileName name
                    , entRef = fileRef
                    })
            .| mapC (LBS.toStrict . serialise)
            .| collectBlocks
            .| buildTree store
        return $ Dir meta blobRef
    else
        throwM $ userError "Unsupported file type."

tagPath :: Store -> String -> FilePath
tagPath (Store storePath) tagname = storePath ++ "/tags/" ++ tagname

makeSnapshot :: Store -> FilePath -> String -> IO ()
makeSnapshot store path tagname = do
    ref <- storeFile store path
    LBS.writeFile (tagPath store tagname) (serialise ref)

restoreSnapshot :: Store -> String -> FilePath -> IO ()
restoreSnapshot store tagname path = do
    refBytes <- LBS.readFile $ tagPath store tagname
    extractFile store (deserialise refBytes) path

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

-- | @'initStore' dir@ creates the directory structure necessary for
-- storage in the directory @dir@. It returns a refernce to the Store.
initStore :: FilePath -> IO Store
initStore dir = do
    forM_ [0,1..0xff] $ \n -> do
        createDirectoryIfMissing True $ printf "%s/sha256/%02x" dir (n :: Int)
    createDirectoryIfMissing True $ dir ++ "/tags"
    let store = Store dir
    saveBlock store zeroBlock
    return store

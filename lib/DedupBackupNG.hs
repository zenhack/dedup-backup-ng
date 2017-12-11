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
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE Rank2Types      #-}
module DedupBackupNG where

import qualified Crypto.Hash.SHA256 as SHA256

import Conduit
import Control.Exception      (bracket, catch, throwIO)
import Control.Monad          (forM_, unless, when)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Int               (Int64)
import Data.Monoid            ((<>))
import System.Directory       (createDirectoryIfMissing)
import System.IO              (Handle, IOMode(ReadMode), hClose, openBinaryFile)
import System.IO.Error        (isAlreadyExistsError)
import Text.Printf            (printf)

import qualified Data.ByteString         as B
import qualified Data.ByteString.Base16  as Base16
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8   as B8
import qualified Data.ByteString.Lazy    as LBS
import qualified System.Posix.Files      as P
import qualified System.Posix.IO         as P
import qualified System.Posix.Types      as P

-- | Wrapper around sha256 digests; just introduces a bit of type safety.
newtype Hash = Hash B.ByteString
    deriving(Show, Eq)

-- | Info about a storage location.
newtype Store = Store FilePath
    deriving(Show, Eq)

-- | A reference to a blob. This includes all information necessary to read the
-- blob, not counting the location of the store.
data BlobRef = BlobRef !Int64 !Hash
    deriving(Show, Eq)

-- | A block together with its hash.
data HashedBlock = HashedBlock !Hash !B.ByteString

-- | A reference to a file in the store.
data FileRef = RegFile !BlobRef

-- | The maximum block size to store.
blockSize :: Integral a => a
blockSize = 4096

-- | The size of a hash in bytes.
hashSize :: Integral a => a
hashSize = 256 `div` 8

-- | Compute the hash of a block.
hash :: B.ByteString -> HashedBlock
hash block = HashedBlock (Hash (SHA256.hash block)) block

-- | Save the provided ByteString to the named file, if the file does
-- not already exist. If it does exist, this is a no-op.
saveFile :: FilePath -> B.ByteString -> IO ()
saveFile filename bytes =
    bracket
        createExclusive
        hClose
        (\h -> B.hPut h bytes)
    `catch`
        (\e -> unless (isAlreadyExistsError e) $ throwIO e)
  where
    createExclusive = P.fdToHandle =<< P.openFd
        filename
        P.WriteOnly
        (Just 0o600)
        P.defaultFileFlags { P.exclusive = True }

-- | @'blockPath' store digest@ is the file name in which the block with
-- sha256 hash @digest@ should be saved within the given store.
blockPath :: Store -> Hash -> FilePath
blockPath (Store storePath) (Hash digest) =
    let hashname@(c1:c2:_) = B8.unpack $ Base16.encode digest
    in storePath ++ "/sha256/" ++ [c1,c2] ++ "/" ++ hashname

-- | @'saveBlock' store block@ Saves @block@ to the store. If the block
-- is already present, this is a no-op.
saveBlock :: MonadIO m => Store -> HashedBlock -> m ()
saveBlock store (HashedBlock digest bytes) =
    liftIO $ saveFile (blockPath store digest) bytes

-- @'loadBlock@ store digest@ returns the block corresponding to the
-- given sha256 hash from @store@.
loadBlock :: Store -> Hash -> IO B.ByteString
loadBlock store digest = B.readFile (blockPath store digest)

-- | Read bytestrings from the handle in chunks of size 'blockSize'.
--
-- TODO: there's probably a library function that does this; we should
-- look for for that and use it instead.
hBlocks :: MonadIO m => Handle -> Source m B.ByteString
hBlocks handle = do
    block <- liftIO $ B.hGet handle blockSize
    when (block /= B.empty) $ do
        yield block
        hBlocks handle

-- | @'saveBlob' store size handle@ saves all blocks read from the handle
-- to the store, expecting @size@ bytes total. 'saveBlob' will not read
-- more blocks than indicated by @size@ (though it may read past the exact
-- byte index, if there are more bytes available in the same block).
--
-- Returns a BlobRef referencing the blob.
--
-- TODO: somehow issue a warning if the blob size is shorter than the
-- available bytes.
saveBlob :: Store -> Int64 -> Handle -> IO BlobRef
saveBlob store size h = do
    let treeDepth = depthForSize size
        numBlocks = blocksForSize size
    Just (HashedBlock digest _) <- runConduit $
        hBlocks h .|
        takeC numBlocks .|
        mapC hash .|
        buildTree store treeDepth .|
        lastC
    return $ BlobRef treeDepth digest

-- Load a blob from disk.
loadBlob :: MonadIO m => Store -> BlobRef -> Producer m B.ByteString
loadBlob store (BlobRef 1 digest) =
    liftIO (loadBlock store digest) >>= yield
loadBlob store (BlobRef n digest) = do
    block <- liftIO $ loadBlock store digest
    forM_ (chunk block) $ \digest' -> do
        loadBlob store (BlobRef (n-1) (Hash digest'))
  where
    chunk bs
        | bs == B.empty = []
        | otherwise = B.take hashSize bs : chunk (B.drop hashSize bs)

-- | Save all blocks in the stream to the store, and pass them along.
saveBlocks :: MonadIO m => Store -> ConduitM HashedBlock HashedBlock m ()
saveBlocks store = iterMC (saveBlock store)

-- | @'depthForSize' n@ computes the depth of a tree for a blob of size @n@.
depthForSize :: Int64 -> Int64
depthForSize 0 = 0
depthForSize n
    | n <= blockSize = 1
    | otherwise = 1 + depthForSize (n `div` branchFactor)
  where
    branchFactor = blockSize `div` hashSize

blocksForSize :: Int64 -> Int
blocksForSize size = fromIntegral $
    let wholeBlocks = size `div` blockSize
        haveRemainder = size `mod` blockSize /= 0
    in if haveRemainder
        then wholeBlocks + 1
        else wholeBlocks

-- | @'buildTree' size@ builds the tree for a blob of size @size@ bytes. It
-- expects the raw (leaf) blocks of the blob to be its input, and yields the tree
-- in post-order (i.e. each interior node is yielded after its children).
buildTree :: MonadIO m => Store -> Int64 -> ConduitM HashedBlock HashedBlock m ()
-- We should only hit the first case if the blob is empty; otherwise we won't
-- recurse down that far.
buildTree store 0 = yield (hash $ B8.pack "") .| saveBlocks store
buildTree store 1 = saveBlocks store
buildTree store n = saveBlocks store .| buildNodes .| buildTree store (n-1)


-- | Build interior nodes from the incoming stream. The incoming blocks are
-- copied to the output, and interior nodes are injected after their contents.
--
-- Note that this only generates one layer of nodes; see 'buildTree'.
buildNodes :: Monad m => ConduitM HashedBlock HashedBlock m ()
buildNodes = go 0 mempty where
    go :: Monad m => Int64 -> Builder.Builder -> ConduitM HashedBlock HashedBlock m ()
    go bufSize buf
        | (bufSize + hashSize) > blockSize = do
            -- adding another hash would overflow the block; yield what we've
            -- got and start the next block.
            yield $ buildHashes buf
            go 0 mempty
        | otherwise = do
            item <- await
            case item of
                Just (HashedBlock (Hash digest) _) -> do
                    go (bufSize + hashSize) (buf <> Builder.byteString digest)
                Nothing ->
                    yield $ buildHashes buf
    buildHashes = hash . LBS.toStrict . Builder.toLazyByteString

storeFile :: Store -> FilePath -> IO FileRef
storeFile store filename = do
    status <- P.getSymbolicLinkStatus filename
    if P.isRegularFile status then do
        let P.COff size = P.fileSize status
        bracket
            (openBinaryFile filename ReadMode)
            hClose
            (\h -> RegFile <$> saveBlob store size h)
    else
        throwIO $ userError "Expected regular file"

extractFile :: Store -> BlobRef -> FilePath -> IO ()
extractFile store ref path = runConduitRes $
    loadBlob store ref .| sinkFileBS path

-- | @'initStore' dir@ creates the directory structure necessary for
-- storage in the directory @dir@. It returns a refernce to the Store.
initStore :: FilePath -> IO Store
initStore dir = do
    forM_ [0,1..0xff] $ \n -> do
        createDirectoryIfMissing True $ printf "%s/sha256/%02x" dir (n :: Int)
    return $ Store dir

-- | Placeholder for main, while we're still experimenting.
main :: IO ()
main = return ()

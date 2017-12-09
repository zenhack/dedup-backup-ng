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
module Main where

import qualified Crypto.Hash.SHA256 as SHA256

import Conduit
    (ConduitM, Source, await, mapC, mapM_C, runConduit, yield, (.|))
import Control.Exception   (bracket, catch, throwIO)
import Control.Monad       (forM_, unless, when)
import Control.Monad.Trans (lift)
import Data.Monoid         ((<>))
import System.Directory    (createDirectoryIfMissing)
import System.IO           (Handle, IOMode(ReadMode), hClose, openBinaryFile)
import System.IO.Error     (isAlreadyExistsError)
import Text.Printf         (printf)

import qualified Data.ByteString         as B
import qualified Data.ByteString.Base16  as Base16
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8   as B8
import qualified Data.ByteString.Lazy    as LBS
import qualified System.Posix.IO         as P

-- | Wrapper around sha256 digests; just introduces a bit of type safety.
newtype Hash = Hash B.ByteString
    deriving(Show, Eq)

-- | Info about a storage location.
newtype Store = Store FilePath
    deriving(Show, Eq)

-- | A block together with its hash.
data HashedBlock = HashedBlock !Hash !B.ByteString

-- | The maximum block size to store.
blockSize = 4096

-- | The size of a hash in bytes.
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

-- | @'blockPath' store digest'@ is the file name in which the block with
-- sha256 hash @digest@ should be saved within the given store.
blockPath :: Store -> Hash -> FilePath
blockPath (Store storePath) (Hash digest) =
    let hashname@(c1:c2:_) = B8.unpack $ Base16.encode digest
    in storePath ++ "/sha256/" ++ [c1,c2] ++ "/" ++ hashname

-- | @'saveBlock' store block@ Saves @block@ to the store. If the block
-- is already present, this is a no-op.
saveBlock :: Store -> HashedBlock -> IO ()
saveBlock store (HashedBlock digest bytes) =
    saveFile (blockPath store digest) bytes

-- @'loadBlock@ store digest@ returns the block corresponding to the
-- given sha256 hash from @store@.
loadBlock :: Store -> Hash -> IO B.ByteString
loadBlock store digest = B.readFile (blockPath store digest)

-- | Read bytestrings from the handle in chunks of size 'blockSize'.
--
-- TODO: there's probably a library function that does this; we should
-- look for for that and use it instead.
hBlocks :: Handle -> Source IO B.ByteString
hBlocks handle = do
    block <- lift $ B.hGet handle blockSize
    when (block /= B.empty) $ do
        yield block
        hBlocks handle

-- | Save all blocks read from the handle to the store. TODO: change this so
-- it also stores & returns enough info to reconstruct the blob.
saveBlob :: Store -> Handle -> IO ()
saveBlob store h = runConduit $
    hBlocks h .|
    mapC hash .|
    buildNodes .|
    mapM_C (saveBlock store)

-- | Build the interior nodes of our B-tree. The input stream should be the
-- leaf blocks of a blob, the output stream will be all of the blocks needed to
-- be stored.
--
-- TODO:
--
-- * describe the scheme in more detail
-- * find a way to report the hash of the tree's root.
-- * I suspect this doesn't work with trees of depth > 2.
buildNodes :: Monad m => ConduitM HashedBlock HashedBlock m ()
buildNodes = go 0 mempty where
    go :: Monad m => Int -> Builder.Builder -> ConduitM HashedBlock HashedBlock m ()
    go n hashes
        | (n + hashSize) > blockSize = do
            yield $ buildHashes hashes
            go 0 mempty
        | otherwise = do
            item <- await
            case item of
                Just block@(HashedBlock (Hash digest) bytes) -> do
                    yield block
                    go (n + hashSize) (hashes <> Builder.byteString digest)
                Nothing ->
                    when (n > 1) $
                        yield $ buildHashes hashes
    buildHashes = hash . LBS.toStrict . Builder.toLazyByteString

-- | Placeholder during experimentation. This stores each of the blocks of
-- the named file in the store at /tmp/bar.
doIt :: FilePath -> IO ()
doIt filename = bracket
    (openBinaryFile filename ReadMode)
    hClose
    (saveBlob $ Store "/tmp/bar")

-- | @'initStore' dir@ creates the directory structure necessary for
-- storage in the directory @dir@.
initStore :: FilePath -> IO ()
initStore dir = forM_ [0,1..0xff] $ \n -> do
    createDirectoryIfMissing True $ printf "%s/sha256/%02x" dir (n :: Int)

-- | Placeholder for main, while we're still experimenting.
main = return ()

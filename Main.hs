-- Beginnings of a next-gen version of https://github.com/zenhack/dedup-backup.
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
module Main where

import qualified Crypto.Hash.SHA256 as SHA256

import Conduit             (Source, mapC, mapM_C, runConduit, yield, (.|))
import Control.Exception   (bracket, catch)
import Control.Monad       (forM_, when)
import Control.Monad.Trans (lift)
import System.Directory    (createDirectoryIfMissing)
import System.IO           (Handle, IOMode(ReadMode), hClose, openBinaryFile)
import Text.Printf         (printf)

import qualified Data.ByteString        as B
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8  as B8
import qualified System.Posix.IO        as P
import qualified System.Posix.Types     as P

-- | Wrapper around sha256 digests; just introduces a bit of type safety.
newtype Hash = Hash B.ByteString
    deriving(Show, Eq)

-- | A block together with its hash.
data HashedBlock = HashedBlock !Hash !B.ByteString

-- | The maximum blob size to store.
blockSize = 4096

-- | Compute the hash of a block.
hash :: B.ByteString -> HashedBlock
hash block = HashedBlock (Hash (SHA256.hash block)) block

-- | Save the provided ByteString to the named file, if the file does
-- not already exist. If it does exist, this is a no-op.
saveFile :: FilePath -> B.ByteString -> IO ()
saveFile filename bytes =
    let createExclusive = P.fdToHandle =<< P.openFd
            filename
            P.WriteOnly
            (Just 0o600)
            P.defaultFileFlags { P.exclusive = True }
    in
        bracket
            createExclusive
            hClose
            (\h -> B.hPut h bytes)
        -- TODO: Vefify that this is an "already exists" error before
        -- just supressing it.
        `catch` (\e -> print (e :: IOError))

-- | @'blobFile' blobDir digest'@ is the file name in which the block with
-- sha256 hash @digest@ should be stored, given that @blobDir@ is the top-level
-- blob directory.
blobFile :: FilePath -> Hash -> FilePath
blobFile blobDir (Hash digest) =
    let hashname@(c1:c2:_) = B8.unpack $ Base16.encode digest
    in blobDir ++ "/sha256/" ++ [c1,c2] ++ "/" ++ hashname

-- | @'emit' blobDir blob@ Saves @blob@ to the blob directory, If the blob
-- is already present, this is a no-op.
emit :: FilePath -> HashedBlock -> IO ()
emit blobDir (HashedBlock digest bytes) =
    saveFile (blobFile blobDir digest) bytes

-- @'loadBlob@ blobDir digest@ returns the blob corresponding to the
-- given sha256 hash, from the blob directory @blobDir@.
loadBlob :: FilePath -> Hash -> IO B.ByteString
loadBlob blobDir digest = B.readFile (blobFile blobDir digest)

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

-- | Placeholder during experimentation. This stores each of the blocks of
-- the named file in the blob dir at /tmp/bar.
doIt :: FilePath -> IO ()
doIt filename = bracket
    (openBinaryFile filename ReadMode)
    hClose
    $ \h -> runConduit $
        hBlocks h .|
        mapC hash .|
        mapM_C (emit "/tmp/bar")

-- | @'initBlobDir' dir@ creates the directory structure necessary for blob
-- storage in the directory @dir@.
initBlobDir :: FilePath -> IO ()
initBlobDir dir = forM_ [0,1..0xff] $ \n -> do
    createDirectoryIfMissing True $ printf "%s/sha256/%02x" dir (n :: Int)

-- | Placeholder for main, while we're still experimenting.
main = return ()
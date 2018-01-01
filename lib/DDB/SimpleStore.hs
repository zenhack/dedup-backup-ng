{-# LANGUAGE RecordWildCards #-}
module DDB.SimpleStore (simpleStore, initStore) where

import DDB
import DDB.Types

import Codec.Serialise     (deserialise, serialise)
import Control.Monad       (forM_, unless)
import Control.Monad.Catch (bracket, catch, throwM)
import System.Directory    (createDirectoryIfMissing)
import System.IO           (hClose)
import System.IO.Error     (isAlreadyExistsError)
import Text.Printf         (printf)

import qualified Data.ByteString        as B
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8  as B8
import qualified Data.ByteString.Lazy   as LBS
import qualified System.Posix.IO        as P

simpleStore :: FilePath -> Store
simpleStore storePath = Store{..} where
    saveBlock (HashedBlock digest (Block bytes)) =
        saveFile (blockPath digest) bytes

    loadBlock digest = Block <$> B.readFile (blockPath digest)

    saveTag tagname ref = LBS.writeFile (tagPath tagname) (serialise ref)
    loadTag tagname = deserialise <$> LBS.readFile (tagPath tagname)

    -- | @'blockPath' digest@ is the file name in which the block with
    -- sha256 hash @digest@ should be saved within the store.
    blockPath :: Hash -> FilePath
    blockPath (Hash digest) =
        let hashname@(c1:c2:_) = B8.unpack $ Base16.encode digest
        in storePath ++ "/sha256/" ++ [c1,c2] ++ "/" ++ hashname

    tagPath :: String -> FilePath
    tagPath tagname = storePath ++ "/tags/" ++ tagname

-- | @'initStore' dir@ creates the directory structure necessary for
-- storage in the directory @dir@. It returns a refernce to the Store.
initStore :: FilePath -> IO Store
initStore dir = do
    forM_ [0,1..0xff] $ \n -> do
        createDirectoryIfMissing True $ printf "%s/sha256/%02x" dir (n :: Int)
    createDirectoryIfMissing True $ dir ++ "/tags"
    let store = simpleStore dir
    saveBlock store zeroBlock
    return store

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

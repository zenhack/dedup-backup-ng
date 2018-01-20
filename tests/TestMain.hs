module Main where

import DDB
import DDB.NewStore
import DDB.SimpleStore
import DDB.Types

import Control.Monad                        (replicateM)
import Data.Proxy                           (Proxy(..))
import System.Unix.Directory                (withTemporaryDirectory)
import Test.Framework                       (defaultMain)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck                      (Gen, Property, arbitrary, choose)
import Test.QuickCheck.Monadic              (monadicIO, pick, run)

import qualified Data.ByteString as B

-- Generate a bytestring up to 4 MiB in size. This is big enough to test
-- deep-ish trees without being prohibitive.
genBlob :: Gen B.ByteString
genBlob = do
    numBytes <- choose (0 :: Int, 4 * 1024 * 1024)
    B.pack <$> replicateM numBytes arbitrary

saveRestoreBlob :: Store a => Proxy a -> FilePath -> B.ByteString -> IO Bool
saveRestoreBlob proxy path blob = do
    let oldPath = path ++ "/old"
        newPath = path ++ "/new"
        storePath = path ++ "/store"
    B.writeFile oldPath blob
    store <- asIOType proxy (openStore storePath)
    ref <- storeFile store oldPath
    closeStore store
    store' <- asIOType proxy (openStore storePath)
    extractFile store' ref newPath
    old <- B.readFile oldPath
    new <- B.readFile newPath
    closeStore store'
    return (old == new)
  where
    asIOType :: Proxy a -> IO a -> IO a
    asIOType _ x = x

prop_saveRestoreBlob :: Store a => Proxy a -> Property
prop_saveRestoreBlob proxy = monadicIO $ do
    blob <- pick genBlob
    run $ withTemporaryDirectory "/tmp/store.XXXXXX" $ \path ->
            saveRestoreBlob proxy path blob

main :: IO ()
main = defaultMain
    [ testProperty
        "saving and then restoring a blob produces the same bytes."
        (prop_saveRestoreBlob (Proxy :: Proxy NewStore))
    ]

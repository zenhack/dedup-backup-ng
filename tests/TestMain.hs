module Main where

import DDB
import DDB.SimpleStore

import Control.Monad                        (replicateM)
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

saveRestoreBlob :: FilePath -> B.ByteString -> IO Bool
saveRestoreBlob path blob = do
    let oldPath = path ++ "/old"
        newPath = path ++ "/new"
    B.writeFile oldPath blob
    store <- initStore (path ++ "/store")
    ref <- storeFile store oldPath
    extractFile store ref newPath
    old <- B.readFile oldPath
    new <- B.readFile newPath
    return (old == new)

prop_saveRestoreBlob :: Property
prop_saveRestoreBlob = monadicIO $ do
    blob <- pick genBlob
    run $ withTemporaryDirectory "/tmp/store.XXXXXX" $ \path ->
            saveRestoreBlob path blob

main :: IO ()
main = defaultMain
    [ testProperty
        "saving and then restoring a blob produces the same bytes."
        prop_saveRestoreBlob
    ]

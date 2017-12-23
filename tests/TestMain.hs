module Main where

import DedupBackupNG
import MonadFileSystem

import FakeFS (runFakeFS)

import Control.Exception                    (catch, throwIO)
import Control.Monad                        (replicateM)
import System.Environment                   (getEnv)
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

saveRestoreBlob :: MonadFileSystem m => FilePath -> B.ByteString -> m Bool
saveRestoreBlob path blob = do
    let oldPath = path ++ "/old"
        newPath = path ++ "/new"
    writeFileBS oldPath blob
    store <- initStore (path ++ "/store")
    ref <- storeFile store oldPath
    extractFile store ref newPath
    old <- readFileBS oldPath
    new <- readFileBS newPath
    return (old == new)

prop_saveRestoreBlob :: Property
prop_saveRestoreBlob = monadicIO $ do
    -- if the environment variable TEST_IN_IO is defined, we run this in IO,
    -- otherwise we use FakeFS.
    useIO <- run $
        (getEnv "TEST_IN_IO" >> return True)
      `catch` (const (return False) :: IOError -> IO Bool)
    blob <- pick genBlob
    if useIO
        then run $ withTemporaryDirectory "/tmp/store.XXXXXX" $ \path ->
                saveRestoreBlob path blob
        else case runFakeFS $ saveRestoreBlob "" blob of
                Left e       -> run $ throwIO e
                Right result -> return result

main :: IO ()
main = defaultMain
    [ testProperty
        "saving and then restoring a blob produces the same bytes."
        prop_saveRestoreBlob
    ]

module Main where

import DDB
import DDB.Store

import Control.Monad                        (replicateM, when)
import Data.Algorithm.Diff                  (Diff(..), getGroupedDiff)
import Data.Function                        ((&))
import System.Process                       (readProcess)
import System.Unix.Directory                (withTemporaryDirectory)
import Test.Framework                       (defaultMain)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck                      (Gen, Property, arbitrary, choose)
import Test.QuickCheck.Monadic              (monadicIO, pick, run)

import qualified Data.ByteString     as B
import qualified Data.HashMap.Strict as M

-- Generate a bytestring up to 4 MiB in size. This is big enough to test
-- deep-ish trees without being prohibitive.
--
-- TODO: we are actually currently using a block size that is much smaller,
-- because if we show diffs on failure a large block size causes us to run out of
-- memory (the diff algorithm uses quadratic space). Probably the best solution
-- is to make the block size tunable, so that we can have a smaller value during
-- test.
genBlob :: Gen B.ByteString
genBlob = do
--    numBytes <- choose (0 :: Int, 4 * 1024 * 1024)
    numBytes <- choose (0 :: Int, 1024)
    B.pack <$> replicateM numBytes arbitrary

genMTree :: FilePath -> IO String
genMTree path = readProcess "bsdtar" ["-cf", "-", "--format=mtree", path] ""

normalize byPath mtreeLine =
    -- Since we get mtrees on the same contents under two different paths,
    -- we need to strip off the path prefixes:
    stripMTreePrefix byPath mtreeLine &
    words &
    -- I don't fully understand what the "flags" are, but we don't track them:
    filter (not . startsWith "flags=") &
    -- The time field is predictably non-deterministic:
    filter (not . startsWith "time=") &
    unwords

startsWith prefix txt = take (length prefix) txt == prefix

stripMTreePrefix :: FilePath -> String -> String
-- TODO: make this more precise
stripMTreePrefix path ('.':txt) | startsWith path txt = '.' : drop (length path) txt
stripMTreePrefix _ txt          = txt

saveRestoreBlob :: FilePath -> B.ByteString -> IO Bool
saveRestoreBlob path blob = do
    let oldPath = path ++ "/old"
        newPath = path ++ "/new"
        storePath = path ++ "/store"
    B.writeFile oldPath blob
    store <- openStore storePath
    Right ref <- storeFile M.empty store oldPath
    closeStore store
    store' <- openStore storePath
    extractFile store' ref newPath
    old <- B.readFile oldPath
    new <- B.readFile newPath
    closeStore store'
    when (old /= new) $ do
        putStrLn "restored data is corrupted."
        putStrLn ""
        let diff = getGroupedDiff (B.unpack old) (B.unpack new)
        putStrLn $ "Number of chunks in diff : " ++ show (length diff)
        putStrLn $ "Chunk sizes : " ++ show (map chunkSize diff)
        putStrLn "Diff:"
        putStrLn ""
        print diff
    return (old == new)
  where
    -- | Determine the size of a chunk
    chunkSize (First x)  = length x
    chunkSize (Second x) = length x
    -- XXX: this is a bit wrong; we're ignoring the second part.
    chunkSize (Both x _) = length x

saveRestorePath oldPath tempPath = do
    mtreeOld <- lines <$> genMTree oldPath
    let storePath = tempPath ++ "/store"
        newPath = tempPath ++ "/new"
    store <- openStore storePath
    Right ref <- storeFile M.empty store oldPath
    extractFile store ref newPath
    mtreeNew <- lines <$> genMTree newPath
    let mtreeOldNorm = map (normalize oldPath) mtreeOld
        mtreeNewNorm = map (normalize newPath) mtreeNew
        result = mtreeOldNorm == mtreeNewNorm
    when (not result) $ do
        let diff = getGroupedDiff mtreeOldNorm mtreeNewNorm
        putStrLn "Mtrees differ:"
        putStrLn ""
        mapM_ print diff
        putStrLn ""
        mapM_ putStrLn mtreeNew
    return result

prop_saveRestoreBlob :: Property
prop_saveRestoreBlob = monadicIO $ do
    blob <- pick genBlob
    run $ withTemporaryDirectory "/tmp/store.XXXXXX" $ \path ->
            saveRestoreBlob path blob

main :: IO ()
main = do
    -- TODO: make this first part a proper test
    withTemporaryDirectory "/tmp/store.XXXXXX" $ \path -> do
        True <- saveRestorePath "." path
        return ()
    defaultMain
        [ testProperty
            "saving and then restoring a blob produces the same bytes."
            prop_saveRestoreBlob
        ]

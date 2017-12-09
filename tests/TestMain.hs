module Main where

import DedupBackupNG hiding (main)

import Control.Monad (when)
import System.Exit   (exitFailure)

main :: IO ()
main = do
    store <- initStore "/tmp/test-ddb-ng-store"
    ref <- storeFile store "./LICENSE"
    extractFile store ref "/tmp/LICENSE-test-ddb-ng"
    old <- readFile "./LICENSE"
    new <- readFile "/tmp/LICENSE-test-ddb-ng"
    when (old /= new) $ exitFailure

module Main (main) where

import Cli
import System.Environment (getArgs)

main :: IO ()
main = do
    args <- getArgs
    case args of
        [storePath, "tags"] -> runCommand storePath Tags
        _                   -> putStrLn "Unknown command"

module Cli where

import DedupBackupNG

import System.Directory (listDirectory)

data Command
    = Tags
    | Save FilePath
    deriving(Show, Read, Eq)

runCommand :: FilePath -> Command -> IO ()
runCommand storePath Tags = do
    contents <- listDirectory (storePath ++ "/tags")
    mapM_ putStrLn contents

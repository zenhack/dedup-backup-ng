module Cli where

import DedupBackupNG
import Options.Applicative

import Data.Monoid      (mempty, (<>))
import System.Directory (listDirectory)

data Command
    = Tags
    | Save FilePath
    deriving(Show, Read, Eq)

cmdParser :: Parser (FilePath, Command)
cmdParser = (,)
    <$> strOption
        ( short 's'
        <> long "store"
        <> metavar "STORE"
        <> help "path to the store"
        )
    <*> hsubparser
        ( command "tags"
            (info (pure Tags) (progDesc "List tags of snapshots in the store"))
        )

runCommand :: FilePath -> Command -> IO ()
runCommand storePath Tags = do
    contents <- listDirectory (storePath ++ "/tags")
    mapM_ putStrLn contents

main :: IO ()
main = do
    (storePath, cmd) <- execParser $ info cmdParser mempty
    runCommand storePath cmd

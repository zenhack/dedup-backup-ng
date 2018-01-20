module Cli where

import DDB
import DDB.NewStore
import DDB.Types
import Options.Applicative

import Control.Exception (bracket)
import Control.Monad     (void)
import Data.Monoid       (mempty, (<>))
import System.Directory  (listDirectory)

data Command
    = Tags
    | Save FilePath String
    | Restore FilePath String
    | Init
    deriving(Show, Read, Eq)

cmdParser :: Parser (FilePath, Command)
cmdParser = (,)
    <$> strOption
        ( short 's'
        <> long "store"
        <> metavar "STORE"
        <> help "path to the store"
        )
    <*> ( hsubparser $
            (command "init"
                (info
                    (pure Init)
                    (progDesc "Intialize the store")))
        <> (command "tags"
                (info
                    (pure Tags)
                    (progDesc "List tags of snapshots in the store")))
        <> (command "save"
                (info
                    (Save
                        <$> strOption
                        ( long "target" -- TODO: not a fan of this name.
                                        -- maybe this should just be positional?
                        <> metavar "TARGET"
                        <> help "The file or directory to back up."
                        )
                        <*> strOption
                        ( short 't'
                        <> long "tag"
                        <> metavar "TAG"
                        <> help "The tag to reference the snapshot by."
                        ))
                    (progDesc "Save a snapshot.")))
        <> (command "restore"
                (info
                    (Restore
                        <$> strOption
                        ( long "target"
                        <> metavar "TARGET"
                        <> help "The location to extract to."
                        )
                        <*> strOption
                        ( short 't'
                        <> long "tag"
                        <> metavar "TAG"
                        <> help "The tag for the snapshot to restore."
                        ))
                    (progDesc "Restore a snapshot.")))
        )

-- TODO: regression: other than init, each of these should fail if the store
-- does not already exist.
runCommand :: FilePath -> Command -> IO ()
runCommand storePath Tags = do
    contents <- listDirectory (storePath ++ "/tags")
    mapM_ putStrLn contents
runCommand storePath Init = withNewStore storePath (\_ -> pure ())
runCommand storePath (Save target tagname) = withNewStore storePath $ \store ->
    makeSnapshot store target tagname
runCommand storePath (Restore target tagname) = withNewStore storePath $ \store ->
    restoreSnapshot store tagname target

withNewStore :: FilePath -> (NewStore -> IO a) -> IO a
withNewStore path = bracket
    (openNewStore path)
    closeStore

openNewStore :: FilePath -> IO NewStore
openNewStore = openStore

main :: IO ()
main = do
    (storePath, cmd) <- execParser $ info (helper <*> cmdParser) mempty
    runCommand storePath cmd

module Cli where

import DDB
import DDB.Store
import Options.Applicative

import Control.Exception (bracket)
import Data.Monoid       (mempty, (<>))
import System.Directory  (listDirectory)

data Command
    = Tags
    | Save
        { saveTarget :: FilePath
        , saveTag    :: String
        }
    | Restore
        { restoreTarget :: FilePath
        , restoreTag    :: String
        }
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
                    (progDesc "Intialize the store")
                )
            )
        <> (command "tags"
                (info
                    (pure Tags)
                    (progDesc "List tags of snapshots in the store")
                )
            )
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
                            )
                    )
                    (progDesc "Save a snapshot.")
                )
            )
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
                            )
                    )
                    (progDesc "Restore a snapshot.")
                )
            )
        )

-- TODO: regression: other than init, each of these should fail if the store
-- does not already exist.
runCommand :: FilePath -> Command -> IO ()
runCommand storePath Tags = do
    contents <- listDirectory (storePath ++ "/tags")
    mapM_ putStrLn contents
runCommand storePath Init = withStore storePath (\_ -> pure ())
runCommand storePath (Save target tagname) = withStore storePath $ \store ->
    makeSnapshot store target tagname
runCommand storePath (Restore target tagname) = withStore storePath $ \store ->
    restoreSnapshot store tagname target

-- TODO: this doesn't belong here; put it in a different module.
withStore :: FilePath -> (Store -> IO a) -> IO a
withStore path = bracket
    (openStore path)
    closeStore

main :: IO ()
main = do
    (storePath, cmd) <- execParser $ info (helper <*> cmdParser) mempty
    runCommand storePath cmd

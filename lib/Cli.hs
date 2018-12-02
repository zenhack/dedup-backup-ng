{-# LANGUAGE NamedFieldPuns #-}
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
        { saveTarget  :: FilePath
        , saveTagname :: String
        , thirdLeg    :: Maybe FilePath
        }
    | Restore
        { restoreTarget  :: FilePath
        , restoreTagname :: String
        }
    | Merge
        { srcStore  :: FilePath
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
                        <*> ( ( Just <$> strOption
                                    ( long "third-leg"
                                    <> metavar "THIRD LEG"
                                    -- TODO: document this feature more fully.
                                    <> help "Third leg"
                                    )
                                )
                                <|> pure Nothing
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
        <> (command "merge"
                (info
                    (Merge
                        <$> strOption
                            ( long "src"
                            <> metavar "SRC"
                            <> help "The source to merge data from"
                            )
                    )
                    (progDesc "Merge another store into this one.")
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
runCommand storePath Save{saveTarget, saveTagname} = withStore storePath $ \store ->
    makeSnapshot store saveTarget saveTagname
runCommand storePath Restore{restoreTarget, restoreTagname} =
    withStore storePath $ \store ->
        restoreSnapshot store restoreTagname restoreTarget
runCommand destStore Merge{srcStore} = do
    withStore destStore $ \d ->
        withStore srcStore $ \s ->
            mergeStore s d

-- TODO: this doesn't belong here; put it in a different module.
withStore :: FilePath -> (Store -> IO a) -> IO a
withStore path = bracket
    (openStore path)
    closeStore

main :: IO ()
main = do
    (storePath, cmd) <- execParser $ info (helper <*> cmdParser) mempty
    runCommand storePath cmd

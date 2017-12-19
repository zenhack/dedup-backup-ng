{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TypeFamilies               #-}
module FakeFS where

import MonadFileSystem

import Control.Monad.Catch      (MonadCatch, MonadMask(..), MonadThrow(..), try)
import Control.Monad.Catch.Pure (CatchT)
import Control.Monad.State      (State, get, modify, put)
import Data.Function            ((&))
import Data.Hashable            (Hashable)
import Data.Int                 (Int64)
import System.FilePath          (takeDirectory, takeFileName)
import System.IO                (IOMode(ReadWriteMode))
import System.IO.Error
    ( alreadyExistsErrorType
    , doesNotExistErrorType
    , illegalOperationErrorType
    , isDoesNotExistError
    , mkIOError
    )

import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy  as LBS
import qualified Data.HashMap.Strict   as M

newtype FakeFS a = FakeFS { runFakeFS :: CatchT (State FsState) a }
    deriving(Functor, Applicative, Monad, MonadThrow, MonadCatch, MonadMask)

newtype FakeHandle = FakeHandle Int
    deriving(Show, Eq, Ord, Hashable)

type Offset = Int64

newtype INodeRef = INodeRef Int
    deriving(Show, Eq, Ord, Hashable)

data FakeFile = FakeFile
    { mode     :: !IOMode
    , offset   :: !Offset
    , inodeRef :: !INodeRef
    }

data FsState = FsState
    { handles    :: M.HashMap FakeHandle FakeFile
    , rootNode   :: !INodeRef
    , inodes     :: M.HashMap INodeRef INode
    , nextHandle :: !Int
    , nextIRef   :: !Int
    }

data INode
    = IRegFile LBS.ByteString
    | IDirectory (M.HashMap FilePath INodeRef)
    | ISymLink FilePath


data FakeFileStatus = FakeFileStatus
    { _isDirectory    :: Bool
    , _isSymbolicLink :: Bool
    , _isRegularFile  :: Bool
    }

nilStatus :: FakeFileStatus
nilStatus = FakeFileStatus
    { _isDirectory = False
    , _isSymbolicLink = False
    , _isRegularFile = False
    }

parsePath :: FilePath -> [String]
parsePath path = normalize (split '/' path)
  where
    split c = map B8.unpack . B8.split c . B8.pack
    normalize (".":xs)    = normalize xs
    normalize ("":xs)     = normalize xs
    normalize (_:"..":xs) = normalize xs
    normalize (x:xs)      = x:normalize xs
    normalize []          = []

newHandle :: FakeFS FakeHandle
newHandle = FakeFS $ do
    fs@FsState{..} <- get
    put fs { nextHandle = nextHandle + 1 }
    return $ FakeHandle nextHandle

newINodeRef :: FakeFS INodeRef
newINodeRef = FakeFS $ do
    fs@FsState{..} <- get
    put fs { nextIRef = nextIRef + 1 }
    return $ INodeRef nextIRef

inodeByPath :: FilePath -> FakeFS (INodeRef, INode)
inodeByPath path = FakeFS $ do
    FsState{..} <- get
    go rootNode (parsePath path)
  where
    go :: INodeRef -> [String] -> CatchT (State FsState) (INodeRef, INode)
    go iref pathParts = do
        FsState{..} <- get
        case (M.lookup iref inodes, pathParts) of
            (Just inode, []) ->
                return (iref, inode)
            (Just (IDirectory dir), p:ps) -> case M.lookup p dir of
                Just iref' ->
                    go iref' ps
                Nothing ->
                    throwM $ mkIOError doesNotExistErrorType "" Nothing Nothing
            (Just _, _:_) ->
                wrongFileType "directory" Nothing
            (Nothing, _) ->
                error "BUG: INodeRef for absent INode."

instance MonadFileSystem FakeFS where
    type Handle FakeFS = FakeHandle
    type FileStatus FakeFS = FakeFileStatus

    hGetBS h len = do
        file@FakeFile{..} <- lookupHandle h
        -- TODO: check that mode is readable.
        inode <- getFileINode file
        case inode of
            (IRegFile bytes) -> do
                fs@FsState{..} <- FakeFS get
                let result = LBS.take (fromIntegral len) $ LBS.drop offset bytes
                let off' = max (offset + fromIntegral len) (LBS.length bytes)
                FakeFS $ put fs { handles = M.insert h
                                    (file { offset = off'
                                          , inodeRef = inodeRef
                                          }) handles
                                }
                return $ LBS.toStrict result
            _ -> error "BUG: handle points to non-regular file."

    hPutBS h bytes = do
        -- TODO: check that the mode of h is writable
        fs@FsState{..} <- FakeFS get
        FakeFile{..} <- lookupHandle h
        FakeFS $ put fs
            { inodes = M.adjust
                (\inode -> case inode of
                    (IRegFile oldbytes) -> IRegFile $ mconcat
                        [ LBS.take offset oldbytes
                        , LBS.fromStrict bytes
                        , LBS.drop
                            (offset + fromIntegral (B8.length bytes))
                            oldbytes
                        ]
                    _ ->
                        error "BUG: handle points to non-regular file.")
                inodeRef
                inodes
            }

    hClose h = FakeFS $ modify $
        \fs -> fs { handles = M.delete h (handles fs) }

    openBinaryFile path mode = do
        (iref, _) <- inodeByPath path
        h <- newHandle
        let file = FakeFile { mode = mode
                            , offset = 0
                            , inodeRef = iref
                            }
        FakeFS $ modify (\fs@FsState{..} ->
            fs { handles = M.insert h file handles })
        return h

    readFileBS path = do
        (_, inode) <- inodeByPath path
        case inode of
            IRegFile bytes -> return $ LBS.toStrict bytes
            _              -> wrongFileType "regular file" (Just path)

    createExclusive path = do
        existingFile <- try $ inodeByPath path
        case existingFile of
            Right _ -> throwM $ mkIOError
                alreadyExistsErrorType
                "File already exists"
                Nothing
                (Just path)
            Left e
                | isDoesNotExistError e -> do
                    (parentIRef, parentINode) <- inodeByPath $ takeDirectory path
                    case parentINode of
                        IDirectory oldDirEnts -> do
                            iref <- newINodeRef
                            h <- newHandle
                            FakeFS $ modify $ \fs@FsState{..} -> fs
                                { handles = M.insert
                                    h
                                    FakeFile { mode = ReadWriteMode, offset = 0, inodeRef = iref }
                                    handles
                                , inodes =
                                    let newDirEnts = M.insert (takeFileName path) iref oldDirEnts
                                    in  M.insert parentIRef (IDirectory newDirEnts) $
                                        M.insert iref (IRegFile LBS.empty)
                                        inodes
                                }
                            return h
                        _ -> wrongFileType "directory" $ Just (takeDirectory path)
                | otherwise -> throwM e


    listDirectory path = do
        (_, inode) <- inodeByPath path
        case inode of
            IDirectory dir -> return $ M.keys dir
            _              -> wrongFileType "directory" (Just path)

    mkdirP path = do
        FsState{..} <- FakeFS get
        let Just (IDirectory node) = M.lookup rootNode inodes
        go rootNode node (parsePath path)
      where
        go :: INodeRef -> M.HashMap FilePath INodeRef -> [FilePath] -> FakeFS ()
        go _ _ [] = return ()
        go parentIRef parentDirEnts (p:ps) = do
            fs@FsState{..} <- FakeFS get
            case M.lookup p parentDirEnts of
                Just childIRef -> case M.lookup childIRef inodes of
                    Just (IDirectory childDirEnts) ->
                        go childIRef childDirEnts ps
                    Just _ ->
                        wrongFileType "directory" (Just p)
                    Nothing ->
                        error "BUG: iref with no inode"
                Nothing -> do
                    childIRef <- newINodeRef
                    let parentDirEnts' = M.insert p childIRef parentDirEnts
                    FakeFS $ put fs { inodes = inodes
                                        & M.insert childIRef (IDirectory M.empty)
                                        & M.insert parentIRef (IDirectory parentDirEnts')
                                    }


    getSymbolicLinkStatus path = do
        (_, inode) <- inodeByPath path
        return $ case inode of
            IRegFile _   -> nilStatus { _isRegularFile = True }
            IDirectory _ -> nilStatus { _isDirectory = True }
            ISymLink _   -> nilStatus { _isSymbolicLink = True }

    readSymbolicLink path = do
        (_, inode) <- inodeByPath path
        case inode of
            ISymLink target -> return target
            _               -> wrongFileType "symlink" (Just path)

    isDirectory = pure . _isDirectory
    isSymbolicLink = pure . _isSymbolicLink
    isRegularFile = pure . _isRegularFile

lookupHandle :: FakeHandle -> FakeFS FakeFile
lookupHandle h = FakeFS $ do
    FsState{..} <- get
    case M.lookup h handles of
        Just file -> return file
        Nothing ->
            throwM $ mkIOError illegalOperationErrorType "Invalid Handle" Nothing Nothing

getFileINode :: FakeFile -> FakeFS INode
getFileINode FakeFile{..} = FakeFS $ do
    FsState{..} <- get
    case M.lookup inodeRef inodes of
        Just inode -> return inode
        Nothing    -> error "BUG: inode for file does not exist."

-- | @'wrongFileType' expected path@ raise an error indicating an unexpected
-- file type. @expected@ is the type of file that was expected. @path@, if not
-- 'Nothing', is the path to the relevant file.
wrongFileType :: MonadThrow m => String -> Maybe FilePath -> m a
wrongFileType expected path = throwM $ mkIOError
    illegalOperationErrorType
    ("Wrong file type; expected " ++ expected)
    Nothing
    path

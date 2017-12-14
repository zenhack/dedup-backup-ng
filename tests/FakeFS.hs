{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TypeFamilies               #-}
module FakeFS where

import MonadFileSystem

import Control.Monad.Catch      (MonadCatch, MonadMask(..), MonadThrow(..))
import Control.Monad.Catch.Pure (CatchT)
import Control.Monad.State      (State, get, modify, put)
import Data.Hashable            (Hashable)
import Data.Int                 (Int64)
import System.IO.Error
    (doesNotExistErrorType, illegalOperationErrorType, mkIOError)

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

data FakeFile = FakeFile !Offset !INodeRef

data FsState = FsState
    { handles  :: M.HashMap FakeHandle FakeFile
    , rootNode ::  INodeRef
    , inodes   :: M.HashMap INodeRef INode
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

inodeByPath :: FilePath -> FakeFS INode
inodeByPath path = FakeFS $ do
    FsState{..} <- get
    go rootNode (parsePath path)
  where
    go :: INodeRef -> [String] -> CatchT (State FsState) INode
    go iref pathParts = do
        FsState{..} <- get
        case (M.lookup iref inodes, pathParts) of
            (Just inode, []) ->
                return inode
            (Just (IDirectory dir), p:ps) -> case M.lookup p dir of
                Just iref' ->
                    go iref' ps
                Nothing ->
                    throwM $ mkIOError doesNotExistErrorType "" Nothing Nothing
            (Just _, _:_) ->
                throwM $ mkIOError illegalOperationErrorType "Not a directory" Nothing Nothing
            (Nothing, _) ->
                error "BUG: INodeRef for absent INode."

instance MonadFileSystem FakeFS where
    type Handle FakeFS = FakeHandle
    type FileStatus FakeFS = FakeFileStatus

    hGetBS h len = FakeFS $ do
        fs@FsState{..} <- get
        case M.lookup h handles of
            Nothing ->
                throwM $ mkIOError illegalOperationErrorType "Invalid Handle" Nothing Nothing
            Just (FakeFile off iref) -> case M.lookup iref inodes of
                Just (IRegFile bytes) -> do
                    let result = LBS.take (fromIntegral len) $ LBS.drop off bytes
                    let off' = max (off + fromIntegral len) (LBS.length bytes)
                    put fs { handles = M.insert h (FakeFile off' iref) handles }
                    return $ LBS.toStrict result
                _ -> error "BUG: Handle is valid but inode does not exist."

    hClose h = FakeFS $ modify $
        \fs -> fs { handles = M.delete h (handles fs) }

    getSymbolicLinkStatus path = do
        inode <- inodeByPath path
        return $ case inode of
            IRegFile _   -> nilStatus { _isRegularFile = True }
            IDirectory _ -> nilStatus { _isDirectory = True }
            ISymLink _   -> nilStatus { _isSymbolicLink = True }

    readSymbolicLink path = do
        inode <- inodeByPath path
        case inode of
            ISymLink target ->
                return target
            _               ->
                throwM $ mkIOError illegalOperationErrorType "Not a symlink" Nothing (Just path)

    isDirectory = pure . _isDirectory
    isSymbolicLink = pure . _isSymbolicLink
    isRegularFile = pure . _isRegularFile

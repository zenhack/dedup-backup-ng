{-# LANGUAGE Rank2Types   #-}
{-# LANGUAGE TypeFamilies #-}
module MonadFileSystem where

import Conduit

import Control.Monad       (unless)
import Control.Monad.Catch (MonadMask, bracket, catch, throwM)
import Data.Void           (Void)
import System.IO.Error     (isAlreadyExistsError)

import qualified Data.ByteString    as B
import qualified System.Directory   as Dir
import qualified System.IO          as IO
import qualified System.Posix.Files as P
import qualified System.Posix.IO    as P

-- Type class to abstract out the filesystem related operations we need, so
-- we can spoof them in the tests. Most of these are equivalent to some
-- existing function in IO, by the same name (or similar where noted).
class MonadMask m => MonadFileSystem m where
    type Handle m
    type FileStatus m

    -- 'B.hGet'
    hGetBS :: Handle m -> Int -> m B.ByteString

    hClose :: Handle m -> m ()
    openBinaryFile :: FilePath -> IO.IOMode -> m (Handle m)

    -- 'B.readFile'
    readFileBS :: FilePath -> m B.ByteString

    -- This is the one function that doesn't map almost directly  to an
    -- existant library function in IO. Save the provided ByteString to
    -- the named file, if the file does not already exist. If it does
    -- exist, this is a no-op.
    saveFile :: FilePath -> B.ByteString -> m ()

    listDirectory :: FilePath -> m [FilePath]
    createDirectoryIfMissing :: Bool -> FilePath -> m ()

    -- Like 'sinkFileBS', except that it doesn't need to run in a MonadResource,
    -- and accepts a file handle, rather than a path.
    --
    -- This makes things easier to work with since runConduitRes requires IO
    -- at the base of the stack.
    fsSinkFileBS :: Handle m -> Consumer B.ByteString m ()

    getSymbolicLinkStatus :: (FileStatus m ~ status) => FilePath -> m status
    readSymbolicLink :: FilePath -> m FilePath

    -- These are like their counterparts from the unix library, except that
    -- the return value is monadic. These shouldn't actually have any effects;
    -- I was getting errors from the type checker about ambiguity when I just
    -- had them as :: FileStatus m -> Bool, and couldn't sort it out :/
    isRegularFile :: FileStatus m -> m Bool
    isDirectory :: FileStatus m -> m Bool
    isSymbolicLink :: FileStatus m -> m Bool

instance MonadFileSystem IO where
    -- trivial aliases:
    type Handle IO = IO.Handle
    type FileStatus IO = P.FileStatus
    hGetBS = B.hGet
    hClose = IO.hClose
    openBinaryFile = IO.openBinaryFile
    readFileBS = B.readFile
    getSymbolicLinkStatus = P.getSymbolicLinkStatus
    readSymbolicLink = P.readSymbolicLink
    listDirectory = Dir.listDirectory
    createDirectoryIfMissing = Dir.createDirectoryIfMissing

    -- just add pure. See the comments in the type class.
    isRegularFile = pure . P.isRegularFile
    isDirectory = pure . P.isDirectory
    isSymbolicLink = pure . P.isSymbolicLink

    fsSinkFileBS h = mapM_C (B.hPut h)

    saveFile filename bytes =
        bracket
            createExclusive
            hClose
            (\h -> B.hPut h bytes)
        `catch`
            (\e -> unless (isAlreadyExistsError e) $ throwM e)
      where
        createExclusive = P.fdToHandle =<< P.openFd
            filename
            P.WriteOnly
            (Just 0o600)
            P.defaultFileFlags { P.exclusive = True }

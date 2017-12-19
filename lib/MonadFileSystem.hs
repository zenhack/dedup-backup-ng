{-# LANGUAGE Rank2Types   #-}
{-# LANGUAGE TypeFamilies #-}
module MonadFileSystem where

import Control.Monad.Catch (MonadMask)

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

    -- 'B.hPut'
    hPutBS :: Handle m -> B.ByteString -> m ()

    hClose :: Handle m -> m ()
    openBinaryFile :: FilePath -> IO.IOMode -> m (Handle m)

    -- 'B.writeFile'
    writeFileBS :: FilePath -> B.ByteString -> m ()

    -- 'B.readFile'
    readFileBS :: FilePath -> m B.ByteString

    -- | Create a file (which must not arlready exist) in exclusive mode.
    createExclusive :: FilePath -> m (Handle m)

    listDirectory :: FilePath -> m [FilePath]

    -- | equivalent to @'createDirectoryIfMissing' True@.
    mkdirP :: FilePath -> m ()

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
    hPutBS = B.hPut
    hClose = IO.hClose
    openBinaryFile = IO.openBinaryFile
    writeFileBS = B.writeFile
    readFileBS = B.readFile
    getSymbolicLinkStatus = P.getSymbolicLinkStatus
    readSymbolicLink = P.readSymbolicLink
    listDirectory = Dir.listDirectory

    mkdirP = Dir.createDirectoryIfMissing True

    -- just add pure. See the comments in the type class.
    isRegularFile = pure . P.isRegularFile
    isDirectory = pure . P.isDirectory
    isSymbolicLink = pure . P.isSymbolicLink

    createExclusive filename = P.fdToHandle =<< P.openFd
            filename
            P.WriteOnly
            (Just 0o600)
            P.defaultFileFlags { P.exclusive = True }

{-# LANGUAGE DeriveGeneric #-}
module DDB.Types where

import Codec.Serialise (Serialise)
import Data.Int        (Int64)
import Data.Word       (Word32)
import GHC.Generics    (Generic)

import qualified Data.ByteString as B

-- | A storage backend.
class Store s where
    -- | @'saveBlock' store block@ Saves @block@ to the store. If the block
    -- is already present, this is a no-op.
    saveBlock :: s -> HashedBlock -> IO ()
    -- @'loadBlock' store digest@ returns the block corresponding to the
    -- given sha256 hash from the store.
    loadBlock :: s -> Hash -> IO Block
    -- | @'loadTag' store tagname@ loads the FileRef for the given tag.
    loadTag   :: s -> String -> IO FileRef
    -- | @'saveTag' store tagname ref@ saves @ref@ under the given tag.
    saveTag   :: s -> String -> FileRef -> IO ()

-- | newtype wrapper around a disk/storage block.
newtype Block = Block B.ByteString
    deriving(Show, Eq, Generic)

-- | Wrapper around sha256 digests.
newtype Hash = Hash B.ByteString
    deriving(Show, Eq, Generic)

instance Serialise Hash

-- | A reference to a blob. This includes all information necessary to read the
-- blob, not counting the location of the store.
data BlobRef = BlobRef !Int64 !Hash
    deriving(Show, Eq, Generic)

instance Serialise BlobRef

-- | A block together with its hash.
data HashedBlock = HashedBlock
    { blockDigest :: !Hash
    , blockBytes  :: !Block
    }

-- | A reference to a file in the store.
data FileRef
    = RegFile !Metadata !BlobRef
    | SymLink !B.ByteString -- target of the link.
    | Dir !Metadata !BlobRef
    deriving(Show, Eq, Generic)

instance Serialise FileRef

data Metadata = Metadata
    { metaMode       :: !Word32
    , metaModTime    :: !Int64
    , metaAccessTime :: !Int64
    , metaOwner      :: !Word32
    , metaGroup      :: !Word32
    } deriving(Show, Eq, Generic)

instance Serialise Metadata

-- | A directory entry. The 'Dir' variant of 'FileRef' points to a blob whose
-- contents are a sequence of these.
data DirEnt = DirEnt
    { entName :: !B.ByteString -- file name
    , entRef  :: !FileRef
    } deriving(Show, Eq, Generic)

instance Serialise DirEnt


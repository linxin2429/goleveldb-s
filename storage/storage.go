package storage

import (
	"errors"
	"fmt"
	"io"
)

type FileType int

const (
	TypeManifest FileType = 1 << iota
	TypeJournal
	TypeTable
	TypeTemp
	TypeAll = TypeManifest | TypeJournal | TypeTable | TypeTemp
)

func (ft FileType) String() string {
	switch ft {
	case TypeManifest:
		return "manifest"
	case TypeJournal:
		return "joural"
	case TypeTable:
		return "table"
	case TypeTemp:
		return "temp"
	}
	return fmt.Sprintf("<unknown:%d>", ft)
}

var (
	ErrInvalidFile = errors.New("leveldb/storage: invalid file for argument")
	ErrLocked      = errors.New("leveldb/storage: already locked")
	ErrClosed      = errors.New("leveldb/storage: closed")
)

// a wrapper of an error indicated corruption of a file
type ErrCorrupted struct {
	Fd  FileDesc
	Err error
}

func isCorrupted(err error) bool {
	switch err.(type) {
	case *ErrCorrupted:
		return true
	default:
		return false
	}
}

func (e *ErrCorrupted) Error() string {
	if !e.Fd.Zero() {
		return fmt.Sprintf("%v [file=%v]", e.Err, e.Fd)
	}
	return e.Err.Error()
}

type Syncer interface {
	// Sync commits the current contents of the file to stable storage.
	Sync() error
}

type Reader interface {
	io.ReadSeeker
	io.ReaderAt
	io.Closer
}

type Writer interface {
	io.WriteCloser
	Syncer
}

type Locker interface {
	Unlock()
}

// file descriptor
type FileDesc struct {
	Type FileType
	Num  int64
}

func (fd FileDesc) String() string {
	switch fd.Type {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case TypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case TypeTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		return fmt.Sprintf("%#x-%d", fd.Type, fd.Num)
	}
}

// Zero returns true if fd == (FileDesc{}).
func (fd FileDesc) Zero() bool {
	return fd == (FileDesc{})
}

func FileDescOk(fd FileDesc) bool {
	switch fd.Type {
	case TypeManifest:
	case TypeJournal:
	case TypeTable:
	case TypeTemp:
	default:
		return false
	}
	return fd.Num >= 0
}

type Storage interface {
	Lock() (Locker, error)
	Log(str string)
	SetMeta(fd FileDesc) error
	GetMeta() (FileDesc, error)
	List(ft FileType) ([]FileDesc, error)
	Open(fd FileDesc) (Reader, error)
	Create(fd FileDesc) (Writer, error)
	Remove(fd FileDesc) error
	Rename(oldfd, newfd FileDesc) error
	Close() error
}

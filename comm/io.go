package comm

import (
	"io"
)

type FlushReaderWriter interface {
	io.ReadWriter
	Flush() error
}

type FlushReaderWriterImpl struct {
	FlushReaderWriter FlushReaderWriter
}

func MakeFlushReaderWriter(o FlushReaderWriter) io.ReadWriter {
	return &FlushReaderWriterImpl{
		FlushReaderWriter: o,
	}
}

func (f *FlushReaderWriterImpl) Read(p []byte) (n int, err error) {
	n, err = f.FlushReaderWriter.Read(p)
	f.FlushReaderWriter.Flush()
	return
}

func (f *FlushReaderWriterImpl) Write(p []byte) (n int, err error) {
	n, err = f.FlushReaderWriter.Write(p)
	f.FlushReaderWriter.Flush()
	return
}

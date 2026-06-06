package comm

import (
	"bufio"
	"io"
	"strconv"
	"strings"
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
func CopyRawChunkedBody(dst io.Writer, r *bufio.Reader) error {
	for {
		// chunk-size line, including extensions
		line, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		if _, err := io.WriteString(dst, line); err != nil {
			return err
		}

		sizePart := strings.TrimSpace(line)
		if i := strings.IndexByte(sizePart, ';'); i >= 0 {
			sizePart = sizePart[:i]
		}

		n, err := strconv.ParseInt(sizePart, 16, 64)
		if err != nil {
			return err
		}

		if n > 0 {
			if _, err := io.CopyN(dst, r, n+2); err != nil { // data + trailing CRLF
				return err
			}
			continue
		}

		// n == 0: trailers, until empty line
		for {
			trailerLine, err := r.ReadString('\n')
			if err != nil {
				return err
			}
			if _, err := io.WriteString(dst, trailerLine); err != nil {
				return err
			}
			if trailerLine == "\r\n" {
				return nil
			}
		}
	}
}

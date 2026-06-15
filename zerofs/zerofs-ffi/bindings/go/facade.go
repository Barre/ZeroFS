// Idiomatic Go conveniences over the uniffi-generated bindings: a
// range-over-func Dir iterator, io.Reader/io.Writer over a File, and
// context.Context wrappers (Call/Do) around the generated blocking calls.
//
// Copied next to the generated `zerofs_ffi.go` at build time so it shares the
// package; Go allows methods only in the type's own package, and these hang
// methods on the generated `*Dir`/`*File`.

package zerofs_ffi

import (
	"context"
	"io"
	"iter"
)

// readBatchHint is the upper bound on entries requested per NextBatch call.
// A nil maxEntries lets the server pick its natural batch size; we pass a hint
// so a single page stays bounded regardless of directory size.
const readBatchHint uint32 = 256

// Entries returns an iterator over the directory's entries in directory order,
// excluding "." and "..". It is a Go 1.23+ range-over-func, so it reads as:
//
//	for entry, err := range dir.Entries() {
//	    if err != nil {
//	        // handle and break
//	    }
//	    use(entry)
//	}
//
// One server batch is pulled off NextBatch at a time (lazily, as the loop
// advances), so a large directory is never buffered whole. If NextBatch fails,
// the iterator yields a single (zero DirEntry, err) pair and then stops; the
// caller decides whether to break. Iteration starts from the directory's
// current cursor position; call Rewind first to list from the beginning.
//
// The Dir handle is not closed by the iterator; close it yourself (defer
// dir.Close()).
func (d *Dir) Entries() iter.Seq2[DirEntry, error] {
	hint := readBatchHint
	return func(yield func(DirEntry, error) bool) {
		for {
			batch, err := d.NextBatch(&hint)
			if err != nil {
				yield(DirEntry{}, err)
				return
			}
			if len(batch) == 0 {
				return
			}
			for _, entry := range batch {
				if !yield(entry, nil) {
					return // caller broke out of the range
				}
			}
		}
	}
}

// fileReader is an io.Reader (and io.WriterTo) that streams a File's contents
// sequentially from a starting offset using positioned ReadAt calls.
type fileReader struct {
	file   *File
	offset uint64
	chunk  uint32
}

// readerChunk is the per-call read size for the streaming reader. The client
// chunks larger reads internally, but keeping each ReadAt bounded means a short
// (sub-chunk) result reliably signals EOF.
const readerChunk uint32 = 1 << 20 // 1 MiB

// Reader returns an io.Reader that streams the file from byte 0 to EOF via
// ReadAt, so the file plugs directly into io.Copy, io.ReadAll, bufio, etc.
// Each Read issues one positioned ReadAt; a short result is treated as EOF
// (io.EOF is returned once no more bytes are available). The reader holds the
// File handle but does not close it; close the file yourself.
func (f *File) Reader() io.Reader {
	return &fileReader{file: f, offset: 0, chunk: readerChunk}
}

// ReaderAt returns a streaming io.Reader starting at the given byte offset.
func (f *File) ReaderAt(offset uint64) io.Reader {
	return &fileReader{file: f, offset: offset, chunk: readerChunk}
}

// Read implements io.Reader. It performs at most one ReadAt per call, copying
// into p, and reports io.EOF when the underlying file yields no more bytes.
func (r *fileReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	want := r.chunk
	if uint32(len(p)) < want {
		want = uint32(len(p))
	}
	data, err := r.file.ReadAt(r.offset, want)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, data)
	r.offset += uint64(n)
	return n, nil
}

// WriteTo implements io.WriterTo. It streams the file from the reader's current
// offset to EOF straight into w, one ReadAt-sized chunk at a time, so
// io.Copy(w, file.Reader()) uses this path and copies with no intermediate
// buffer of its own. It returns the number of bytes written and the first error
// encountered (reaching EOF is reported as nil, per the io.WriterTo contract).
func (r *fileReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		data, err := r.file.ReadAt(r.offset, r.chunk)
		if err != nil {
			return total, err
		}
		if len(data) == 0 {
			return total, nil // EOF: WriterTo reports it as no error
		}
		n, werr := w.Write(data)
		total += int64(n)
		r.offset += uint64(n)
		if werr != nil {
			return total, werr
		}
		if n < len(data) {
			return total, io.ErrShortWrite
		}
	}
}

// fileWriter is an io.Writer that appends to a File via positioned WriteAt
// calls, advancing its offset by each write's length. It is the write-side
// mirror of fileReader.
type fileWriter struct {
	file   *File
	offset uint64
}

// Writer returns an io.Writer that writes the file starting at byte 0 via
// WriteAt, advancing the offset after each write, so the file is a drop-in
// destination for io.Copy(file.Writer(), src), bufio.NewWriter, etc. The writer
// holds the File handle but does not close, flush, or sync it; do that
// yourself (e.g. file.SyncAll then file.Close).
func (f *File) Writer() io.Writer {
	return &fileWriter{file: f, offset: 0}
}

// WriterAt returns an io.Writer that begins writing at the given byte offset and
// advances from there.
func (f *File) WriterAt(offset uint64) io.Writer {
	return &fileWriter{file: f, offset: offset}
}

// Write implements io.Writer. It issues one WriteAt for p at the current offset;
// on success the whole slice is written (WriteAt is all-or-error), the offset
// advances by len(p), and it reports (len(p), nil) as io.Writer requires.
func (w *fileWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := w.file.WriteAt(w.offset, p); err != nil {
		return 0, err
	}
	w.offset += uint64(len(p))
	return len(p), nil
}

// Call runs fn (typically one of the generated blocking calls) and returns its
// result, but stops waiting as soon as ctx is done, returning ctx.Err() in that
// case. It is the bridge from Go's context.Context idiom to the generated
// synchronous API, which takes no context.
//
// CAVEAT: cancellation ABANDONS THE WAIT, it does not cancel the work. fn runs
// in its own goroutine; when ctx is cancelled Call returns immediately, but the
// underlying Rust/FFI call keeps running until the server answers, and its
// result (and any error) is then discarded. There is no server-side
// cancellation. This matches uniffi's own cancellation model. Pass a context
// with a deadline to bound how long YOU wait, not how long the server works.
func Call[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	type result struct {
		val T
		err error
	}
	ch := make(chan result, 1) // buffered so the goroutine never leaks on cancel
	go func() {
		val, err := fn()
		ch <- result{val, err}
	}()
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case r := <-ch:
		return r.val, r.err
	}
}

// Do is the no-result form of Call, for blocking calls that return only an
// error (Write, CreateDirAll, RemoveDirAll, ...). The same caveat applies:
// cancelling ctx abandons the wait but does not cancel the in-flight call.
func Do(ctx context.Context, fn func() error) error {
	_, err := Call(ctx, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

// Convenience accessors mirroring the Rust client, so callers write
// meta.IsDir() instead of meta.FileType == FileTypeDir.

// IsFile reports whether the entry is a regular file.
func (m Metadata) IsFile() bool { return m.FileType == FileTypeFile }

// IsDir reports whether the entry is a directory.
func (m Metadata) IsDir() bool { return m.FileType == FileTypeDir }

// IsSymlink reports whether the entry is a symbolic link.
func (m Metadata) IsSymlink() bool { return m.FileType == FileTypeSymlink }

// Permissions returns the permission bits (mode & 0o7777).
func (m Metadata) Permissions() uint32 { return m.Mode & 0o7777 }

// Canonicalize and ReadLink return raw bytes (lossless). These decode them as
// UTF-8 strings for the common case.

// CanonicalizeStr is Canonicalize with the result decoded as a UTF-8 string.
func (c *Client) CanonicalizeStr(path string) (string, error) {
	b, err := c.Canonicalize(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ReadLinkStr is ReadLink with the target decoded as a UTF-8 string.
func (c *Client) ReadLinkStr(path string) (string, error) {
	b, err := c.ReadLink(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

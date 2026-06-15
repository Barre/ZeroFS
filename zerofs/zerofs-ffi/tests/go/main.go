package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	zerofs "zerofs.example/binding/zerofs_ffi"
)

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: %s <target>", os.Args[0])
	}
	target := os.Args[1]

	fs, err := zerofs.ClientConnect(target)
	if err != nil {
		fail("connect(%q): %v", target, err)
	}
	defer fs.Close()

	const dir = "/go-e2e"
	// Clean slate in case a previous run left it behind.
	_ = fs.RemoveDirAll(dir)

	if err := fs.CreateDirAll(dir, 0o755); err != nil {
		fail("create_dir_all(%q): %v", dir, err)
	}

	small := dir + "/hello.txt"
	payload := []byte("hello, zerofs from Go\n")
	if err := fs.Write(small, payload); err != nil {
		fail("write(%q): %v", small, err)
	}
	got, err := fs.Read(small)
	if err != nil {
		fail("read(%q): %v", small, err)
	}
	if !bytes.Equal(got, payload) {
		fail("read roundtrip mismatch: got %q want %q", got, payload)
	}

	// stat: assert size, type, and that the time field is a Go time.Time.
	md, err := fs.Stat(small)
	if err != nil {
		fail("stat(%q): %v", small, err)
	}
	if md.Size != uint64(len(payload)) {
		fail("stat size: got %d want %d", md.Size, len(payload))
	}
	if md.FileType != zerofs.FileTypeFile {
		fail("stat file_type: got %v want FileTypeFile", md.FileType)
	}
	// md.Mtime is statically typed as time.Time; exercise it as one.
	var mtime time.Time = md.Mtime
	if mtime.IsZero() {
		fail("stat mtime is the zero time.Time: %v", mtime)
	}
	if mtime.After(time.Now().Add(24 * time.Hour)) {
		fail("stat mtime implausibly far in the future: %v", mtime)
	}
	fmt.Printf("stat: size=%d type=%v mtime=%s (Go time.Time)\n",
		md.Size, md.FileType, mtime.Format(time.RFC3339))

	// Metadata predicates on a regular file.
	if !md.IsFile() || md.IsDir() || md.IsSymlink() {
		fail("file predicates: IsFile=%v IsDir=%v IsSymlink=%v", md.IsFile(), md.IsDir(), md.IsSymlink())
	}
	if md.Permissions() != md.Mode&0o7777 {
		fail("permissions: %o vs %o", md.Permissions(), md.Mode&0o7777)
	}

	// Populate a few entries for the directory iterator.
	want := []string{"a", "b", "c", "hello.txt"}
	for _, n := range []string{"a", "b", "c"} {
		if err := fs.Write(dir+"/"+n, []byte(n)); err != nil {
			fail("write(%q): %v", dir+"/"+n, err)
		}
	}

	// Directory iterator (range-over-func). Assert names.
	d, err := fs.OpenDir(dir)
	if err != nil {
		fail("open_dir(%q): %v", dir, err)
	}
	var names []string
	for entry, err := range d.Entries() {
		if err != nil {
			d.Close()
			fail("dir iterate: %v", err)
		}
		names = append(names, entry.Name)
	}
	d.Close()
	sort.Strings(names)
	if !equalStrings(names, want) {
		fail("dir entries: got %v want %v", names, want)
	}
	fmt.Printf("dir entries: %v\n", names)

	// Streaming reader over a multi-chunk file (> 1 MiB so Read loops).
	big := dir + "/big.bin"
	const bigSize = 5*1024*1024 + 123 // 5 MiB + change: spans many reader chunks
	bigData := make([]byte, bigSize)
	for i := range bigData {
		bigData[i] = byte(i*31 + 7)
	}
	if err := fs.Write(big, bigData); err != nil {
		fail("write(%q): %v", big, err)
	}
	bf, err := fs.Open(big, zerofs.OpenOptions{Read: true, Mode: 0o644})
	if err != nil {
		fail("open(%q): %v", big, err)
	}
	streamed, err := io.ReadAll(bf.Reader())
	bf.Close()
	if err != nil {
		fail("stream read(%q): %v", big, err)
	}
	if len(streamed) != bigSize {
		fail("stream length: got %d want %d", len(streamed), bigSize)
	}
	if !bytes.Equal(streamed, bigData) {
		fail("stream content mismatch over %d bytes", bigSize)
	}
	fmt.Printf("stream read: %d bytes via io.ReadAll(file.Reader())\n", len(streamed))

	// io.WriterTo: io.Copy(dst, file.Reader()) must select WriteTo and stream
	// the whole file into the buffer with the bytes intact.
	rf, err := fs.Open(big, zerofs.OpenOptions{Read: true, Mode: 0o644})
	if err != nil {
		fail("open(%q) for WriteTo: %v", big, err)
	}
	if _, ok := rf.Reader().(io.WriterTo); !ok {
		rf.Close()
		fail("file.Reader() does not implement io.WriterTo")
	}
	var copyBuf bytes.Buffer
	copied, err := io.Copy(&copyBuf, rf.Reader())
	rf.Close()
	if err != nil {
		fail("io.Copy from file.Reader(): %v", err)
	}
	if copied != int64(bigSize) {
		fail("WriteTo copied length: got %d want %d", copied, bigSize)
	}
	if !bytes.Equal(copyBuf.Bytes(), bigData) {
		fail("WriteTo content mismatch over %d bytes", bigSize)
	}
	fmt.Printf("WriteTo: io.Copy(buf, file.Reader()) -> %d bytes\n", copied)

	// io.Writer: io.Copy(file.Writer(), src) must roundtrip the source bytes
	// to the file (read back via the streaming reader and compared).
	wpath := dir + "/written.bin"
	srcData := make([]byte, 3*1024*1024+45) // multi-chunk source
	for i := range srcData {
		srcData[i] = byte(i*17 + 3)
	}
	wf, err := fs.Open(wpath, zerofs.OpenOptions{Write: true, Create: true, Truncate: true, Mode: 0o644})
	if err != nil {
		fail("open(%q) for Writer: %v", wpath, err)
	}
	written, err := io.Copy(wf.Writer(), bytes.NewReader(srcData))
	if err != nil {
		wf.Close()
		fail("io.Copy into file.Writer(): %v", err)
	}
	if err := wf.SyncAll(); err != nil {
		wf.Close()
		fail("sync after Writer: %v", err)
	}
	wf.Close()
	if written != int64(len(srcData)) {
		fail("Writer copied length: got %d want %d", written, len(srcData))
	}
	roundtrip, err := fs.Read(wpath)
	if err != nil {
		fail("read back written file: %v", err)
	}
	if !bytes.Equal(roundtrip, srcData) {
		fail("Writer roundtrip mismatch over %d bytes", len(srcData))
	}
	fmt.Printf("Writer: io.Copy(file.Writer(), src) -> %d bytes (roundtrip ok)\n", written)

	// Symlink + string path helpers + directory predicate.
	if _, err := fs.Symlink("hello.txt", dir+"/link"); err != nil {
		fail("symlink: %v", err)
	}
	lmd, err := fs.Stat(dir + "/link") // stat does not follow symlinks
	if err != nil {
		fail("stat(link): %v", err)
	}
	if !lmd.IsSymlink() {
		fail("link not reported as a symlink")
	}
	linkTarget, err := fs.ReadLinkStr(dir + "/link")
	if err != nil {
		fail("read_link_str: %v", err)
	}
	if linkTarget != "hello.txt" {
		fail("read_link_str: got %q want %q", linkTarget, "hello.txt")
	}
	dmd, err := fs.Stat(dir)
	if err != nil {
		fail("stat(dir): %v", err)
	}
	if !dmd.IsDir() {
		fail("dir not reported as a directory")
	}
	canon, err := fs.CanonicalizeStr(small)
	if err != nil {
		fail("canonicalize_str: %v", err)
	}
	if canon != small {
		fail("canonicalize_str: got %q want %q", canon, small)
	}
	fmt.Printf("predicates+strpaths: link->%s, canon=%s, dir.IsDir=%v\n", linkTarget, canon, dmd.IsDir())

	// Call/Do context helpers.
	// Live context: Call returns the underlying value.
	liveData, err := zerofs.Call(context.Background(), func() ([]byte, error) {
		return fs.Read(small)
	})
	if err != nil {
		fail("Call(live): %v", err)
	}
	if !bytes.Equal(liveData, payload) {
		fail("Call(live) value mismatch: got %q want %q", liveData, payload)
	}
	// Live context: Do returns nil for a successful error-only call.
	if err := zerofs.Do(context.Background(), func() error {
		return fs.Write(dir+"/touch", []byte("x"))
	}); err != nil {
		fail("Do(live): %v", err)
	}
	// Cancelled context: Call must return ctx.Err() promptly without waiting
	// for the (blocking) fn to finish.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, cerr := zerofs.Call(cancelled, func() (int, error) {
		time.Sleep(2 * time.Second) // simulate a slow server call
		return 42, nil
	})
	elapsed := time.Since(start)
	if !errors.Is(cerr, context.Canceled) {
		fail("Call(cancelled): got %v want context.Canceled", cerr)
	}
	if elapsed > 500*time.Millisecond {
		fail("Call(cancelled) did not return promptly: took %s", elapsed)
	}
	// Same for Do.
	if derr := zerofs.Do(cancelled, func() error {
		time.Sleep(2 * time.Second)
		return nil
	}); !errors.Is(derr, context.Canceled) {
		fail("Do(cancelled): got %v want context.Canceled", derr)
	}
	fmt.Printf("Call/Do: live returns value; cancelled returns ctx.Err() in %s\n", elapsed.Round(time.Millisecond))

	// NotFound error (typed via errors.Is).
	_, err = fs.Read(dir + "/does-not-exist")
	if err == nil {
		fail("read of missing path: expected error, got nil")
	}
	if !errors.Is(err, zerofs.ErrZeroFsErrorNotFound) {
		fail("read of missing path: expected NotFound, got %T: %v", err, err)
	}
	fmt.Printf("not-found: %v\n", err)

	if err := fs.RemoveDirAll(dir); err != nil {
		fail("remove_dir_all(%q): %v", dir, err)
	}

	fmt.Println("PASS")
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

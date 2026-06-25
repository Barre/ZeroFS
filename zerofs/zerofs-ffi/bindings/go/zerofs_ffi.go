package zerofs_ffi

// #include <zerofs_ffi.h>
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"runtime/cgo"
	"sync/atomic"
	"time"
	"unsafe"
)

// This is needed, because as of go 1.24
// type RustBuffer C.RustBuffer cannot have methods,
// RustBuffer is treated as non-local type
type GoRustBuffer struct {
	inner C.RustBuffer
}

type RustBufferI interface {
	AsReader() *bytes.Reader
	Free()
	ToGoBytes() []byte
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

// C.RustBuffer fields exposed as an interface so they can be accessed in different Go packages.
// See https://github.com/golang/go/issues/13467
type ExternalCRustBuffer interface {
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

func RustBufferFromC(b C.RustBuffer) ExternalCRustBuffer {
	return GoRustBuffer{
		inner: b,
	}
}

func CFromRustBuffer(b ExternalCRustBuffer) C.RustBuffer {
	return C.RustBuffer{
		capacity: C.uint64_t(b.Capacity()),
		len:      C.uint64_t(b.Len()),
		data:     (*C.uchar)(b.Data()),
	}
}

func RustBufferFromExternal(b ExternalCRustBuffer) GoRustBuffer {
	return GoRustBuffer{
		inner: C.RustBuffer{
			capacity: C.uint64_t(b.Capacity()),
			len:      C.uint64_t(b.Len()),
			data:     (*C.uchar)(b.Data()),
		},
	}
}

func (cb GoRustBuffer) Capacity() uint64 {
	return uint64(cb.inner.capacity)
}

func (cb GoRustBuffer) Len() uint64 {
	return uint64(cb.inner.len)
}

func (cb GoRustBuffer) Data() unsafe.Pointer {
	return unsafe.Pointer(cb.inner.data)
}

func (cb GoRustBuffer) AsReader() *bytes.Reader {
	b := unsafe.Slice((*byte)(cb.inner.data), C.uint64_t(cb.inner.len))
	return bytes.NewReader(b)
}

func (cb GoRustBuffer) Free() {
	rustCall(func(status *C.RustCallStatus) bool {
		C.ffi_zerofs_ffi_rustbuffer_free(cb.inner, status)
		return false
	})
}

func (cb GoRustBuffer) ToGoBytes() []byte {
	return C.GoBytes(unsafe.Pointer(cb.inner.data), C.int(cb.inner.len))
}

func stringToRustBuffer(str string) C.RustBuffer {
	return bytesToRustBuffer([]byte(str))
}

func bytesToRustBuffer(b []byte) C.RustBuffer {
	if len(b) == 0 {
		return C.RustBuffer{}
	}
	// We can pass the pointer along here, as it is pinned
	// for the duration of this call
	foreign := C.ForeignBytes{
		len:  C.int(len(b)),
		data: (*C.uchar)(unsafe.Pointer(&b[0])),
	}

	return rustCall(func(status *C.RustCallStatus) C.RustBuffer {
		return C.ffi_zerofs_ffi_rustbuffer_from_bytes(foreign, status)
	})
}

type BufLifter[GoType any] interface {
	Lift(value RustBufferI) GoType
}

type BufLowerer[GoType any] interface {
	Lower(value GoType) C.RustBuffer
}

type BufReader[GoType any] interface {
	Read(reader io.Reader) GoType
}

type BufWriter[GoType any] interface {
	Write(writer io.Writer, value GoType)
}

func LowerIntoRustBuffer[GoType any](bufWriter BufWriter[GoType], value GoType) C.RustBuffer {
	// This might be not the most efficient way but it does not require knowing allocation size
	// beforehand
	var buffer bytes.Buffer
	bufWriter.Write(&buffer, value)

	bytes, err := io.ReadAll(&buffer)
	if err != nil {
		panic(fmt.Errorf("reading written data: %w", err))
	}
	return bytesToRustBuffer(bytes)
}

func LiftFromRustBuffer[GoType any](bufReader BufReader[GoType], rbuf RustBufferI) GoType {
	defer rbuf.Free()
	reader := rbuf.AsReader()
	item := bufReader.Read(reader)
	if reader.Len() > 0 {
		// TODO: Remove this
		leftover, _ := io.ReadAll(reader)
		panic(fmt.Errorf("Junk remaining in buffer after lifting: %s", string(leftover)))
	}
	return item
}

func rustCallWithError[E any, U any](converter BufReader[E], callback func(*C.RustCallStatus) U) (U, E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[E], status C.RustCallStatus) E {
	switch status.code {
	case 0:
		var zero E
		return zero
	case 1:
		return LiftFromRustBuffer(converter, GoRustBuffer{inner: status.errorBuf})
	case 2:
		// when the rust code sees a panic, it tries to construct a rustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{inner: status.errorBuf})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		panic(fmt.Errorf("unknown status code: %d", status.code))
	}
}

func checkCallStatusUnknown(status C.RustCallStatus) error {
	switch status.code {
	case 0:
		return nil
	case 1:
		panic(fmt.Errorf("function not returning an error returned an error"))
	case 2:
		// when the rust code sees a panic, it tries to construct a C.RustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: status.errorBuf,
			})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		return fmt.Errorf("unknown status code: %d", status.code)
	}
}

func rustCall[U any](callback func(*C.RustCallStatus) U) U {
	returnValue, err := rustCallWithError[error](nil, callback)
	if err != nil {
		panic(err)
	}
	return returnValue
}

type NativeError interface {
	AsError() error
}

func writeInt8(writer io.Writer, value int8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint8(writer io.Writer, value uint8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt16(writer io.Writer, value int16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint16(writer io.Writer, value uint16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt32(writer io.Writer, value int32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint32(writer io.Writer, value uint32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(writer io.Writer, value int64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint64(writer io.Writer, value uint64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat32(writer io.Writer, value float32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat64(writer io.Writer, value float64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func readInt8(reader io.Reader) int8 {
	var result int8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint8(reader io.Reader) uint8 {
	var result uint8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt16(reader io.Reader) int16 {
	var result int16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint16(reader io.Reader) uint16 {
	var result uint16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt32(reader io.Reader) int32 {
	var result int32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint32(reader io.Reader) uint32 {
	var result uint32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt64(reader io.Reader) int64 {
	var result int64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint64(reader io.Reader) uint64 {
	var result uint64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat32(reader io.Reader) float32 {
	var result float32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat64(reader io.Reader) float64 {
	var result float64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func init() {

	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 30
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_zerofs_ffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("zerofs_ffi: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_func_error_to_errno()
		})
		if checksum != 60097 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_func_error_to_errno: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_append()
		})
		if checksum != 21653 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_append: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_canonicalize()
		})
		if checksum != 64250 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_canonicalize: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_capabilities()
		})
		if checksum != 27337 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_capabilities: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_chmod()
		})
		if checksum != 23500 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_chmod: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_chown()
		})
		if checksum != 4020 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_chown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_close()
		})
		if checksum != 8269 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_create()
		})
		if checksum != 32088 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_create: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_create_dir()
		})
		if checksum != 8002 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_create_dir: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_create_dir_all()
		})
		if checksum != 7370 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_create_dir_all: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_exists()
		})
		if checksum != 27112 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_exists: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_hard_link()
		})
		if checksum != 13183 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_hard_link: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_metadata()
		})
		if checksum != 40316 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_mknod()
		})
		if checksum != 30006 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_mknod: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_open()
		})
		if checksum != 46145 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_open: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_open_dir()
		})
		if checksum != 40674 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_open_dir: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_read()
		})
		if checksum != 21483 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_read_dir()
		})
		if checksum != 50342 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_read_dir: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_read_link()
		})
		if checksum != 43608 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_read_link: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_read_range()
		})
		if checksum != 16945 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_read_range: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_remove_dir()
		})
		if checksum != 21868 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_remove_dir: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_remove_dir_all()
		})
		if checksum != 37468 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_remove_dir_all: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_remove_file()
		})
		if checksum != 9940 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_remove_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_rename()
		})
		if checksum != 27969 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_rename: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_set_attr()
		})
		if checksum != 29999 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_set_attr: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_set_times()
		})
		if checksum != 44427 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_set_times: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_stat()
		})
		if checksum != 10640 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_stat: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_statfs()
		})
		if checksum != 56977 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_statfs: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_symlink()
		})
		if checksum != 43553 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_symlink: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_sync()
		})
		if checksum != 3493 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_sync: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_truncate()
		})
		if checksum != 15847 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_truncate: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_client_write()
		})
		if checksum != 2537 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_client_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_close()
		})
		if checksum != 11495 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_create_dir_at()
		})
		if checksum != 52336 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_create_dir_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_link_at()
		})
		if checksum != 14961 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_link_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_metadata()
		})
		if checksum != 4239 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_metadata_at()
		})
		if checksum != 40359 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_metadata_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_mknod_at()
		})
		if checksum != 28494 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_mknod_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_next_batch()
		})
		if checksum != 52557 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_next_batch: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_open_at()
		})
		if checksum != 29465 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_open_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_open_dir_at()
		})
		if checksum != 41514 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_open_dir_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_read_link_at()
		})
		if checksum != 3127 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_read_link_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_remove_dir_at()
		})
		if checksum != 18679 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_remove_dir_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_remove_file_at()
		})
		if checksum != 11396 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_remove_file_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_rename_at()
		})
		if checksum != 64253 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_rename_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_rewind()
		})
		if checksum != 38124 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_rewind: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_set_attr()
		})
		if checksum != 23784 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_set_attr: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_set_attr_at()
		})
		if checksum != 46882 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_set_attr_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_dir_symlink_at()
		})
		if checksum != 7170 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_dir_symlink_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_close()
		})
		if checksum != 58432 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_metadata()
		})
		if checksum != 47610 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_read_at()
		})
		if checksum != 57059 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_read_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_set_attr()
		})
		if checksum != 50984 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_set_attr: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_set_len()
		})
		if checksum != 26323 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_set_len: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_sync_all()
		})
		if checksum != 46219 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_sync_all: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_sync_data()
		})
		if checksum != 22245 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_sync_data: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_method_file_write_at()
		})
		if checksum != 25989 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_method_file_write_at: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_constructor_client_connect()
		})
		if checksum != 40788 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_constructor_client_connect: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_zerofs_ffi_checksum_constructor_client_connect_with()
		})
		if checksum != 33750 {
			// If this happens try cleaning and rebuilding your project
			panic("zerofs_ffi: uniffi_zerofs_ffi_checksum_constructor_client_connect_with: UniFFI API checksum mismatch")
		}
	}
}

type FfiConverterUint32 struct{}

var FfiConverterUint32INSTANCE = FfiConverterUint32{}

func (FfiConverterUint32) Lower(value uint32) C.uint32_t {
	return C.uint32_t(value)
}

func (FfiConverterUint32) Write(writer io.Writer, value uint32) {
	writeUint32(writer, value)
}

func (FfiConverterUint32) Lift(value C.uint32_t) uint32 {
	return uint32(value)
}

func (FfiConverterUint32) Read(reader io.Reader) uint32 {
	return readUint32(reader)
}

type FfiDestroyerUint32 struct{}

func (FfiDestroyerUint32) Destroy(_ uint32) {}

type FfiConverterInt32 struct{}

var FfiConverterInt32INSTANCE = FfiConverterInt32{}

func (FfiConverterInt32) Lower(value int32) C.int32_t {
	return C.int32_t(value)
}

func (FfiConverterInt32) Write(writer io.Writer, value int32) {
	writeInt32(writer, value)
}

func (FfiConverterInt32) Lift(value C.int32_t) int32 {
	return int32(value)
}

func (FfiConverterInt32) Read(reader io.Reader) int32 {
	return readInt32(reader)
}

type FfiDestroyerInt32 struct{}

func (FfiDestroyerInt32) Destroy(_ int32) {}

type FfiConverterUint64 struct{}

var FfiConverterUint64INSTANCE = FfiConverterUint64{}

func (FfiConverterUint64) Lower(value uint64) C.uint64_t {
	return C.uint64_t(value)
}

func (FfiConverterUint64) Write(writer io.Writer, value uint64) {
	writeUint64(writer, value)
}

func (FfiConverterUint64) Lift(value C.uint64_t) uint64 {
	return uint64(value)
}

func (FfiConverterUint64) Read(reader io.Reader) uint64 {
	return readUint64(reader)
}

type FfiDestroyerUint64 struct{}

func (FfiDestroyerUint64) Destroy(_ uint64) {}

type FfiConverterBool struct{}

var FfiConverterBoolINSTANCE = FfiConverterBool{}

func (FfiConverterBool) Lower(value bool) C.int8_t {
	if value {
		return C.int8_t(1)
	}
	return C.int8_t(0)
}

func (FfiConverterBool) Write(writer io.Writer, value bool) {
	if value {
		writeInt8(writer, 1)
	} else {
		writeInt8(writer, 0)
	}
}

func (FfiConverterBool) Lift(value C.int8_t) bool {
	return value != 0
}

func (FfiConverterBool) Read(reader io.Reader) bool {
	return readInt8(reader) != 0
}

type FfiDestroyerBool struct{}

func (FfiDestroyerBool) Destroy(_ bool) {}

type FfiConverterString struct{}

var FfiConverterStringINSTANCE = FfiConverterString{}

func (FfiConverterString) Lift(rb RustBufferI) string {
	defer rb.Free()
	reader := rb.AsReader()
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("reading reader: %w", err))
	}
	return string(b)
}

func (FfiConverterString) Read(reader io.Reader) string {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading string, expected %d, read %d", length, read_length))
	}
	return string(buffer)
}

func (FfiConverterString) Lower(value string) C.RustBuffer {
	return stringToRustBuffer(value)
}

func (c FfiConverterString) LowerExternal(value string) ExternalCRustBuffer {
	return RustBufferFromC(stringToRustBuffer(value))
}

func (FfiConverterString) Write(writer io.Writer, value string) {
	if len(value) > math.MaxInt32 {
		panic("String is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := io.WriteString(writer, value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing string, expected %d, written %d", len(value), write_length))
	}
}

type FfiDestroyerString struct{}

func (FfiDestroyerString) Destroy(_ string) {}

type FfiConverterBytes struct{}

var FfiConverterBytesINSTANCE = FfiConverterBytes{}

func (c FfiConverterBytes) Lower(value []byte) C.RustBuffer {
	return LowerIntoRustBuffer[[]byte](c, value)
}

func (c FfiConverterBytes) LowerExternal(value []byte) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterBytes) Write(writer io.Writer, value []byte) {
	if len(value) > math.MaxInt32 {
		panic("[]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := writer.Write(value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing []byte, expected %d, written %d", len(value), write_length))
	}
}

func (c FfiConverterBytes) Lift(rb RustBufferI) []byte {
	return LiftFromRustBuffer[[]byte](c, rb)
}

func (c FfiConverterBytes) Read(reader io.Reader) []byte {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading []byte, expected %d, read %d", length, read_length))
	}
	return buffer
}

type FfiDestroyerBytes struct{}

func (FfiDestroyerBytes) Destroy(_ []byte) {}

type FfiConverterTimestamp struct{}

var FfiConverterTimestampINSTANCE = FfiConverterTimestamp{}

func (c FfiConverterTimestamp) Lift(rb RustBufferI) time.Time {
	return LiftFromRustBuffer[time.Time](c, rb)
}

func (c FfiConverterTimestamp) Read(reader io.Reader) time.Time {
	sec := readInt64(reader)
	nsec := readUint32(reader)

	var sign int64 = 1
	if sec < 0 {
		sign = -1
	}

	return time.Unix(sec, int64(nsec)*sign)
}

func (c FfiConverterTimestamp) Lower(value time.Time) C.RustBuffer {
	return LowerIntoRustBuffer[time.Time](c, value)
}

func (c FfiConverterTimestamp) LowerExternal(value time.Time) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterTimestamp) Write(writer io.Writer, value time.Time) {
	sec := value.Unix()
	nsec := uint32(value.Nanosecond())
	if value.Unix() < 0 {
		nsec = 1_000_000_000 - nsec
		sec += 1
	}

	writeInt64(writer, sec)
	writeUint32(writer, nsec)
}

type FfiDestroyerTimestamp struct{}

func (FfiDestroyerTimestamp) Destroy(_ time.Time) {}

// Below is an implementation of synchronization requirements outlined in the link.
// https://github.com/mozilla/uniffi-rs/blob/0dc031132d9493ca812c3af6e7dd60ad2ea95bf0/uniffi_bindgen/src/bindings/kotlin/templates/ObjectRuntime.kt#L31

type FfiObject struct {
	handle        C.uint64_t
	callCounter   atomic.Int64
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t
	freeFunction  func(C.uint64_t, *C.RustCallStatus)
	destroyed     atomic.Bool
}

func newFfiObject(
	handle C.uint64_t,
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t,
	freeFunction func(C.uint64_t, *C.RustCallStatus),
) FfiObject {
	return FfiObject{
		handle:        handle,
		cloneFunction: cloneFunction,
		freeFunction:  freeFunction,
	}
}

func (ffiObject *FfiObject) incrementPointer(debugName string) C.uint64_t {
	for {
		counter := ffiObject.callCounter.Load()
		if counter <= -1 {
			panic(fmt.Errorf("%v object has already been destroyed", debugName))
		}
		if counter == math.MaxInt64 {
			panic(fmt.Errorf("%v object call counter would overflow", debugName))
		}
		if ffiObject.callCounter.CompareAndSwap(counter, counter+1) {
			break
		}
	}

	return rustCall(func(status *C.RustCallStatus) C.uint64_t {
		return ffiObject.cloneFunction(ffiObject.handle, status)
	})
}

func (ffiObject *FfiObject) decrementPointer() {
	if ffiObject.callCounter.Add(-1) == -1 {
		ffiObject.freeRustArcPtr()
	}
}

func (ffiObject *FfiObject) destroy() {
	if ffiObject.destroyed.CompareAndSwap(false, true) {
		if ffiObject.callCounter.Add(-1) == -1 {
			ffiObject.freeRustArcPtr()
		}
	}
}

func (ffiObject *FfiObject) freeRustArcPtr() {
	if ffiObject.handle == 0 {
		return
	}
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.handle, status)
		return 0
	})
}

// One ZeroFS session, one identity. Safe to share and call concurrently;
// reconnects transparently.
type ClientInterface interface {
	// Append `data` at end-of-file; returns the offset where it landed.
	Append(path string, data []byte) (uint64, error)
	// Resolve every symlink in `path` and return the canonical path bytes.
	Canonicalize(path string) ([]byte, error)
	// Snapshot of currently negotiated session properties.
	Capabilities() Capabilities
	// Change permission bits.
	Chmod(path string, mode uint32) (Metadata, error)
	// Change owner and/or group (`None` leaves a field untouched).
	Chown(path string, uid *uint32, gid *uint32) (Metadata, error)
	// Mark the client closed and release the session best-effort.
	Close()
	// Shorthand: open read-write with create+truncate, mode 0o644.
	Create(path string) (*File, error)
	// Create a directory; the parent must exist.
	CreateDir(path string, mode uint32) (Metadata, error)
	// Create a directory and any missing ancestors.
	CreateDirAll(path string, mode uint32) error
	// True if the path exists (any file type, no symlink following).
	Exists(path string) (bool, error)
	// Create a hard link at `link` pointing to the inode of `original`.
	HardLink(original string, link string) (Metadata, error)
	// Like `stat` but resolves symlinks (final and intermediate), 40-hop cap.
	Metadata(path string) (Metadata, error)
	// Create a fifo, socket, or device node.
	Mknod(path string, kind NodeKind, mode uint32) (Metadata, error)
	// Open (and optionally create) a file for positioned I/O.
	Open(path string, opts OpenOptions) (*File, error)
	// Open a directory for incremental listing and byte-exact child operations.
	OpenDir(path string) (*Dir, error)
	// Read the entire file into memory.
	Read(path string) ([]byte, error)
	// List a whole directory (`.`/`..` excluded).
	ReadDir(path string) ([]DirEntry, error)
	// Read a symlink target as raw bytes (lossless).
	ReadLink(path string) ([]byte, error)
	// Read up to `len` bytes at `offset`; a shorter result means EOF.
	ReadRange(path string, offset uint64, len uint32) ([]byte, error)
	// Remove an empty directory.
	RemoveDir(path string) error
	// Remove a directory and all its contents, recursively (not atomic).
	RemoveDirAll(path string) error
	// Remove a file, symlink, or device node.
	RemoveFile(path string) error
	// Atomically rename/move within the filesystem; replaces an existing target.
	Rename(from string, to string) error
	// Apply any combination of metadata changes; returns post-change metadata.
	SetAttr(path string, attrs SetAttrs) (Metadata, error)
	// Set access/modification times (`None` leaves a field untouched).
	SetTimes(path string, atime *SetTime, mtime *SetTime) (Metadata, error)
	// Report the entry at `path` without following symlinks.
	Stat(path string) (Metadata, error)
	// Filesystem-wide usage and limits.
	Statfs() (StatFs, error)
	// Create a symlink at `link_path` containing `target` (stored verbatim).
	Symlink(target string, linkPath string) (Metadata, error)
	// Flush to durable (S3-backed) storage (filesystem-global on ZeroFS).
	Sync() error
	// Truncate or extend a file to `size` bytes.
	Truncate(path string, size uint64) (Metadata, error)
	// Create-or-truncate `path` and write all of `data`.
	Write(path string, data []byte) error
}

// One ZeroFS session, one identity. Safe to share and call concurrently;
// reconnects transparently.
type Client struct {
	ffiObject FfiObject
}

// Connect with defaults. Targets: `unix:/sock`, `tcp://host:port`,
// `host:port`, `host` (port 5564), or a bare filesystem path.
func ClientConnect(target string) (*Client, error) {
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Client {
			return FfiConverterClientINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_constructor_client_connect(FfiConverterStringINSTANCE.Lower(target)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Connect with explicit identity, attach target, timeout, and tuning.
func ClientConnectWith(target string, opts ConnectOptions) (*Client, error) {
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Client {
			return FfiConverterClientINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_constructor_client_connect_with(FfiConverterStringINSTANCE.Lower(target), FfiConverterConnectOptionsINSTANCE.Lower(opts)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Append `data` at end-of-file; returns the offset where it landed.
func (_self *Client) Append(path string, data []byte) (uint64, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) uint64 {
			return FfiConverterUint64INSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_append(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterBytesINSTANCE.Lower(data)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Resolve every symlink in `path` and return the canonical path bytes.
func (_self *Client) Canonicalize(path string) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_canonicalize(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Snapshot of currently negotiated session properties.
func (_self *Client) Capabilities() Capabilities {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterCapabilitiesINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_zerofs_ffi_fn_method_client_capabilities(
				_pointer, _uniffiStatus),
		}
	}))
}

// Change permission bits.
func (_self *Client) Chmod(path string, mode uint32) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_chmod(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterUint32INSTANCE.Lower(mode)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Change owner and/or group (`None` leaves a field untouched).
func (_self *Client) Chown(path string, uid *uint32, gid *uint32) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_chown(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterOptionalUint32INSTANCE.Lower(uid), FfiConverterOptionalUint32INSTANCE.Lower(gid)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Mark the client closed and release the session best-effort.
func (_self *Client) Close() {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	uniffiRustCallAsync[error](
		nil,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_close(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

}

// Shorthand: open read-write with create+truncate, mode 0o644.
func (_self *Client) Create(path string) (*File, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *File {
			return FfiConverterFileINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_create(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Create a directory; the parent must exist.
func (_self *Client) CreateDir(path string, mode uint32) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_create_dir(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterUint32INSTANCE.Lower(mode)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Create a directory and any missing ancestors.
func (_self *Client) CreateDirAll(path string, mode uint32) error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_create_dir_all(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterUint32INSTANCE.Lower(mode)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// True if the path exists (any file type, no symlink following).
func (_self *Client) Exists(path string) (bool, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.int8_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_i8(handle, status)
			return res
		},
		// liftFn
		func(ffi C.int8_t) bool {
			return FfiConverterBoolINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_exists(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_i8(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_i8(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Create a hard link at `link` pointing to the inode of `original`.
func (_self *Client) HardLink(original string, link string) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_hard_link(
			_pointer, FfiConverterStringINSTANCE.Lower(original), FfiConverterStringINSTANCE.Lower(link)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Like `stat` but resolves symlinks (final and intermediate), 40-hop cap.
func (_self *Client) Metadata(path string) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_metadata(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Create a fifo, socket, or device node.
func (_self *Client) Mknod(path string, kind NodeKind, mode uint32) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_mknod(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterNodeKindINSTANCE.Lower(kind), FfiConverterUint32INSTANCE.Lower(mode)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Open (and optionally create) a file for positioned I/O.
func (_self *Client) Open(path string, opts OpenOptions) (*File, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *File {
			return FfiConverterFileINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_open(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterOpenOptionsINSTANCE.Lower(opts)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Open a directory for incremental listing and byte-exact child operations.
func (_self *Client) OpenDir(path string) (*Dir, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Dir {
			return FfiConverterDirINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_open_dir(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Read the entire file into memory.
func (_self *Client) Read(path string) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_read(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// List a whole directory (`.`/`..` excluded).
func (_self *Client) ReadDir(path string) ([]DirEntry, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []DirEntry {
			return FfiConverterSequenceDirEntryINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_read_dir(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Read a symlink target as raw bytes (lossless).
func (_self *Client) ReadLink(path string) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_read_link(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Read up to `len` bytes at `offset`; a shorter result means EOF.
func (_self *Client) ReadRange(path string, offset uint64, len uint32) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_read_range(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterUint64INSTANCE.Lower(offset), FfiConverterUint32INSTANCE.Lower(len)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Remove an empty directory.
func (_self *Client) RemoveDir(path string) error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_remove_dir(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Remove a directory and all its contents, recursively (not atomic).
func (_self *Client) RemoveDirAll(path string) error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_remove_dir_all(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Remove a file, symlink, or device node.
func (_self *Client) RemoveFile(path string) error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_remove_file(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Atomically rename/move within the filesystem; replaces an existing target.
func (_self *Client) Rename(from string, to string) error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_rename(
			_pointer, FfiConverterStringINSTANCE.Lower(from), FfiConverterStringINSTANCE.Lower(to)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Apply any combination of metadata changes; returns post-change metadata.
func (_self *Client) SetAttr(path string, attrs SetAttrs) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_set_attr(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterSetAttrsINSTANCE.Lower(attrs)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Set access/modification times (`None` leaves a field untouched).
func (_self *Client) SetTimes(path string, atime *SetTime, mtime *SetTime) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_set_times(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterOptionalSetTimeINSTANCE.Lower(atime), FfiConverterOptionalSetTimeINSTANCE.Lower(mtime)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Report the entry at `path` without following symlinks.
func (_self *Client) Stat(path string) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_stat(
			_pointer, FfiConverterStringINSTANCE.Lower(path)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Filesystem-wide usage and limits.
func (_self *Client) Statfs() (StatFs, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) StatFs {
			return FfiConverterStatFsINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_statfs(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Create a symlink at `link_path` containing `target` (stored verbatim).
func (_self *Client) Symlink(target string, linkPath string) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_symlink(
			_pointer, FfiConverterStringINSTANCE.Lower(target), FfiConverterStringINSTANCE.Lower(linkPath)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Flush to durable (S3-backed) storage (filesystem-global on ZeroFS).
func (_self *Client) Sync() error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_sync(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Truncate or extend a file to `size` bytes.
func (_self *Client) Truncate(path string, size uint64) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_client_truncate(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterUint64INSTANCE.Lower(size)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Create-or-truncate `path` and write all of `data`.
func (_self *Client) Write(path string, data []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*Client")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_client_write(
			_pointer, FfiConverterStringINSTANCE.Lower(path), FfiConverterBytesINSTANCE.Lower(data)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}
func (object *Client) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterClient struct{}

var FfiConverterClientINSTANCE = FfiConverterClient{}

func (c FfiConverterClient) Lift(handle C.uint64_t) *Client {
	result := &Client{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_zerofs_ffi_fn_clone_client(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_zerofs_ffi_fn_free_client(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Client).Destroy)
	return result
}

func (c FfiConverterClient) Read(reader io.Reader) *Client {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterClient) Lower(value *Client) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Client")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterClient) Write(writer io.Writer, value *Client) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalClient(handle uint64) *Client {
	return FfiConverterClientINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalClient(value *Client) uint64 {
	return uint64(FfiConverterClientINSTANCE.Lower(value))
}

type FfiDestroyerClient struct{}

func (_ FfiDestroyerClient) Destroy(value *Client) {
	value.Destroy()
}

// An open directory: a pull-based listing cursor plus `openat`-style child ops.
type DirInterface interface {
	// Mark the handle closed, then clunk best-effort. Idempotent; never hangs.
	Close()
	// mkdirat(2) with explicit mode; returns the new directory's metadata.
	CreateDirAt(name []byte, mode uint32) (Metadata, error)
	// linkat(2): hard-link `original_dir`/`original_name` as `self`/`new_name`.
	LinkAt(originalDir *Dir, originalName []byte, newName []byte) (Metadata, error)
	// Metadata for the directory itself.
	Metadata() (Metadata, error)
	// fstatat(2)-alike; never follows symlinks.
	MetadataAt(name []byte) (Metadata, error)
	// mknodat(2): create a fifo, socket, or device node child.
	MknodAt(name []byte, kind NodeKind, mode uint32) (Metadata, error)
	// Next batch of entries in directory order; `None` max returns one server
	// batch. An empty list means end of directory.
	NextBatch(maxEntries *uint32) ([]DirEntry, error)
	// openat(2)-alike: open (and optionally create) a child file.
	OpenAt(name []byte, opts OpenOptions) (*File, error)
	// Open a child directory (descend without UTF-8).
	OpenDirAt(name []byte) (*Dir, error)
	// readlinkat(2): raw target bytes.
	ReadLinkAt(name []byte) ([]byte, error)
	// unlinkat(2) with AT_REMOVEDIR.
	RemoveDirAt(name []byte) error
	// unlinkat(2).
	RemoveFileAt(name []byte) error
	// renameat(2) across two open directories (`new_dir` may be `self`).
	RenameAt(oldName []byte, newDir *Dir, newName []byte) error
	// Restart iteration from the first entry.
	Rewind() error
	// Apply metadata changes to the directory itself.
	SetAttr(attrs SetAttrs) (Metadata, error)
	// Apply metadata changes to a child without opening it.
	SetAttrAt(name []byte, attrs SetAttrs) (Metadata, error)
	// symlinkat(2): create child `name` containing raw byte `target` verbatim.
	SymlinkAt(name []byte, target []byte) (Metadata, error)
}

// An open directory: a pull-based listing cursor plus `openat`-style child ops.
type Dir struct {
	ffiObject FfiObject
}

// Mark the handle closed, then clunk best-effort. Idempotent; never hangs.
func (_self *Dir) Close() {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	uniffiRustCallAsync[error](
		nil,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_dir_close(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

}

// mkdirat(2) with explicit mode; returns the new directory's metadata.
func (_self *Dir) CreateDirAt(name []byte, mode uint32) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_create_dir_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name), FfiConverterUint32INSTANCE.Lower(mode)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// linkat(2): hard-link `original_dir`/`original_name` as `self`/`new_name`.
func (_self *Dir) LinkAt(originalDir *Dir, originalName []byte, newName []byte) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_link_at(
			_pointer, FfiConverterDirINSTANCE.Lower(originalDir), FfiConverterBytesINSTANCE.Lower(originalName), FfiConverterBytesINSTANCE.Lower(newName)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Metadata for the directory itself.
func (_self *Dir) Metadata() (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_metadata(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// fstatat(2)-alike; never follows symlinks.
func (_self *Dir) MetadataAt(name []byte) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_metadata_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// mknodat(2): create a fifo, socket, or device node child.
func (_self *Dir) MknodAt(name []byte, kind NodeKind, mode uint32) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_mknod_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name), FfiConverterNodeKindINSTANCE.Lower(kind), FfiConverterUint32INSTANCE.Lower(mode)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Next batch of entries in directory order; `None` max returns one server
// batch. An empty list means end of directory.
func (_self *Dir) NextBatch(maxEntries *uint32) ([]DirEntry, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []DirEntry {
			return FfiConverterSequenceDirEntryINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_next_batch(
			_pointer, FfiConverterOptionalUint32INSTANCE.Lower(maxEntries)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// openat(2)-alike: open (and optionally create) a child file.
func (_self *Dir) OpenAt(name []byte, opts OpenOptions) (*File, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *File {
			return FfiConverterFileINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_open_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name), FfiConverterOpenOptionsINSTANCE.Lower(opts)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Open a child directory (descend without UTF-8).
func (_self *Dir) OpenDirAt(name []byte) (*Dir, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_zerofs_ffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Dir {
			return FfiConverterDirINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_open_dir_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// readlinkat(2): raw target bytes.
func (_self *Dir) ReadLinkAt(name []byte) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_read_link_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// unlinkat(2) with AT_REMOVEDIR.
func (_self *Dir) RemoveDirAt(name []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_dir_remove_dir_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// unlinkat(2).
func (_self *Dir) RemoveFileAt(name []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_dir_remove_file_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// renameat(2) across two open directories (`new_dir` may be `self`).
func (_self *Dir) RenameAt(oldName []byte, newDir *Dir, newName []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_dir_rename_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(oldName), FfiConverterDirINSTANCE.Lower(newDir), FfiConverterBytesINSTANCE.Lower(newName)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Restart iteration from the first entry.
func (_self *Dir) Rewind() error {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_dir_rewind(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Apply metadata changes to the directory itself.
func (_self *Dir) SetAttr(attrs SetAttrs) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_set_attr(
			_pointer, FfiConverterSetAttrsINSTANCE.Lower(attrs)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Apply metadata changes to a child without opening it.
func (_self *Dir) SetAttrAt(name []byte, attrs SetAttrs) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_set_attr_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name), FfiConverterSetAttrsINSTANCE.Lower(attrs)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// symlinkat(2): create child `name` containing raw byte `target` verbatim.
func (_self *Dir) SymlinkAt(name []byte, target []byte) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*Dir")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_dir_symlink_at(
			_pointer, FfiConverterBytesINSTANCE.Lower(name), FfiConverterBytesINSTANCE.Lower(target)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *Dir) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDir struct{}

var FfiConverterDirINSTANCE = FfiConverterDir{}

func (c FfiConverterDir) Lift(handle C.uint64_t) *Dir {
	result := &Dir{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_zerofs_ffi_fn_clone_dir(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_zerofs_ffi_fn_free_dir(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Dir).Destroy)
	return result
}

func (c FfiConverterDir) Read(reader io.Reader) *Dir {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDir) Lower(value *Dir) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Dir")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDir) Write(writer io.Writer, value *Dir) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDir(handle uint64) *Dir {
	return FfiConverterDirINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDir(value *Dir) uint64 {
	return uint64(FfiConverterDirINSTANCE.Lower(value))
}

type FfiDestroyerDir struct{}

func (_ FfiDestroyerDir) Destroy(value *Dir) {
	value.Destroy()
}

// An open file. All I/O is positioned; safe to use from many tasks at once.
type FileInterface interface {
	// Mark the handle closed, then clunk best-effort. Idempotent; never hangs.
	Close()
	// Current metadata of this open file (fstat).
	Metadata() (Metadata, error)
	// Read up to `len` bytes at `offset`; a shorter result means EOF.
	ReadAt(offset uint64, len uint32) ([]byte, error)
	// Apply metadata changes through this handle.
	SetAttr(attrs SetAttrs) (Metadata, error)
	// Truncate or extend to `size` bytes.
	SetLen(size uint64) error
	// Flush data and metadata to durable (S3-backed) storage.
	SyncAll() error
	// Flush file data only.
	SyncData() error
	// Write all of `data` at `offset` (any size, chunked internally).
	WriteAt(offset uint64, data []byte) error
}

// An open file. All I/O is positioned; safe to use from many tasks at once.
type File struct {
	ffiObject FfiObject
}

// Mark the handle closed, then clunk best-effort. Idempotent; never hangs.
func (_self *File) Close() {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	uniffiRustCallAsync[error](
		nil,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_file_close(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

}

// Current metadata of this open file (fstat).
func (_self *File) Metadata() (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_file_metadata(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Read up to `len` bytes at `offset`; a shorter result means EOF.
func (_self *File) ReadAt(offset uint64, len uint32) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []byte {
			return FfiConverterBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_file_read_at(
			_pointer, FfiConverterUint64INSTANCE.Lower(offset), FfiConverterUint32INSTANCE.Lower(len)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Apply metadata changes through this handle.
func (_self *File) SetAttr(attrs SetAttrs) (Metadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_zerofs_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) Metadata {
			return FfiConverterMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_zerofs_ffi_fn_method_file_set_attr(
			_pointer, FfiConverterSetAttrsINSTANCE.Lower(attrs)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Truncate or extend to `size` bytes.
func (_self *File) SetLen(size uint64) error {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_file_set_len(
			_pointer, FfiConverterUint64INSTANCE.Lower(size)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Flush data and metadata to durable (S3-backed) storage.
func (_self *File) SyncAll() error {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_file_sync_all(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Flush file data only.
func (_self *File) SyncData() error {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_file_sync_data(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Write all of `data` at `offset` (any size, chunked internally).
func (_self *File) WriteAt(offset uint64, data []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*File")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[*ZeroFsError](
		FfiConverterZeroFsErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_zerofs_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_zerofs_ffi_fn_method_file_write_at(
			_pointer, FfiConverterUint64INSTANCE.Lower(offset), FfiConverterBytesINSTANCE.Lower(data)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_zerofs_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}
func (object *File) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFile struct{}

var FfiConverterFileINSTANCE = FfiConverterFile{}

func (c FfiConverterFile) Lift(handle C.uint64_t) *File {
	result := &File{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_zerofs_ffi_fn_clone_file(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_zerofs_ffi_fn_free_file(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*File).Destroy)
	return result
}

func (c FfiConverterFile) Read(reader io.Reader) *File {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterFile) Lower(value *File) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*File")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterFile) Write(writer io.Writer, value *File) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalFile(handle uint64) *File {
	return FfiConverterFileINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalFile(value *File) uint64 {
	return uint64(FfiConverterFileINSTANCE.Lower(value))
}

type FfiDestroyerFile struct{}

func (_ FfiDestroyerFile) Destroy(value *File) {
	value.Destroy()
}

// Live snapshot of negotiated session properties (may change across reconnects).
type Capabilities struct {
	// ZeroFS v1 extensions active.
	ExtensionsV1 bool
	// ZeroFS v2 extensions active.
	ExtensionsV2 bool
	// Negotiated 9P message size in bytes.
	Msize uint32
	// Largest single-message read payload.
	MaxReadChunk uint32
	// Largest single-message write payload.
	MaxWriteChunk uint32
}

func (r *Capabilities) Destroy() {
	FfiDestroyerBool{}.Destroy(r.ExtensionsV1)
	FfiDestroyerBool{}.Destroy(r.ExtensionsV2)
	FfiDestroyerUint32{}.Destroy(r.Msize)
	FfiDestroyerUint32{}.Destroy(r.MaxReadChunk)
	FfiDestroyerUint32{}.Destroy(r.MaxWriteChunk)
}

type FfiConverterCapabilities struct{}

var FfiConverterCapabilitiesINSTANCE = FfiConverterCapabilities{}

func (c FfiConverterCapabilities) Lift(rb RustBufferI) Capabilities {
	return LiftFromRustBuffer[Capabilities](c, rb)
}

func (c FfiConverterCapabilities) Read(reader io.Reader) Capabilities {
	return Capabilities{
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterCapabilities) Lower(value Capabilities) C.RustBuffer {
	return LowerIntoRustBuffer[Capabilities](c, value)
}

func (c FfiConverterCapabilities) LowerExternal(value Capabilities) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Capabilities](c, value))
}

func (c FfiConverterCapabilities) Write(writer io.Writer, value Capabilities) {
	FfiConverterBoolINSTANCE.Write(writer, value.ExtensionsV1)
	FfiConverterBoolINSTANCE.Write(writer, value.ExtensionsV2)
	FfiConverterUint32INSTANCE.Write(writer, value.Msize)
	FfiConverterUint32INSTANCE.Write(writer, value.MaxReadChunk)
	FfiConverterUint32INSTANCE.Write(writer, value.MaxWriteChunk)
}

type FfiDestroyerCapabilities struct{}

func (_ FfiDestroyerCapabilities) Destroy(value Capabilities) {
	value.Destroy()
}

// Connection identity, attach target, and tuning. Defaults mirror the Rust
// client (`connect_timeout_ms` 30000, `msize` 1 MiB, identity from the process).
type ConnectOptions struct {
	// Numeric uid asserted at attach (`None` = process euid).
	Uid *uint32
	// Group for files created through this client (`None` = process egid).
	Gid *uint32
	// Username sent at attach (`None` = `$USER`); informational.
	Uname *string
	// Attach name (export selector); empty selects the default export.
	Aname string
	// Requested 9P message size.
	Msize uint32
	// Bound on the initial connect+attach in ms; `None` = wait indefinitely.
	ConnectTimeoutMs *uint32
}

func (r *ConnectOptions) Destroy() {
	FfiDestroyerOptionalUint32{}.Destroy(r.Uid)
	FfiDestroyerOptionalUint32{}.Destroy(r.Gid)
	FfiDestroyerOptionalString{}.Destroy(r.Uname)
	FfiDestroyerString{}.Destroy(r.Aname)
	FfiDestroyerUint32{}.Destroy(r.Msize)
	FfiDestroyerOptionalUint32{}.Destroy(r.ConnectTimeoutMs)
}

type FfiConverterConnectOptions struct{}

var FfiConverterConnectOptionsINSTANCE = FfiConverterConnectOptions{}

func (c FfiConverterConnectOptions) Lift(rb RustBufferI) ConnectOptions {
	return LiftFromRustBuffer[ConnectOptions](c, rb)
}

func (c FfiConverterConnectOptions) Read(reader io.Reader) ConnectOptions {
	return ConnectOptions{
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterConnectOptions) Lower(value ConnectOptions) C.RustBuffer {
	return LowerIntoRustBuffer[ConnectOptions](c, value)
}

func (c FfiConverterConnectOptions) LowerExternal(value ConnectOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ConnectOptions](c, value))
}

func (c FfiConverterConnectOptions) Write(writer io.Writer, value ConnectOptions) {
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Uid)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Gid)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.Uname)
	FfiConverterStringINSTANCE.Write(writer, value.Aname)
	FfiConverterUint32INSTANCE.Write(writer, value.Msize)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.ConnectTimeoutMs)
}

type FfiDestroyerConnectOptions struct{}

func (_ FfiDestroyerConnectOptions) Destroy(value ConnectOptions) {
	value.Destroy()
}

// One directory entry; `.` and `..` are filtered out.
type DirEntry struct {
	// Name decoded as UTF-8 (lossy: invalid bytes become U+FFFD).
	Name string
	// Exact on-wire name bytes; feed into the `Dir` `*_at` methods verbatim.
	NameBytes []byte
	// True when `name` losslessly round-trips to `name_bytes`.
	NameIsUtf8 bool
	// Entry type, known without a stat.
	FileType FileType
	// Stable inode number.
	Ino uint64
	// Full metadata when readdirplus is negotiated (v1+); `None` otherwise.
	Metadata *Metadata
}

func (r *DirEntry) Destroy() {
	FfiDestroyerString{}.Destroy(r.Name)
	FfiDestroyerBytes{}.Destroy(r.NameBytes)
	FfiDestroyerBool{}.Destroy(r.NameIsUtf8)
	FfiDestroyerFileType{}.Destroy(r.FileType)
	FfiDestroyerUint64{}.Destroy(r.Ino)
	FfiDestroyerOptionalMetadata{}.Destroy(r.Metadata)
}

type FfiConverterDirEntry struct{}

var FfiConverterDirEntryINSTANCE = FfiConverterDirEntry{}

func (c FfiConverterDirEntry) Lift(rb RustBufferI) DirEntry {
	return LiftFromRustBuffer[DirEntry](c, rb)
}

func (c FfiConverterDirEntry) Read(reader io.Reader) DirEntry {
	return DirEntry{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterFileTypeINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterOptionalMetadataINSTANCE.Read(reader),
	}
}

func (c FfiConverterDirEntry) Lower(value DirEntry) C.RustBuffer {
	return LowerIntoRustBuffer[DirEntry](c, value)
}

func (c FfiConverterDirEntry) LowerExternal(value DirEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DirEntry](c, value))
}

func (c FfiConverterDirEntry) Write(writer io.Writer, value DirEntry) {
	FfiConverterStringINSTANCE.Write(writer, value.Name)
	FfiConverterBytesINSTANCE.Write(writer, value.NameBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.NameIsUtf8)
	FfiConverterFileTypeINSTANCE.Write(writer, value.FileType)
	FfiConverterUint64INSTANCE.Write(writer, value.Ino)
	FfiConverterOptionalMetadataINSTANCE.Write(writer, value.Metadata)
}

type FfiDestroyerDirEntry struct{}

func (_ FfiDestroyerDirEntry) Destroy(value DirEntry) {
	value.Destroy()
}

// POSIX-shaped attributes.
type Metadata struct {
	// Stable inode number (ZeroFS never reuses inode ids).
	Ino uint64
	// File type.
	FileType FileType
	// Full st_mode (type bits + permission bits).
	Mode uint32
	// Hard-link count.
	Nlink uint64
	// Owner uid.
	Uid uint32
	// Owner gid.
	Gid uint32
	// Size in bytes.
	Size uint64
	// Preferred I/O block size reported by the server.
	BlockSize uint64
	// Number of 512-byte blocks allocated.
	Blocks uint64
	// Device id for char/block nodes; 0 otherwise.
	Rdev uint64
	// Last access time.
	Atime time.Time
	// Last modification time.
	Mtime time.Time
	// Last status-change time.
	Ctime time.Time
	// RESERVED: creation time (servers currently report 0 = the epoch).
	Btime time.Time
	// RESERVED: content-change counter (servers currently report 0).
	DataVersion uint64
}

func (r *Metadata) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.Ino)
	FfiDestroyerFileType{}.Destroy(r.FileType)
	FfiDestroyerUint32{}.Destroy(r.Mode)
	FfiDestroyerUint64{}.Destroy(r.Nlink)
	FfiDestroyerUint32{}.Destroy(r.Uid)
	FfiDestroyerUint32{}.Destroy(r.Gid)
	FfiDestroyerUint64{}.Destroy(r.Size)
	FfiDestroyerUint64{}.Destroy(r.BlockSize)
	FfiDestroyerUint64{}.Destroy(r.Blocks)
	FfiDestroyerUint64{}.Destroy(r.Rdev)
	FfiDestroyerTimestamp{}.Destroy(r.Atime)
	FfiDestroyerTimestamp{}.Destroy(r.Mtime)
	FfiDestroyerTimestamp{}.Destroy(r.Ctime)
	FfiDestroyerTimestamp{}.Destroy(r.Btime)
	FfiDestroyerUint64{}.Destroy(r.DataVersion)
}

type FfiConverterMetadata struct{}

var FfiConverterMetadataINSTANCE = FfiConverterMetadata{}

func (c FfiConverterMetadata) Lift(rb RustBufferI) Metadata {
	return LiftFromRustBuffer[Metadata](c, rb)
}

func (c FfiConverterMetadata) Read(reader io.Reader) Metadata {
	return Metadata{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterFileTypeINSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
		FfiConverterTimestampINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterMetadata) Lower(value Metadata) C.RustBuffer {
	return LowerIntoRustBuffer[Metadata](c, value)
}

func (c FfiConverterMetadata) LowerExternal(value Metadata) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Metadata](c, value))
}

func (c FfiConverterMetadata) Write(writer io.Writer, value Metadata) {
	FfiConverterUint64INSTANCE.Write(writer, value.Ino)
	FfiConverterFileTypeINSTANCE.Write(writer, value.FileType)
	FfiConverterUint32INSTANCE.Write(writer, value.Mode)
	FfiConverterUint64INSTANCE.Write(writer, value.Nlink)
	FfiConverterUint32INSTANCE.Write(writer, value.Uid)
	FfiConverterUint32INSTANCE.Write(writer, value.Gid)
	FfiConverterUint64INSTANCE.Write(writer, value.Size)
	FfiConverterUint64INSTANCE.Write(writer, value.BlockSize)
	FfiConverterUint64INSTANCE.Write(writer, value.Blocks)
	FfiConverterUint64INSTANCE.Write(writer, value.Rdev)
	FfiConverterTimestampINSTANCE.Write(writer, value.Atime)
	FfiConverterTimestampINSTANCE.Write(writer, value.Mtime)
	FfiConverterTimestampINSTANCE.Write(writer, value.Ctime)
	FfiConverterTimestampINSTANCE.Write(writer, value.Btime)
	FfiConverterUint64INSTANCE.Write(writer, value.DataVersion)
}

type FfiDestroyerMetadata struct{}

func (_ FfiDestroyerMetadata) Destroy(value Metadata) {
	value.Destroy()
}

// Options for opening (and optionally creating) a file. Defaults: all flags
// false, mode 0o644.
type OpenOptions struct {
	// Open for reading.
	Read bool
	// Open for writing.
	Write bool
	// Open-or-create.
	Create bool
	// Fail unless this call creates the file (the atomic exclusive primitive).
	CreateNew bool
	// Truncate to zero length on open.
	Truncate bool
	// Permission bits when the open creates the file.
	Mode uint32
}

func (r *OpenOptions) Destroy() {
	FfiDestroyerBool{}.Destroy(r.Read)
	FfiDestroyerBool{}.Destroy(r.Write)
	FfiDestroyerBool{}.Destroy(r.Create)
	FfiDestroyerBool{}.Destroy(r.CreateNew)
	FfiDestroyerBool{}.Destroy(r.Truncate)
	FfiDestroyerUint32{}.Destroy(r.Mode)
}

type FfiConverterOpenOptions struct{}

var FfiConverterOpenOptionsINSTANCE = FfiConverterOpenOptions{}

func (c FfiConverterOpenOptions) Lift(rb RustBufferI) OpenOptions {
	return LiftFromRustBuffer[OpenOptions](c, rb)
}

func (c FfiConverterOpenOptions) Read(reader io.Reader) OpenOptions {
	return OpenOptions{
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterOpenOptions) Lower(value OpenOptions) C.RustBuffer {
	return LowerIntoRustBuffer[OpenOptions](c, value)
}

func (c FfiConverterOpenOptions) LowerExternal(value OpenOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[OpenOptions](c, value))
}

func (c FfiConverterOpenOptions) Write(writer io.Writer, value OpenOptions) {
	FfiConverterBoolINSTANCE.Write(writer, value.Read)
	FfiConverterBoolINSTANCE.Write(writer, value.Write)
	FfiConverterBoolINSTANCE.Write(writer, value.Create)
	FfiConverterBoolINSTANCE.Write(writer, value.CreateNew)
	FfiConverterBoolINSTANCE.Write(writer, value.Truncate)
	FfiConverterUint32INSTANCE.Write(writer, value.Mode)
}

type FfiDestroyerOpenOptions struct{}

func (_ FfiDestroyerOpenOptions) Destroy(value OpenOptions) {
	value.Destroy()
}

// Metadata changes; `None` fields are untouched. All-`None` is a no-op.
type SetAttrs struct {
	// New permission bits (low 12 bits used).
	Mode *uint32
	// New owner uid.
	Uid *uint32
	// New owner gid.
	Gid *uint32
	// New length (truncates or extends).
	Size *uint64
	// New access time.
	Atime *SetTime
	// New modification time.
	Mtime *SetTime
}

func (r *SetAttrs) Destroy() {
	FfiDestroyerOptionalUint32{}.Destroy(r.Mode)
	FfiDestroyerOptionalUint32{}.Destroy(r.Uid)
	FfiDestroyerOptionalUint32{}.Destroy(r.Gid)
	FfiDestroyerOptionalUint64{}.Destroy(r.Size)
	FfiDestroyerOptionalSetTime{}.Destroy(r.Atime)
	FfiDestroyerOptionalSetTime{}.Destroy(r.Mtime)
}

type FfiConverterSetAttrs struct{}

var FfiConverterSetAttrsINSTANCE = FfiConverterSetAttrs{}

func (c FfiConverterSetAttrs) Lift(rb RustBufferI) SetAttrs {
	return LiftFromRustBuffer[SetAttrs](c, rb)
}

func (c FfiConverterSetAttrs) Read(reader io.Reader) SetAttrs {
	return SetAttrs{
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
		FfiConverterOptionalUint64INSTANCE.Read(reader),
		FfiConverterOptionalSetTimeINSTANCE.Read(reader),
		FfiConverterOptionalSetTimeINSTANCE.Read(reader),
	}
}

func (c FfiConverterSetAttrs) Lower(value SetAttrs) C.RustBuffer {
	return LowerIntoRustBuffer[SetAttrs](c, value)
}

func (c FfiConverterSetAttrs) LowerExternal(value SetAttrs) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SetAttrs](c, value))
}

func (c FfiConverterSetAttrs) Write(writer io.Writer, value SetAttrs) {
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Mode)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Uid)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Gid)
	FfiConverterOptionalUint64INSTANCE.Write(writer, value.Size)
	FfiConverterOptionalSetTimeINSTANCE.Write(writer, value.Atime)
	FfiConverterOptionalSetTimeINSTANCE.Write(writer, value.Mtime)
}

type FfiDestroyerSetAttrs struct{}

func (_ FfiDestroyerSetAttrs) Destroy(value SetAttrs) {
	value.Destroy()
}

// Filesystem usage and limits.
type StatFs struct {
	// Optimal transfer block size.
	BlockSize uint32
	// Total data blocks in the filesystem.
	Blocks uint64
	// Free blocks.
	BlocksFree uint64
	// Free blocks available to unprivileged users.
	BlocksAvailable uint64
	// Total inodes (files).
	Files uint64
	// Free inodes.
	FilesFree uint64
	// Filesystem id.
	FilesystemId uint64
	// Maximum filename length.
	MaxNameLen uint32
}

func (r *StatFs) Destroy() {
	FfiDestroyerUint32{}.Destroy(r.BlockSize)
	FfiDestroyerUint64{}.Destroy(r.Blocks)
	FfiDestroyerUint64{}.Destroy(r.BlocksFree)
	FfiDestroyerUint64{}.Destroy(r.BlocksAvailable)
	FfiDestroyerUint64{}.Destroy(r.Files)
	FfiDestroyerUint64{}.Destroy(r.FilesFree)
	FfiDestroyerUint64{}.Destroy(r.FilesystemId)
	FfiDestroyerUint32{}.Destroy(r.MaxNameLen)
}

type FfiConverterStatFs struct{}

var FfiConverterStatFsINSTANCE = FfiConverterStatFs{}

func (c FfiConverterStatFs) Lift(rb RustBufferI) StatFs {
	return LiftFromRustBuffer[StatFs](c, rb)
}

func (c FfiConverterStatFs) Read(reader io.Reader) StatFs {
	return StatFs{
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterStatFs) Lower(value StatFs) C.RustBuffer {
	return LowerIntoRustBuffer[StatFs](c, value)
}

func (c FfiConverterStatFs) LowerExternal(value StatFs) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[StatFs](c, value))
}

func (c FfiConverterStatFs) Write(writer io.Writer, value StatFs) {
	FfiConverterUint32INSTANCE.Write(writer, value.BlockSize)
	FfiConverterUint64INSTANCE.Write(writer, value.Blocks)
	FfiConverterUint64INSTANCE.Write(writer, value.BlocksFree)
	FfiConverterUint64INSTANCE.Write(writer, value.BlocksAvailable)
	FfiConverterUint64INSTANCE.Write(writer, value.Files)
	FfiConverterUint64INSTANCE.Write(writer, value.FilesFree)
	FfiConverterUint64INSTANCE.Write(writer, value.FilesystemId)
	FfiConverterUint32INSTANCE.Write(writer, value.MaxNameLen)
}

type FfiDestroyerStatFs struct{}

func (_ FfiDestroyerStatFs) Destroy(value StatFs) {
	value.Destroy()
}

// File type derived from the mode/dirent type.
type FileType uint

const (
	// Regular file.
	FileTypeFile FileType = 1
	// Directory.
	FileTypeDir FileType = 2
	// Symbolic link.
	FileTypeSymlink FileType = 3
	// Named pipe (FIFO).
	FileTypeFifo FileType = 4
	// Unix-domain socket node.
	FileTypeSocket FileType = 5
	// Character device.
	FileTypeCharDevice FileType = 6
	// Block device.
	FileTypeBlockDevice FileType = 7
	// Unrecognized type.
	FileTypeUnknown FileType = 8
)

type FfiConverterFileType struct{}

var FfiConverterFileTypeINSTANCE = FfiConverterFileType{}

func (c FfiConverterFileType) Lift(rb RustBufferI) FileType {
	return LiftFromRustBuffer[FileType](c, rb)
}

func (c FfiConverterFileType) Lower(value FileType) C.RustBuffer {
	return LowerIntoRustBuffer[FileType](c, value)
}

func (c FfiConverterFileType) LowerExternal(value FileType) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FileType](c, value))
}
func (FfiConverterFileType) Read(reader io.Reader) FileType {
	id := readInt32(reader)
	return FileType(id)
}

func (FfiConverterFileType) Write(writer io.Writer, value FileType) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFileType struct{}

func (_ FfiDestroyerFileType) Destroy(value FileType) {
}

// Kind of special node for `mknod`.
type NodeKind interface {
	Destroy()
}

// Named pipe (FIFO).
type NodeKindFifo struct {
}

func (e NodeKindFifo) Destroy() {
}

// Unix-domain socket node.
type NodeKindSocket struct {
}

func (e NodeKindSocket) Destroy() {
}

// Block device with the given major/minor numbers.
type NodeKindBlockDevice struct {
	Major uint32
	Minor uint32
}

func (e NodeKindBlockDevice) Destroy() {
	FfiDestroyerUint32{}.Destroy(e.Major)
	FfiDestroyerUint32{}.Destroy(e.Minor)
}

// Character device with the given major/minor numbers.
type NodeKindCharDevice struct {
	Major uint32
	Minor uint32
}

func (e NodeKindCharDevice) Destroy() {
	FfiDestroyerUint32{}.Destroy(e.Major)
	FfiDestroyerUint32{}.Destroy(e.Minor)
}

type FfiConverterNodeKind struct{}

var FfiConverterNodeKindINSTANCE = FfiConverterNodeKind{}

func (c FfiConverterNodeKind) Lift(rb RustBufferI) NodeKind {
	return LiftFromRustBuffer[NodeKind](c, rb)
}

func (c FfiConverterNodeKind) Lower(value NodeKind) C.RustBuffer {
	return LowerIntoRustBuffer[NodeKind](c, value)
}

func (c FfiConverterNodeKind) LowerExternal(value NodeKind) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[NodeKind](c, value))
}
func (FfiConverterNodeKind) Read(reader io.Reader) NodeKind {
	id := readInt32(reader)
	switch id {
	case 1:
		return NodeKindFifo{}
	case 2:
		return NodeKindSocket{}
	case 3:
		return NodeKindBlockDevice{
			FfiConverterUint32INSTANCE.Read(reader),
			FfiConverterUint32INSTANCE.Read(reader),
		}
	case 4:
		return NodeKindCharDevice{
			FfiConverterUint32INSTANCE.Read(reader),
			FfiConverterUint32INSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterNodeKind.Read()", id))
	}
}

func (FfiConverterNodeKind) Write(writer io.Writer, value NodeKind) {
	switch variant_value := value.(type) {
	case NodeKindFifo:
		writeInt32(writer, 1)
	case NodeKindSocket:
		writeInt32(writer, 2)
	case NodeKindBlockDevice:
		writeInt32(writer, 3)
		FfiConverterUint32INSTANCE.Write(writer, variant_value.Major)
		FfiConverterUint32INSTANCE.Write(writer, variant_value.Minor)
	case NodeKindCharDevice:
		writeInt32(writer, 4)
		FfiConverterUint32INSTANCE.Write(writer, variant_value.Major)
		FfiConverterUint32INSTANCE.Write(writer, variant_value.Minor)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterNodeKind.Write", value))
	}
}

type FfiDestroyerNodeKind struct{}

func (_ FfiDestroyerNodeKind) Destroy(value NodeKind) {
	value.Destroy()
}

// A time to set: the server's current clock, or an explicit instant.
type SetTime interface {
	Destroy()
}

// Use the server's current clock.
type SetTimeNow struct {
}

func (e SetTimeNow) Destroy() {
}

// Set to this explicit instant.
type SetTimeAt struct {
	Time time.Time
}

func (e SetTimeAt) Destroy() {
	FfiDestroyerTimestamp{}.Destroy(e.Time)
}

type FfiConverterSetTime struct{}

var FfiConverterSetTimeINSTANCE = FfiConverterSetTime{}

func (c FfiConverterSetTime) Lift(rb RustBufferI) SetTime {
	return LiftFromRustBuffer[SetTime](c, rb)
}

func (c FfiConverterSetTime) Lower(value SetTime) C.RustBuffer {
	return LowerIntoRustBuffer[SetTime](c, value)
}

func (c FfiConverterSetTime) LowerExternal(value SetTime) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SetTime](c, value))
}
func (FfiConverterSetTime) Read(reader io.Reader) SetTime {
	id := readInt32(reader)
	switch id {
	case 1:
		return SetTimeNow{}
	case 2:
		return SetTimeAt{
			FfiConverterTimestampINSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterSetTime.Read()", id))
	}
}

func (FfiConverterSetTime) Write(writer io.Writer, value SetTime) {
	switch variant_value := value.(type) {
	case SetTimeNow:
		writeInt32(writer, 1)
	case SetTimeAt:
		writeInt32(writer, 2)
		FfiConverterTimestampINSTANCE.Write(writer, variant_value.Time)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterSetTime.Write", value))
	}
}

type FfiDestroyerSetTime struct{}

func (_ FfiDestroyerSetTime) Destroy(value SetTime) {
	value.Destroy()
}

// The single error type, flat and exhaustive. The variant↔errno mapping is
// 1:1; any other server errno surfaces as [`ZeroFsError::Io`].
type ZeroFsError struct {
	err error
}

// Convience method to turn *ZeroFsError into error
// Avoiding treating nil pointer as non nil error interface
func (err *ZeroFsError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err ZeroFsError) Error() string {
	return fmt.Sprintf("ZeroFsError: %s", err.err.Error())
}

func (err ZeroFsError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrZeroFsErrorNotFound = fmt.Errorf("ZeroFsErrorNotFound")
var ErrZeroFsErrorPermissionDenied = fmt.Errorf("ZeroFsErrorPermissionDenied")
var ErrZeroFsErrorNotPermitted = fmt.Errorf("ZeroFsErrorNotPermitted")
var ErrZeroFsErrorAlreadyExists = fmt.Errorf("ZeroFsErrorAlreadyExists")
var ErrZeroFsErrorNotADirectory = fmt.Errorf("ZeroFsErrorNotADirectory")
var ErrZeroFsErrorIsADirectory = fmt.Errorf("ZeroFsErrorIsADirectory")
var ErrZeroFsErrorDirectoryNotEmpty = fmt.Errorf("ZeroFsErrorDirectoryNotEmpty")
var ErrZeroFsErrorNameTooLong = fmt.Errorf("ZeroFsErrorNameTooLong")
var ErrZeroFsErrorInvalidArgument = fmt.Errorf("ZeroFsErrorInvalidArgument")
var ErrZeroFsErrorTooManySymlinks = fmt.Errorf("ZeroFsErrorTooManySymlinks")
var ErrZeroFsErrorClosed = fmt.Errorf("ZeroFsErrorClosed")
var ErrZeroFsErrorConnectFailed = fmt.Errorf("ZeroFsErrorConnectFailed")
var ErrZeroFsErrorNotLeader = fmt.Errorf("ZeroFsErrorNotLeader")
var ErrZeroFsErrorStale = fmt.Errorf("ZeroFsErrorStale")
var ErrZeroFsErrorIo = fmt.Errorf("ZeroFsErrorIo")
var ErrZeroFsErrorProtocol = fmt.Errorf("ZeroFsErrorProtocol")

// Variant structs
// No entry at the path (ENOENT).
type ZeroFsErrorNotFound struct {
	Path string
}

// No entry at the path (ENOENT).
func NewZeroFsErrorNotFound(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorNotFound{
		Path: path}}
}

func (e ZeroFsErrorNotFound) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorNotFound) Error() string {
	return fmt.Sprint("NotFound",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorNotFound) Is(target error) bool {
	return target == ErrZeroFsErrorNotFound
}

// Access denied by permission bits (EACCES).
type ZeroFsErrorPermissionDenied struct {
	Path string
}

// Access denied by permission bits (EACCES).
func NewZeroFsErrorPermissionDenied(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorPermissionDenied{
		Path: path}}
}

func (e ZeroFsErrorPermissionDenied) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorPermissionDenied) Error() string {
	return fmt.Sprint("PermissionDenied",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorPermissionDenied) Is(target error) bool {
	return target == ErrZeroFsErrorPermissionDenied
}

// The operation requires ownership or privilege (EPERM).
type ZeroFsErrorNotPermitted struct {
	Path string
}

// The operation requires ownership or privilege (EPERM).
func NewZeroFsErrorNotPermitted(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorNotPermitted{
		Path: path}}
}

func (e ZeroFsErrorNotPermitted) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorNotPermitted) Error() string {
	return fmt.Sprint("NotPermitted",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorNotPermitted) Is(target error) bool {
	return target == ErrZeroFsErrorNotPermitted
}

// The target already exists (EEXIST).
type ZeroFsErrorAlreadyExists struct {
	Path string
}

// The target already exists (EEXIST).
func NewZeroFsErrorAlreadyExists(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorAlreadyExists{
		Path: path}}
}

func (e ZeroFsErrorAlreadyExists) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorAlreadyExists) Error() string {
	return fmt.Sprint("AlreadyExists",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorAlreadyExists) Is(target error) bool {
	return target == ErrZeroFsErrorAlreadyExists
}

// A path component is not a directory (ENOTDIR).
type ZeroFsErrorNotADirectory struct {
	Path string
}

// A path component is not a directory (ENOTDIR).
func NewZeroFsErrorNotADirectory(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorNotADirectory{
		Path: path}}
}

func (e ZeroFsErrorNotADirectory) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorNotADirectory) Error() string {
	return fmt.Sprint("NotADirectory",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorNotADirectory) Is(target error) bool {
	return target == ErrZeroFsErrorNotADirectory
}

// The target is a directory where a non-directory was required (EISDIR).
type ZeroFsErrorIsADirectory struct {
	Path string
}

// The target is a directory where a non-directory was required (EISDIR).
func NewZeroFsErrorIsADirectory(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorIsADirectory{
		Path: path}}
}

func (e ZeroFsErrorIsADirectory) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorIsADirectory) Error() string {
	return fmt.Sprint("IsADirectory",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorIsADirectory) Is(target error) bool {
	return target == ErrZeroFsErrorIsADirectory
}

// A directory removal found the directory non-empty (ENOTEMPTY).
type ZeroFsErrorDirectoryNotEmpty struct {
	Path string
}

// A directory removal found the directory non-empty (ENOTEMPTY).
func NewZeroFsErrorDirectoryNotEmpty(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorDirectoryNotEmpty{
		Path: path}}
}

func (e ZeroFsErrorDirectoryNotEmpty) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorDirectoryNotEmpty) Error() string {
	return fmt.Sprint("DirectoryNotEmpty",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorDirectoryNotEmpty) Is(target error) bool {
	return target == ErrZeroFsErrorDirectoryNotEmpty
}

// A name exceeds 255 bytes (ENAMETOOLONG).
type ZeroFsErrorNameTooLong struct {
	Name string
}

// A name exceeds 255 bytes (ENAMETOOLONG).
func NewZeroFsErrorNameTooLong(
	name string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorNameTooLong{
		Name: name}}
}

func (e ZeroFsErrorNameTooLong) destroy() {
	FfiDestroyerString{}.Destroy(e.Name)
}

func (err ZeroFsErrorNameTooLong) Error() string {
	return fmt.Sprint("NameTooLong",
		": ",

		"Name=",
		err.Name,
	)
}

func (self ZeroFsErrorNameTooLong) Is(target error) bool {
	return target == ErrZeroFsErrorNameTooLong
}

// Bad input, client-side or EINVAL from the server.
type ZeroFsErrorInvalidArgument struct {
	Message string
}

// Bad input, client-side or EINVAL from the server.
func NewZeroFsErrorInvalidArgument(
	message string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorInvalidArgument{
		Message: message}}
}

func (e ZeroFsErrorInvalidArgument) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ZeroFsErrorInvalidArgument) Error() string {
	return fmt.Sprint("InvalidArgument",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ZeroFsErrorInvalidArgument) Is(target error) bool {
	return target == ErrZeroFsErrorInvalidArgument
}

// Symlink resolution exceeded the 40-hop cap (ELOOP).
type ZeroFsErrorTooManySymlinks struct {
	Path string
}

// Symlink resolution exceeded the 40-hop cap (ELOOP).
func NewZeroFsErrorTooManySymlinks(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorTooManySymlinks{
		Path: path}}
}

func (e ZeroFsErrorTooManySymlinks) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorTooManySymlinks) Error() string {
	return fmt.Sprint("TooManySymlinks",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorTooManySymlinks) Is(target error) bool {
	return target == ErrZeroFsErrorTooManySymlinks
}

// Handle or client used after `close()` (EBADF).
type ZeroFsErrorClosed struct {
}

// Handle or client used after `close()` (EBADF).
func NewZeroFsErrorClosed() *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorClosed{}}
}

func (e ZeroFsErrorClosed) destroy() {
}

func (err ZeroFsErrorClosed) Error() string {
	return fmt.Sprint("Closed")
}

func (self ZeroFsErrorClosed) Is(target error) bool {
	return target == ErrZeroFsErrorClosed
}

// The initial connection or attach failed.
type ZeroFsErrorConnectFailed struct {
	Message string
}

// The initial connection or attach failed.
func NewZeroFsErrorConnectFailed(
	message string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorConnectFailed{
		Message: message}}
}

func (e ZeroFsErrorConnectFailed) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ZeroFsErrorConnectFailed) Error() string {
	return fmt.Sprint("ConnectFailed",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ZeroFsErrorConnectFailed) Is(target error) bool {
	return target == ErrZeroFsErrorConnectFailed
}

// The node is no longer the HA leader (P9_ENOTLEADER); a re-route + retry is
// safe, which a failover client does transparently.
type ZeroFsErrorNotLeader struct {
	Path string
}

// The node is no longer the HA leader (P9_ENOTLEADER); a re-route + retry is
// safe, which a failover client does transparently.
func NewZeroFsErrorNotLeader(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorNotLeader{
		Path: path}}
}

func (e ZeroFsErrorNotLeader) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorNotLeader) Error() string {
	return fmt.Sprint("NotLeader",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorNotLeader) Is(target error) bool {
	return target == ErrZeroFsErrorNotLeader
}

// Stale handle (ESTALE). From `sync_all`/`sync_data` this is the durability signal:
// the `.zerofs4` lineage broke, so writes acked on this handle before the fsync may
// not be durable; redo them and fsync again. From other operations it is a plain
// stale inode/handle (re-open the path).
type ZeroFsErrorStale struct {
	Path string
}

// Stale handle (ESTALE). From `sync_all`/`sync_data` this is the durability signal:
// the `.zerofs4` lineage broke, so writes acked on this handle before the fsync may
// not be durable; redo them and fsync again. From other operations it is a plain
// stale inode/handle (re-open the path).
func NewZeroFsErrorStale(
	path string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorStale{
		Path: path}}
}

func (e ZeroFsErrorStale) destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

func (err ZeroFsErrorStale) Error() string {
	return fmt.Sprint("Stale",
		": ",

		"Path=",
		err.Path,
	)
}

func (self ZeroFsErrorStale) Is(target error) bool {
	return target == ErrZeroFsErrorStale
}

// Any other server errno, preserved verbatim.
type ZeroFsErrorIo struct {
	Errno   int32
	Path    string
	Message string
}

// Any other server errno, preserved verbatim.
func NewZeroFsErrorIo(
	errno int32,
	path string,
	message string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorIo{
		Errno:   errno,
		Path:    path,
		Message: message}}
}

func (e ZeroFsErrorIo) destroy() {
	FfiDestroyerInt32{}.Destroy(e.Errno)
	FfiDestroyerString{}.Destroy(e.Path)
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ZeroFsErrorIo) Error() string {
	return fmt.Sprint("Io",
		": ",

		"Errno=",
		err.Errno,
		", ",
		"Path=",
		err.Path,
		", ",
		"Message=",
		err.Message,
	)
}

func (self ZeroFsErrorIo) Is(target error) bool {
	return target == ErrZeroFsErrorIo
}

// Wire-level failure: codec error, unexpected reply, failed negotiation.
type ZeroFsErrorProtocol struct {
	Message string
}

// Wire-level failure: codec error, unexpected reply, failed negotiation.
func NewZeroFsErrorProtocol(
	message string,
) *ZeroFsError {
	return &ZeroFsError{err: &ZeroFsErrorProtocol{
		Message: message}}
}

func (e ZeroFsErrorProtocol) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ZeroFsErrorProtocol) Error() string {
	return fmt.Sprint("Protocol",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ZeroFsErrorProtocol) Is(target error) bool {
	return target == ErrZeroFsErrorProtocol
}

type FfiConverterZeroFsError struct{}

var FfiConverterZeroFsErrorINSTANCE = FfiConverterZeroFsError{}

func (c FfiConverterZeroFsError) Lift(eb RustBufferI) *ZeroFsError {
	return LiftFromRustBuffer[*ZeroFsError](c, eb)
}

func (c FfiConverterZeroFsError) Lower(value *ZeroFsError) C.RustBuffer {
	return LowerIntoRustBuffer[*ZeroFsError](c, value)
}

func (c FfiConverterZeroFsError) LowerExternal(value *ZeroFsError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ZeroFsError](c, value))
}

func (c FfiConverterZeroFsError) Read(reader io.Reader) *ZeroFsError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &ZeroFsError{&ZeroFsErrorNotFound{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &ZeroFsError{&ZeroFsErrorPermissionDenied{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 3:
		return &ZeroFsError{&ZeroFsErrorNotPermitted{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 4:
		return &ZeroFsError{&ZeroFsErrorAlreadyExists{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 5:
		return &ZeroFsError{&ZeroFsErrorNotADirectory{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 6:
		return &ZeroFsError{&ZeroFsErrorIsADirectory{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 7:
		return &ZeroFsError{&ZeroFsErrorDirectoryNotEmpty{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 8:
		return &ZeroFsError{&ZeroFsErrorNameTooLong{
			Name: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 9:
		return &ZeroFsError{&ZeroFsErrorInvalidArgument{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 10:
		return &ZeroFsError{&ZeroFsErrorTooManySymlinks{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 11:
		return &ZeroFsError{&ZeroFsErrorClosed{}}
	case 12:
		return &ZeroFsError{&ZeroFsErrorConnectFailed{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 13:
		return &ZeroFsError{&ZeroFsErrorNotLeader{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 14:
		return &ZeroFsError{&ZeroFsErrorStale{
			Path: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 15:
		return &ZeroFsError{&ZeroFsErrorIo{
			Errno:   FfiConverterInt32INSTANCE.Read(reader),
			Path:    FfiConverterStringINSTANCE.Read(reader),
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 16:
		return &ZeroFsError{&ZeroFsErrorProtocol{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterZeroFsError.Read()", errorID))
	}
}

func (c FfiConverterZeroFsError) Write(writer io.Writer, value *ZeroFsError) {
	switch variantValue := value.err.(type) {
	case *ZeroFsErrorNotFound:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorPermissionDenied:
		writeInt32(writer, 2)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorNotPermitted:
		writeInt32(writer, 3)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorAlreadyExists:
		writeInt32(writer, 4)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorNotADirectory:
		writeInt32(writer, 5)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorIsADirectory:
		writeInt32(writer, 6)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorDirectoryNotEmpty:
		writeInt32(writer, 7)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorNameTooLong:
		writeInt32(writer, 8)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Name)
	case *ZeroFsErrorInvalidArgument:
		writeInt32(writer, 9)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ZeroFsErrorTooManySymlinks:
		writeInt32(writer, 10)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorClosed:
		writeInt32(writer, 11)
	case *ZeroFsErrorConnectFailed:
		writeInt32(writer, 12)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ZeroFsErrorNotLeader:
		writeInt32(writer, 13)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorStale:
		writeInt32(writer, 14)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
	case *ZeroFsErrorIo:
		writeInt32(writer, 15)
		FfiConverterInt32INSTANCE.Write(writer, variantValue.Errno)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Path)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ZeroFsErrorProtocol:
		writeInt32(writer, 16)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterZeroFsError.Write", value))
	}
}

type FfiDestroyerZeroFsError struct{}

func (_ FfiDestroyerZeroFsError) Destroy(value *ZeroFsError) {
	switch variantValue := value.err.(type) {
	case ZeroFsErrorNotFound:
		variantValue.destroy()
	case ZeroFsErrorPermissionDenied:
		variantValue.destroy()
	case ZeroFsErrorNotPermitted:
		variantValue.destroy()
	case ZeroFsErrorAlreadyExists:
		variantValue.destroy()
	case ZeroFsErrorNotADirectory:
		variantValue.destroy()
	case ZeroFsErrorIsADirectory:
		variantValue.destroy()
	case ZeroFsErrorDirectoryNotEmpty:
		variantValue.destroy()
	case ZeroFsErrorNameTooLong:
		variantValue.destroy()
	case ZeroFsErrorInvalidArgument:
		variantValue.destroy()
	case ZeroFsErrorTooManySymlinks:
		variantValue.destroy()
	case ZeroFsErrorClosed:
		variantValue.destroy()
	case ZeroFsErrorConnectFailed:
		variantValue.destroy()
	case ZeroFsErrorNotLeader:
		variantValue.destroy()
	case ZeroFsErrorStale:
		variantValue.destroy()
	case ZeroFsErrorIo:
		variantValue.destroy()
	case ZeroFsErrorProtocol:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerZeroFsError.Destroy", value))
	}
}

type FfiConverterOptionalUint32 struct{}

var FfiConverterOptionalUint32INSTANCE = FfiConverterOptionalUint32{}

func (c FfiConverterOptionalUint32) Lift(rb RustBufferI) *uint32 {
	return LiftFromRustBuffer[*uint32](c, rb)
}

func (_ FfiConverterOptionalUint32) Read(reader io.Reader) *uint32 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint32INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint32) Lower(value *uint32) C.RustBuffer {
	return LowerIntoRustBuffer[*uint32](c, value)
}

func (c FfiConverterOptionalUint32) LowerExternal(value *uint32) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint32](c, value))
}

func (_ FfiConverterOptionalUint32) Write(writer io.Writer, value *uint32) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint32INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint32 struct{}

func (_ FfiDestroyerOptionalUint32) Destroy(value *uint32) {
	if value != nil {
		FfiDestroyerUint32{}.Destroy(*value)
	}
}

type FfiConverterOptionalUint64 struct{}

var FfiConverterOptionalUint64INSTANCE = FfiConverterOptionalUint64{}

func (c FfiConverterOptionalUint64) Lift(rb RustBufferI) *uint64 {
	return LiftFromRustBuffer[*uint64](c, rb)
}

func (_ FfiConverterOptionalUint64) Read(reader io.Reader) *uint64 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint64INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint64) Lower(value *uint64) C.RustBuffer {
	return LowerIntoRustBuffer[*uint64](c, value)
}

func (c FfiConverterOptionalUint64) LowerExternal(value *uint64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint64](c, value))
}

func (_ FfiConverterOptionalUint64) Write(writer io.Writer, value *uint64) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint64INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint64 struct{}

func (_ FfiDestroyerOptionalUint64) Destroy(value *uint64) {
	if value != nil {
		FfiDestroyerUint64{}.Destroy(*value)
	}
}

type FfiConverterOptionalString struct{}

var FfiConverterOptionalStringINSTANCE = FfiConverterOptionalString{}

func (c FfiConverterOptionalString) Lift(rb RustBufferI) *string {
	return LiftFromRustBuffer[*string](c, rb)
}

func (_ FfiConverterOptionalString) Read(reader io.Reader) *string {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterStringINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalString) Lower(value *string) C.RustBuffer {
	return LowerIntoRustBuffer[*string](c, value)
}

func (c FfiConverterOptionalString) LowerExternal(value *string) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*string](c, value))
}

func (_ FfiConverterOptionalString) Write(writer io.Writer, value *string) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalString struct{}

func (_ FfiDestroyerOptionalString) Destroy(value *string) {
	if value != nil {
		FfiDestroyerString{}.Destroy(*value)
	}
}

type FfiConverterOptionalMetadata struct{}

var FfiConverterOptionalMetadataINSTANCE = FfiConverterOptionalMetadata{}

func (c FfiConverterOptionalMetadata) Lift(rb RustBufferI) *Metadata {
	return LiftFromRustBuffer[*Metadata](c, rb)
}

func (_ FfiConverterOptionalMetadata) Read(reader io.Reader) *Metadata {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterMetadataINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalMetadata) Lower(value *Metadata) C.RustBuffer {
	return LowerIntoRustBuffer[*Metadata](c, value)
}

func (c FfiConverterOptionalMetadata) LowerExternal(value *Metadata) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*Metadata](c, value))
}

func (_ FfiConverterOptionalMetadata) Write(writer io.Writer, value *Metadata) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterMetadataINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalMetadata struct{}

func (_ FfiDestroyerOptionalMetadata) Destroy(value *Metadata) {
	if value != nil {
		FfiDestroyerMetadata{}.Destroy(*value)
	}
}

type FfiConverterOptionalSetTime struct{}

var FfiConverterOptionalSetTimeINSTANCE = FfiConverterOptionalSetTime{}

func (c FfiConverterOptionalSetTime) Lift(rb RustBufferI) *SetTime {
	return LiftFromRustBuffer[*SetTime](c, rb)
}

func (_ FfiConverterOptionalSetTime) Read(reader io.Reader) *SetTime {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterSetTimeINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalSetTime) Lower(value *SetTime) C.RustBuffer {
	return LowerIntoRustBuffer[*SetTime](c, value)
}

func (c FfiConverterOptionalSetTime) LowerExternal(value *SetTime) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*SetTime](c, value))
}

func (_ FfiConverterOptionalSetTime) Write(writer io.Writer, value *SetTime) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterSetTimeINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalSetTime struct{}

func (_ FfiDestroyerOptionalSetTime) Destroy(value *SetTime) {
	if value != nil {
		FfiDestroyerSetTime{}.Destroy(*value)
	}
}

type FfiConverterSequenceDirEntry struct{}

var FfiConverterSequenceDirEntryINSTANCE = FfiConverterSequenceDirEntry{}

func (c FfiConverterSequenceDirEntry) Lift(rb RustBufferI) []DirEntry {
	return LiftFromRustBuffer[[]DirEntry](c, rb)
}

func (c FfiConverterSequenceDirEntry) Read(reader io.Reader) []DirEntry {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]DirEntry, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterDirEntryINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceDirEntry) Lower(value []DirEntry) C.RustBuffer {
	return LowerIntoRustBuffer[[]DirEntry](c, value)
}

func (c FfiConverterSequenceDirEntry) LowerExternal(value []DirEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]DirEntry](c, value))
}

func (c FfiConverterSequenceDirEntry) Write(writer io.Writer, value []DirEntry) {
	if len(value) > math.MaxInt32 {
		panic("[]DirEntry is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterDirEntryINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceDirEntry struct{}

func (FfiDestroyerSequenceDirEntry) Destroy(sequence []DirEntry) {
	for _, value := range sequence {
		FfiDestroyerDirEntry{}.Destroy(value)
	}
}

const (
	uniffiRustFuturePollReady      int8 = 0
	uniffiRustFuturePollMaybeReady int8 = 1
)

type rustFuturePollFunc func(C.uint64_t, C.UniffiRustFutureContinuationCallback, C.uint64_t)
type rustFutureCompleteFunc[T any] func(C.uint64_t, *C.RustCallStatus) T
type rustFutureFreeFunc func(C.uint64_t)

//export zerofs_ffi_uniffiFutureContinuationCallback
func zerofs_ffi_uniffiFutureContinuationCallback(data C.uint64_t, pollResult C.int8_t) {
	h := cgo.Handle(uintptr(data))
	waiter := h.Value().(chan int8)
	waiter <- int8(pollResult)
}

func uniffiRustCallAsync[E any, T any, F any](
	errConverter BufReader[E],
	completeFunc rustFutureCompleteFunc[F],
	liftFunc func(F) T,
	rustFuture C.uint64_t,
	pollFunc rustFuturePollFunc,
	freeFunc rustFutureFreeFunc,
) (T, E) {
	defer freeFunc(rustFuture)

	pollResult := int8(-1)
	waiter := make(chan int8, 1)

	chanHandle := cgo.NewHandle(waiter)
	defer chanHandle.Delete()

	for pollResult != uniffiRustFuturePollReady {
		pollFunc(
			rustFuture,
			(C.UniffiRustFutureContinuationCallback)(C.zerofs_ffi_uniffiFutureContinuationCallback),
			C.uint64_t(chanHandle),
		)
		pollResult = <-waiter
	}

	var status C.RustCallStatus
	ffiValue := completeFunc(rustFuture, &status)
	err := checkCallStatus(errConverter, status)
	if status.code != 0 {
		var zero T
		return zero, err
	}
	return liftFunc(ffiValue), err
}

//export zerofs_ffi_uniffiFreeGorutine
func zerofs_ffi_uniffiFreeGorutine(data C.uint64_t) {
	handle := cgo.Handle(uintptr(data))
	defer handle.Delete()

	guard := handle.Value().(chan struct{})
	guard <- struct{}{}
}

// Linux errno for an error: the strict 1:1 variant↔errno mapping (the `Io`
// variant returns its own `errno`). A free function rather than an enum method
// so it crosses every binding (some generators forbid methods on enums).
func ErrorToErrno(error *ZeroFsError) int32 {
	return FfiConverterInt32INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.int32_t {
		return C.uniffi_zerofs_ffi_fn_func_error_to_errno(FfiConverterZeroFsErrorINSTANCE.Lower(error), _uniffiStatus)
	}))
}

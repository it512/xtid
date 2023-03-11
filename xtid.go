package xtid

import (
	"bytes"
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (

	// Timestamp is a uint64
	timestampLengthInBytes = 8

	// Payload is 10-bytes
	payloadLengthInBytes = 10

	// type is uint16
	typeLengthInbytes = 2

	// XTIDs are 20 bytes when binary encoded
	byteLength = timestampLengthInBytes + typeLengthInbytes + payloadLengthInBytes

	payloadStart = timestampLengthInBytes + typeLengthInbytes

	// The length of a XTID when string (base62) encoded
	stringEncodedLength = 27

	// A string-encoded minimum value for a XTID
	minStringEncoded = "000000000000000000000000000"

	// A string-encoded maximum value for a XTID
	maxStringEncoded = "aWgEPTl1tmebfsQzFP4bxwgy80V"
)

var (
	source io.Reader = newEntropyPool()
)

// XTIDs are 20 bytes:
//
// 00-07 byte: uint64 timestamp
// 08~11 byte: uint32 type
// 12-19 byte: random "payload"
type XTID [byteLength]byte

var (
	errSize        = fmt.Errorf("Valid XTIDs are %v bytes", byteLength)
	errStrSize     = fmt.Errorf("Valid encoded XTIDs are %v characters", stringEncodedLength)
	errStrValue    = fmt.Errorf("Valid encoded XTIDs are bounded by %s and %s", minStringEncoded, maxStringEncoded)
	errPayloadSize = fmt.Errorf("Valid XTID payloads are %v bytes", payloadLengthInBytes)

	// Represents a completely empty (invalid) XTID
	Nil XTID
	// Represents the highest value a XTID can have
	Max = XTID{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
)

// Append appends the string representation of i to b, returning a slice to a
// potentially larger memory area.
func (i XTID) Append(b []byte) []byte {
	return fastAppendEncodeBase62(b, i[:])
}

// The timestamp portion of the ID as a Time object
func (i XTID) Time() time.Time {
	return correctedUTCTimestampToTime(i.Timestamp())
}

func (i XTID) Type() uint16 {
	return binary.BigEndian.Uint16(i[timestampLengthInBytes:payloadStart])
}

// The timestamp portion of the ID as a bare integer which is uncorrected
// for XTID's special epoch.
func (i XTID) Timestamp() uint64 {
	return binary.BigEndian.Uint64(i[:timestampLengthInBytes])
}

// String-encoded representation that can be passed through Parse()
func (i XTID) String() string {
	return string(i.Append(make([]byte, 0, stringEncodedLength)))
}

// Raw byte representation of XTID
func (i XTID) Bytes() []byte {
	// Safe because this is by-value
	return i[:]
}

// IsNil returns true if this is a "nil" XTID
func (i XTID) IsNil() bool {
	return i == Nil
}

// Get satisfies the flag.Getter interface, making it possible to use XTIDs as
// part of of the command line options of a program.
func (i XTID) Get() any {
	return i
}

// Set satisfies the flag.Value interface, making it possible to use XTIDs as
// part of of the command line options of a program.
func (i *XTID) Set(s string) error {
	return i.UnmarshalText([]byte(s))
}

func (i XTID) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

func (i *XTID) UnmarshalText(b []byte) error {
	id, err := Parse(string(b))
	if err != nil {
		return err
	}
	*i = id
	return nil
}

func (i XTID) MarshalBinary() ([]byte, error) {
	return i.Bytes(), nil
}

func (i *XTID) UnmarshalBinary(b []byte) error {
	id, err := FromBytes(b)
	if err != nil {
		return err
	}
	*i = id
	return nil
}

func (i *XTID) scan(b []byte) error {
	switch len(b) {
	case 0:
		*i = Nil
		return nil
	case byteLength:
		return i.UnmarshalBinary(b)
	case stringEncodedLength:
		return i.UnmarshalText(b)
	default:
		return errSize
	}
}

// MarshalGQL implements the graphql.Marshaler interface
func (i XTID) MarshalGQL(w io.Writer) {
	io.WriteString(w, strconv.Quote(i.String()))
}

// UnmarshalGQL implements the graphql.UnMarshaler interface
func (i *XTID) UnmarshalGQL(v any) error {
	return i.Scan(v)
}

func (i XTID) MarshalJSON() ([]byte, error) {
	return i.MarshalText()
}

func (i *XTID) UnmarshalJSON(v []byte) error {
	return i.scan(v)
}

// Value converts the XTID into a SQL driver value which can be used to
// directly use the XTID as parameter to a SQL query.
func (i XTID) Value() (driver.Value, error) {
	if i.IsNil() {
		return nil, nil
	}
	return i.String(), nil
}

// Scan implements the sql.Scanner interface. It supports converting from
// string, []byte, or nil into a XTID value. Attempting to convert from
// another type will return an error.
func (i *XTID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		return i.scan(nil)
	case []byte:
		return i.scan(v)
	case string:
		return i.scan([]byte(v))
	default:
		return fmt.Errorf("Scan: unable to scan type %T into XTID", v)
	}
}

// Parse decodes a string-encoded representation of a XTID object
func Parse(s string) (XTID, error) {
	if len(s) != stringEncodedLength {
		return Nil, errStrSize
	}

	src := [stringEncodedLength]byte{}
	dst := [byteLength]byte{}

	copy(src[:], s[:])

	if err := fastDecodeBase62(dst[:], src[:]); err != nil {
		return Nil, errStrValue
	}

	return FromBytes(dst[:])
}

// Parse decodes a string-encoded representation of a XTID object.
// Same behavior as Parse, but returns a Nil XTID on error.
func ParseOrNil(s string) XTID {
	id, err := Parse(s)
	if err != nil {
		return Nil
	}
	return id
}

func timeToCorrectedUTCTimestamp(t time.Time) uint64 {
	return uint64(t.UnixMicro())
}

func correctedUTCTimestampToTime(ts uint64) time.Time {
	return time.UnixMicro(int64(ts))
}

func Must(id XTID, err error) XTID {
	if err != nil {
		panic(fmt.Sprintf("Couldn't generate XTID, inconceivable! error: %v", err))
	}
	return id
}

func NewOrNil() (id XTID) {
	id, _ = NewWithType(0)
	return
}

func NewWithType(typ uint16) (id XTID, err error) {
	id, err = Make(time.Now(), typ)
	return
}

// Make a new XTID using custome time and type
func Make(t time.Time, typ uint16) (id XTID, err error) {
	_, err = io.ReadFull(source, id[payloadStart:])

	if err != nil {
		id = Nil // don't leak random bytes on error
		return
	}

	ts := timeToCorrectedUTCTimestamp(t)
	binary.BigEndian.PutUint64(id[:timestampLengthInBytes], ts)
	binary.BigEndian.PutUint16(id[timestampLengthInBytes:payloadStart], typ)

	return
}

// Constructs a XTID from a 20-byte binary representation
func FromBytes(b []byte) (XTID, error) {
	var id XTID

	if len(b) != byteLength {
		return Nil, errSize
	}

	copy(id[:], b)
	return id, nil
}

// Constructs a XTID from a 20-byte binary representation.
// Same behavior as FromBytes, but returns a Nil XTID on error.
func FromBytesOrNil(b []byte) XTID {
	id, err := FromBytes(b)
	if err != nil {
		return Nil
	}
	return id
}

// Sets the global source of random bytes for XTID generation. This
// should probably only be set once globally. While this is technically
// thread-safe as in it won't cause corruption, there's no guarantee
// on ordering.
func SetSource(src io.Reader) {
	if src == nil {
		source = rand.Reader
		return
	}
	source = src
}

// Implements comparison for XTID type
func Compare(a, b XTID) int {
	return bytes.Compare(a[:], b[:])
}

func IDGen(typ uint16) func() XTID {
	return func() (id XTID) {
		id, _ = NewWithType(typ)
		return
	}
}

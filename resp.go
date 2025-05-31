package main

import (
	"bufio"
	"io"
	"log"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func CreateResp(rd io.Reader) *Resp {
	return &Resp{
		reader: bufio.NewReader(rd),
	}
}

type Writer struct {
	writer io.Writer
}

func CreateWriter(wr io.Writer) *Writer {
	return &Writer{
		writer: wr,
	}
}

// readLine reads a line terminated by CRLF from the Resp's reader.
// It returns the line content, the number of bytes read, and any error encountered.
func (resp *Resp) readLine() (line []byte, size int, err error) {
	for {
		readByte, err := resp.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}

		size += 1
		line = append(line, readByte)

		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], size, nil
}

// readInteger parses an integer from the RESP reader, returning its value, the bytes read, and any encountered error.
func (resp *Resp) readInteger() (x int, size int, err error) {
	line, size, err := resp.readLine()
	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, size, err
	}

	return int(i64), size, nil
}

// readArray reads and parses an array from the RESP protocol.
// It uses the RESP reader, determining the array length and retrieving its elements.
// Returns a Value of the type "array" or an error if parsing fails.
func (resp *Resp) readArray() (Value, error) {
	value := Value{}
	value.typ = "array"

	// Read the length of the array
	length, _, err := resp.readInteger()
	if err != nil {
		return value, err
	}

	// Foreach line, parse and read the value
	value.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := resp.Read()
		if err != nil {
			return value, err
		}
		// Add parsed value to the array
		value.array[i] = val
	}

	return value, nil
}

// readBulk reads a bulk string from the RESP reader.
// It parses the length, retrieves the bulk string content, and ensures the trailing CRLF is consumed.
// Returns a Value of the type "bulk" or an error if parsing fails.
func (resp *Resp) readBulk() (Value, error) {
	value := Value{}
	value.typ = "bulk"

	length, _, err := resp.readInteger()
	if err != nil {
		return value, err
	}

	bulk := make([]byte, length)

	_, err = resp.reader.Read(bulk)
	if err != nil {
		return value, err
	}

	value.bulk = string(bulk)

	// Read the trailing CRLF
	_, _, err = resp.readLine()
	if err != nil {
		return value, err
	}

	return value, nil
}

// Read interprets the next RESP element from the reader and parses it into a Value. Returns the parsed Value or an error.
func (resp *Resp) Read() (Value, error) {
	typ, err := resp.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch typ {
	case ARRAY:
		return resp.readArray()
	case BULK:
		return resp.readBulk()
	default:
		log.Printf("Unknown type: %v", string(typ))
		return Value{}, nil
	}
}

// Write serializes the Value instance into a RESP byte-encoded format and writes it to the Writer.
func (wr *Writer) Write(value Value) error {
	var bytes = value.Marshal()
	_, err := wr.writer.Write(bytes)

	if err != nil {
		return err
	}

	return nil
}

// Marshal serializes the Value instance into a RESP byte-encoded format based on its type.
func (value Value) Marshal() []byte {
	switch value.typ {
	case "array":
		return value.marshalArray()
	case "bulk":
		return value.marshalBulk()
	case "string":
		return value.marshalString()
	case "integer":
		return value.marshalInteger()
	case "null":
		return value.marshalNull()
	case "error":
		return value.marshalError()
	default:
		return []byte{}
	}
}

// marshalString converts the `Value` instance with type "string" into a RESP byte-encoded format for transmission.
func (value Value) marshalString() []byte {
	var bytes []byte

	bytes = append(bytes, STRING)
	bytes = append(bytes, value.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshalBulk converts the `Value` instance with type "bulk" into a RESP byte-encoded format for transmission.
func (value Value) marshalBulk() []byte {
	var bytes []byte

	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(value.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, value.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshalInteger converts the `Value` instance with type "integer" into a RESP byte-encoded format for transmission.
func (value Value) marshalInteger() []byte {
	var bytes []byte

	bytes = append(bytes, INTEGER)
	bytes = append(bytes, strconv.Itoa(value.num)...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshalArray serializes a `Value` instance with type "array" into a RESP byte-encoded format for transmission.
func (value Value) marshalArray() []byte {
	length := len(value.array)
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(length)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < length; i++ {
		bytes = append(bytes, value.array[i].Marshal()...)
	}

	return bytes
}

// marshalError converts the `Value` instance with type "error" into a RESP byte-encoded format for transmission.
func (value Value) marshalError() []byte {
	var bytes []byte

	bytes = append(bytes, ERROR)
	bytes = append(bytes, value.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshalNull converts the `Value` instance with type "null" into a RESP byte-encoded format for transmission.
func (value Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

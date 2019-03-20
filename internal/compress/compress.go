package compress

import (
	"compress/flate"
	"fmt"
	"io"
	"io/ioutil"

	"bytes"

	"github.com/golang/snappy"
)

func SnappyDecompress(compressedMsg []byte) ([]byte, error) {
	body, err := snappy.Decode(nil, compressedMsg)
	if err != nil {
		return nil, fmt.Errorf("error in decode snappy compressed message: %v", err)
	}

	return body, nil
}

func DeflateDecompress(compressedMsg []byte) ([]byte, error) {
	br := bytes.NewReader(compressedMsg)
	fr := flate.NewReader(br)
	defer fr.Close()
	body, err := ioutil.ReadAll(fr)
	if err != nil && !(err == io.ErrUnexpectedEOF && br.Len() == 0) {
		return nil, fmt.Errorf("error in decode deflate compressed message: %v", err)
	}

	return body, nil
}

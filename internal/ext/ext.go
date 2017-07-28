package ext

import (
	"regexp"
	"fmt"
)

const (
	E_EXT_NOT_SUPPORT = "E_EXT_NOT_SUPPORT"
	E_BAD_TAG	= "E_BAD_TAG"
	E_INVALID_JSON_HEADER = "E_INVALID_JSON_HEADER"

	CLEINT_DISPATCH_TAG_KEY = "##client_dispatch_tag"
	TRACE_ID_KEY = "##trace_id"
)

var MAX_TAG_LEN = 100
var validTagFmt = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type ExtVer uint8

//ext versions
// version for message has no ext
var NO_EXT_VER = ExtVer(uint8(0))
// version for message has tag ext
var TAG_EXT_VER = ExtVer(uint8(2))
// version for message has json header ext
var JSON_HEADER_EXT_VER = ExtVer(uint8(4))


var noExt = NoExt{}
type NoExt []byte

func NewNoExt() NoExt {
	return noExt
}

func (n NoExt) ExtVersion() ExtVer {
	return NO_EXT_VER
}

func (n NoExt) GetBytes() []byte {
	return nil
}

type TagExt []byte

func NewTagExt(tagName []byte) (TagExt, error) {
	if err:= ValidateTag(string(tagName)); err != nil {
		return nil, err
	}
	return TagExt(tagName), nil
}

func (tag TagExt) GetTagName() string {
	return string(tag)
}

func ValidateTag(beValidated string) error {
	lenTag := len(beValidated)
	if lenTag > MAX_TAG_LEN {
		return fmt.Errorf("invalid tag len %v, exceed limit %v", lenTag, MAX_TAG_LEN)
	}
	if !validTagFmt.Match([]byte(beValidated)) {
		return fmt.Errorf("invalid tag %v", beValidated)
	}
	return nil
}

type JsonHeaderExt struct {
	bytes []byte
}

func NewJsonHeaderExt() *JsonHeaderExt {
	return &JsonHeaderExt{}
}

func (self *JsonHeaderExt) SetJsonHeaderBytes(jsonExtBytes []byte) {
	self.bytes = jsonExtBytes[0:]
}

func (self *JsonHeaderExt) ExtVersion() ExtVer {
	return JSON_HEADER_EXT_VER
}

func (self *JsonHeaderExt) GetBytes() []byte {
	return self.bytes
}

type IExtContent interface {
	ExtVersion() ExtVer
	GetBytes()   []byte
}
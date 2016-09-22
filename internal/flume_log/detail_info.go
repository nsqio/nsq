package flume_log

import ()

type DetailInfo struct {
	module string
	detail map[string]interface{}
}

type ExtraInfo map[string]interface{}

func NewDetailInfo(m string) *DetailInfo {
	detailInfo := &DetailInfo{
		module: m,
		detail: make(map[string]interface{}),
	}
	return detailInfo
}

func (d *DetailInfo) SetExtraInfo(extra interface{}) {
	d.detail["extra"] = extra
}

func (d *DetailInfo) AddKeyValue(key string, value interface{}) {
	d.detail[key] = value
}

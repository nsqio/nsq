// This is an NSQ client that reads the specified topic/channel
// and performs HTTP requests (GET/POST) to the specified endpoints

package main

import (
	"reflect"
	"testing"
)

func TestParseCustomHeaders(t *testing.T) {
	type args struct {
		strs []string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			"Valid Custom Headers",
			args{[]string{"header1: value1", "header2:value2", "header3:value3", "header4:value4"}},
			map[string]string{"header1": "value1", "header2": "value2", "header3": "value3", "header4": "value4"},
			false,
		},
		{
			"Invalid Custom Headers where key is present but no value",
			args{[]string{"header1:", "header2:value2", "header3: value3", "header4:value4"}},
			nil,
			true,
		},
		{
			"Invalid Custom Headers where key is not present but value is present",
			args{[]string{"header1: value1", ": value2", "header3:value3", "header4:value4"}},
			nil,
			true,
		},
		{
			"Invalid Custom Headers where key and value are not present but ':' is specified",
			args{[]string{"header1:value1", "header2:value2", ":", "header4:value4"}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCustomHeaders(tt.args.strs)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCustomHeaders() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseCustomHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

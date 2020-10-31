package app

import (
	"testing"
)

func TestFloatArray(t *testing.T) {
	arr := FloatArray{}
	param := "1.0,1.1,1.2,1.3"
	if err := arr.Set(param); err != nil {
		t.Fatal("could not set param ", err)
	}

	wantParam := "1.300000,1.200000,1.100000,1.000000"
	if wantParam != arr.String() {
		t.Fatalf("param string is not correct, got:'%v', want:'%v'", arr.String(), wantParam)
	}
}

func TestFloatArrayEmpty(t *testing.T) {
	arr := FloatArray{}
	wantParam := ""
	if wantParam != arr.String() {
		t.Fatalf("param string is not correct, got:'%v', want:'%v'", arr.String(), wantParam)
	}
}

func BenchmarkFloatArray(b *testing.B) {
	for i := 0; i < b.N; i++ {
		arr := FloatArray{}
		param := "1.0,1.1,1.2,1.3"
		if err := arr.Set(param); err != nil {
			b.Fatal("could not set param ", err)
		}

		wantParam := "1.300000,1.200000,1.100000,1.000000"
		if wantParam != arr.String() {
			b.Fatalf("param string is not correct, got:'%v', want:'%v'", arr.String(), wantParam)
		}
	}
}

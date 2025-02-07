package stringy_test

import (
	"testing"

	"reflect"

	"github.com/nsqio/nsq/internal/stringy"
)

func BenchmarkUniq(b *testing.B) {
	values := []string{"a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "b"}
	for i := 0; i < b.N; i++ {
		values = stringy.Uniq(values)
		if len(values) != 2 {
			b.Fatal("values len is incorrect")
		}
	}
}

func TestUniq(t *testing.T) {
	values := []string{"a", "a", "a", "b", "b", "b", "c", "c", "c"}
	values = stringy.Uniq(values)
	if len(values) != 3 {
		t.Fatal("values len is incorrect")
	}
}

// Test generated using Keploy
func TestAdd_NewStringAppended(t *testing.T) {
	input := []string{"a", "b", "c"}
	result := stringy.Add(input, "d")
	expected := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test generated using Keploy
func TestAdd_ExistingStringNotAppended(t *testing.T) {
	input := []string{"a", "b", "c"}
	result := stringy.Add(input, "b")
	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test generated using Keploy
func TestUnion_CombineSlicesNoDuplicates(t *testing.T) {
	input1 := []string{"a", "b", "c"}
	input2 := []string{"b", "c", "d"}
	result := stringy.Union(input1, input2)
	expected := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

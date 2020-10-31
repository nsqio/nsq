package app

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

type FloatArray []float64

func (a *FloatArray) Get() interface{} { return []float64(*a) }

func (a *FloatArray) Set(param string) error {
	for _, s := range strings.Split(param, ",") {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			log.Fatalf("Could not parse: %s", s)
		}
		*a = append(*a, v)
	}
	sort.Sort(*a)
	return nil
}

func (a FloatArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FloatArray) Less(i, j int) bool { return a[i] > a[j] }
func (a FloatArray) Len() int           { return len(a) }

func (a *FloatArray) String() string {
	if len(*a) == 0 {
		return ""
	}
	s := make([]string, len(*a))
	for i, v := range *a {
		s[i] = fmt.Sprintf("%f", v)
	}
	return strings.Join(s, ",")
}

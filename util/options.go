package util

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func ResolveOptions(options interface{}, flagSet *flag.FlagSet, cfg map[string]interface{}) {
	val := reflect.ValueOf(options).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		if flagName == "" {
			continue
		}
		flag := flagSet.Lookup(flagName)
		if flag == nil {
			log.Panicf("ERROR: flag %s does not exist", flagName)
		}
		cfgName := field.Tag.Get("cfg")
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}
		var v interface{}
		if hasArg(flagName) {
			v = flag.Value.String()
		} else {
			cfgVal, ok := cfg[cfgName]
			if !ok {
				continue
			}
			v = cfgVal
		}
		fieldVal := val.FieldByName(field.Name)
		coerced, err := Coerce(v, fieldVal.Interface(), field.Tag.Get("arg"))
		if err != nil {
			log.Fatalf("ERROR: option resolution failed to coerce %v for %s (%+v) - %s",
				v, field.Name, fieldVal, err)
		}
		fieldVal.Set(reflect.ValueOf(coerced))
	}
}

func coerceBool(v interface{}) (bool, error) {
	switch v.(type) {
	case bool:
		return v.(bool), nil
	case string:
		return strconv.ParseBool(v.(string))
	case int, int16, uint16, int32, uint32, int64, uint64:
		return reflect.ValueOf(v).Int() == 0, nil
	}
	return false, errors.New("invalid value type")
}

func coerceInt64(v interface{}) (int64, error) {
	switch v.(type) {
	case string:
		return strconv.ParseInt(v.(string), 10, 64)
	case int, int16, uint16, int32, uint32, int64, uint64:
		return reflect.ValueOf(v).Int(), nil
	}
	return 0, errors.New("invalid value type")
}

func coerceDuration(v interface{}, arg string) (time.Duration, error) {
	switch v.(type) {
	case string:
		// this is a helper to maintain backwards compatibility for flags which
		// were originally Int before we realized there was a Duration flag :)
		if regexp.MustCompile(`^[0-9]+$`).MatchString(v.(string)) {
			intVal, err := strconv.Atoi(v.(string))
			if err != nil {
				return 0, err
			}
			mult, err := time.ParseDuration(arg)
			if err != nil {
				return 0, err
			}
			return time.Duration(intVal) * mult, nil
		}
		return time.ParseDuration(v.(string))
	case int, int16, uint16, int32, uint32, int64, uint64:
		// treat like ms
		return time.Duration(reflect.ValueOf(v).Int()) * time.Millisecond, nil
	case time.Duration:
		return v.(time.Duration), nil
	}
	return 0, errors.New("invalid value type")
}

func coerceStringSlice(v interface{}) ([]string, error) {
	var tmp []string
	switch v.(type) {
	case string:
		for _, s := range strings.Split(v.(string), ",") {
			tmp = append(tmp, s)
		}
	case []interface{}:
		for _, si := range v.([]interface{}) {
			tmp = append(tmp, si.(string))
		}
	case StringArray:
		for _, s := range v.(StringArray) {
			tmp = append(tmp, s)
		}
	}
	return tmp, nil
}

func coerceFloat64Slice(v interface{}) ([]float64, error) {
	var tmp []float64
	switch v.(type) {
	case string:
		for _, s := range strings.Split(v.(string), ",") {
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return nil, err
			}
			tmp = append(tmp, f)
		}
	case []interface{}:
		for _, fi := range v.([]interface{}) {
			tmp = append(tmp, fi.(float64))
		}
	case FloatArray:
		for _, f := range v.(FloatArray) {
			tmp = append(tmp, f)
		}
	}
	return tmp, nil
}

func coerceString(v interface{}) (string, error) {
	switch v.(type) {
	case string:
		return v.(string), nil
	}
	return fmt.Sprintf("%s", v), nil
}

func Coerce(v interface{}, opt interface{}, arg string) (interface{}, error) {
	switch opt.(type) {
	case bool:
		return coerceBool(v)
	case int:
		i, err := coerceInt64(v)
		if err != nil {
			return nil, err
		}
		return int(i), nil
	case int32:
		i, err := coerceInt64(v)
		if err != nil {
			return nil, err
		}
		return int32(i), nil
	case int64:
		return coerceInt64(v)
	case string:
		return coerceString(v)
	case time.Duration:
		return coerceDuration(v, arg)
	case []string:
		return coerceStringSlice(v)
	case []float64:
		return coerceFloat64Slice(v)
	}
	return nil, errors.New("invalid type")
}

func hasArg(s string) bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, s) {
			return true
		}
	}
	return false
}

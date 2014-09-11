package options

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

func Resolve(options interface{}, flagSet *flag.FlagSet, cfg map[string]interface{}) {
	val := reflect.ValueOf(options).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		// pull out the struct tags:
		//    flag - the name of the command line flag
		//    deprecated - (optional) the name of the deprecated command line flag
		//    cfg - (optional, defaults to underscored flag) the name of the config file option
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		deprecatedFlagName := field.Tag.Get("deprecated")
		cfgName := field.Tag.Get("cfg")
		if flagName == "" {
			// resolvable fields must have at least the `flag` struct tag
			continue
		}
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}

		// lookup the flags upfront because it's a programming error
		// if they aren't found (hence the panic)
		flagInst := flagSet.Lookup(flagName)
		if flagInst == nil {
			log.Panicf("ERROR: flag %s does not exist", flagName)
		}
		var deprecatedFlag *flag.Flag
		if deprecatedFlagName != "" {
			deprecatedFlag = flagSet.Lookup(deprecatedFlagName)
			if deprecatedFlag == nil {
				log.Panicf("ERROR: deprecated flag %s does not exist", deprecatedFlagName)
			}
		}

		// resolve the flags with the following priority (highest to lowest):
		//
		// 1. command line flag
		// 2. deprecated command line flag
		// 3. config file option
		var v interface{}
		if hasArg(flagName) {
			v = flagInst.Value.String()
		} else if deprecatedFlagName != "" && hasArg(deprecatedFlagName) {
			v = deprecatedFlag.Value.String()
			log.Printf("WARNING: use of the --%s command line flag is deprecated (use --%s)",
				deprecatedFlagName, flagName)
		} else {
			cfgVal, ok := cfg[cfgName]
			if !ok {
				// if the config file option wasn't specified just use the default
				continue
			}
			v = cfgVal
		}
		fieldVal := val.FieldByName(field.Name)
		coerced, err := coerce(v, fieldVal.Interface(), field.Tag.Get("arg"))
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
	case []string:
		tmp = v.([]string)
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
	case []string:
		for _, s := range v.([]string) {
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return nil, err
			}
			tmp = append(tmp, f)
		}
	case []float64:
		log.Printf("%+v", v)
		tmp = v.([]float64)
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

func coerce(v interface{}, opt interface{}, arg string) (interface{}, error) {
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

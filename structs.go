package bingo

import (
	"reflect"
	"slices"
	"strings"
)

type CustomMarshaller struct{}

func (c CustomMarshaller) Marshal(v interface{}) ([]byte, error) {
	return MarshalIgnoreTags(v)
}

func getVariantStructValue(v reflect.Value, t reflect.Type) reflect.Value {
	sf := make([]reflect.StructField, 0)
	for i := 0; i < t.NumField(); i++ {
		sf = append(sf, t.Field(i))

		if t.Field(i).Tag.Get("json") != "" {
			sf[i].Tag = ``
		}
	}
	newType := reflect.StructOf(sf)
	return v.Convert(newType)
}

func MarshalIgnoreTags(obj interface{}) ([]byte, error) {
	value := reflect.ValueOf(obj)
	t := value.Type()
	newValue := getVariantStructValue(value, t)
	return json.Marshal(newValue.Interface())
}

func GetIndexesFromStruct(v interface{}) map[string]any {
	indexes := map[string]any{}
	t := reflect.TypeOf(v)
	val := reflect.ValueOf(v)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag := field.Tag.Get("bingo"); tag == "" {
			continue
		}
		properties := strings.Split(field.Tag.Get("bingo"), ",")
		if slices.Contains(properties, "index") {
			if val.Field(i).IsValid() {
				indexes[field.Name] = val.Field(i).Interface()
			}
		}
	}
	return indexes
}

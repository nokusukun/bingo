package main

import (
	"encoding/json"
	"fmt"
	"github.com/nokusukun/bingo"
	"reflect"
	"slices"
	"strings"
)

type User struct {
	bingo.Document
	Name     string `json:"name" bingo:"index"`
	Password string `json:"password"`
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

func main() {
	//z := User{
	//	Document: bingo.Document{
	//		ID: "1",
	//	},
	//	Name:     "Nokusukun",
	//	Password: "dskwqweq",
	//}
	////v, _ := MarshalIgnoreTags(z)
	//
	x := &User{}
	json.Unmarshal([]byte("{\"_id\":\"1\",\"Name\":\"Nokusukun\",\"Password\":\"dskwqweq\"}"), x)
	fmt.Printf("%+v", x)
	//fmt.Println(GetIndexesFromStruct(z))
}

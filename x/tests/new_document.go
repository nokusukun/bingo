package main

import (
	"fmt"
	"github.com/nokusukun/bingo"
	"os"
	"strings"
)

type Platform string

const (
	Excel  = "excel"
	Sheets = "sheets"
)

type Function struct {
	bingo.Document
	Name        string   `json:"name,omitempty"`
	Category    string   `json:"category,omitempty"`
	Args        []string `json:"args,omitempty"`
	Example     string   `json:"example,omitempty"`
	Description string   `json:"description,omitempty"`
	URL         string   `json:"URL,omitempty"`
	Platform    Platform `json:"platform,omitempty"`
}

func main() {
	driver, err := bingo.NewDriver(bingo.DriverConfiguration{
		Filename: "clippy.db",
	})
	if err != nil {
		panic(err)
	}

	defer func() {
		os.Remove("clippy.db")
	}()

	functionDB := bingo.CollectionFrom[Function](driver, "functions")

	// Registering a custom ID generator,
	// this will be called when a new document is inserted
	// and the if the document does not have a key/id set.
	functionDB.OnNewId = func(_ int, doc *Function) []byte {
		return []byte(strings.ToLower(doc.Name))
	}

	// Inserting
	key, err := functionDB.Insert(Function{
		Name:        "SUM",
		Category:    "Math",
		Args:        []string{"a", "b"},
		Example:     "SUM(1, 2)",
		Description: "Adds two numbers together",
		URL:         "https://support.google.com/docs/answer/3093669?hl=en",
		Platform:    Sheets,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("Inserted document with key", string(key))

	searchQuery := "sum"
	platform := "sheets"
	// Querying
	query := functionDB.Query(bingo.Query[Function]{
		Filter: func(doc Function) bool {
			return doc.Platform == Platform(platform) && strings.Contains(strings.ToLower(doc.Name), strings.ToLower(searchQuery))
		},
		Count: 3,
	})
	if query.Error != nil {
		panic(query.Error)
	}

	if !query.Any() {
		panic("No documents found!")
	}

	fmt.Println("Found", query.Count(), "documents")
	for _, function := range query.Items {
		fmt.Printf("%s: %s\n", function.Name, function.Description)
	}

	sum, err := functionDB.FindByKey("sum")
	if err != nil {
		panic(err)
	}
	sum.Category = "Algebra"
	err = functionDB.UpdateOne(sum)
	if err != nil {
		panic(err)
	}

	newSum, _ := functionDB.FindByBytesKey(sum.Key())
	fmt.Println("Updated SUM category to", newSum.Category)
	fmt.Println(newSum)
}

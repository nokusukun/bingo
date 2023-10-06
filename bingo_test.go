package bingo_test

import (
	"github.com/nokusukun/bingodb"
	"os"
	"testing"
)

type TestDocument struct {
	ID   string `validate:"required"`
	Name string `validate:"required"`
}

func (td TestDocument) Key() []byte {
	return []byte(td.ID)
}

// Initialize the driver
func TestNewDriver(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "test.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("test.db")
	}()
}

// Basic CRUD operations
func TestCRUD(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testcrud.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testcrud.db")
	}()

	// Collection
	coll := bingo.CollectionFrom[TestDocument](driver, "testCollection")

	// Insert
	doc := TestDocument{ID: "1", Name: "Test"}
	result := coll.Insert(doc)
	if !result.Success {
		t.Fatalf("Failed to insert document: %v", result.Error())
	}

	// Find
	foundDoc, err := coll.FindById("1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if foundDoc.Name != "Test" {
		t.Fatalf("Unexpected document data: %v", foundDoc)
	}
}

func TestFindAll(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testquery.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testquery.db")
	}()

	// Collection
	coll := bingo.CollectionFrom[TestDocument](driver, "testCollection")

	docs := []TestDocument{
		{ID: "1", Name: "Apple"},
		{ID: "2", Name: "Banana"},
		{ID: "3", Name: "Cherry"},
	}
	for _, doc := range docs {
		result := coll.Insert(doc)
		if !result.Success {
			t.Fatalf("Failed to insert document: %v", result.Error())
		}
	}

	result, _ := coll.Find(func(doc TestDocument) bool {
		return true
	}, bingo.Skip(1), bingo.Count(1))

	if result[0].Name != "Banana" {
		t.Fatalf("Unexpected query result: %v", result[0])
	}
}

func TestUpdateOne(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testquery.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testquery.db")
	}()

	// Collection
	coll := bingo.CollectionFrom[TestDocument](driver, "testCollection")

	docs := []TestDocument{
		{ID: "1", Name: "Apple"},
		{ID: "2", Name: "Banana"},
		{ID: "3", Name: "Cherry"},
	}
	for _, doc := range docs {
		result := coll.Insert(doc)
		if !result.Success {
			t.Fatalf("Failed to insert document: %v", result.Error())
		}
	}

	result, err := coll.FindOne(func(doc TestDocument) bool {
		return doc.Name == "Apple"
	})

	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	result.Name = "Pineapple"
	err = coll.UpdateOne(result)
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	result, err = coll.FindOne(func(doc TestDocument) bool {
		return doc.Name == "Pineapple"
	})
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if result.Name != "Pineapple" {
		t.Fatalf("Unexpected query result: %v", result)
	}
}

func TestDeleteOne(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testquery.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testquery.db")
	}()

	// Collection
	coll := bingo.CollectionFrom[TestDocument](driver, "testCollection")

	docs := []TestDocument{
		{ID: "1", Name: "Apple"},
		{ID: "2", Name: "Banana"},
		{ID: "3", Name: "Cherry"},
	}
	for _, doc := range docs {
		result := coll.Insert(doc)
		if !result.Success {
			t.Fatalf("Failed to insert document: %v", result.Error())
		}
	}

	result, err := coll.FindOne(func(doc TestDocument) bool {
		return doc.Name == "Apple"
	})

	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	err = coll.DeleteOne(result)
	if err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	_, err = coll.FindOne(func(doc TestDocument) bool {
		return doc.Name == "Apple"
	})
	if err == nil {
		t.Fatalf("Found document that should have been deleted")
	}
}

func TestQueryFunctionality(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testquery.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testquery.db")
	}()

	// Collection
	coll := bingo.CollectionFrom[TestDocument](driver, "testQueryCollection")

	// Insert multiple docs
	docs := []TestDocument{
		{ID: "1", Name: "Apple"},
		{ID: "2", Name: "Banana"},
		{ID: "3", Name: "Cherry"},
	}
	for _, doc := range docs {
		result := coll.Insert(doc)
		if !result.Success {
			t.Fatalf("Failed to insert document: %v", result.Error())
		}
	}

	// Query by filter
	query := bingo.Query[TestDocument]{
		Filter: func(doc TestDocument) bool {
			return doc.Name == "Banana"
		},
	}
	qResult := coll.Query(query)
	if !qResult.Any() {
		t.Fatalf("Query didn't return any results")
	}
	if qResult.First().Name != "Banana" {
		t.Fatalf("Unexpected query result: %v", qResult.First())
	}

	// Query by keys
	keyQuery := bingo.Query[TestDocument]{Keys: []string{"1", "3"}}
	kResult := coll.Query(keyQuery)
	if kResult.Count() != 2 {
		t.Fatalf("Unexpected count for key-based query: %d", kResult.Count())
	}
}

func TestErrorScenarios(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testerrors.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testerrors.db")
	}()

	coll := bingo.CollectionFrom[TestDocument](driver, "testErrorCollection")

	// Insert doc with missing ID (should fail validation)
	doc := TestDocument{Name: "Invalid"}
	result := coll.Insert(doc)
	if result.Success || result.Error() == nil {
		t.Fatal("Expected insertion failure due to validation error")
	}

	// Find non-existent document
	_, err = coll.FindById("nonexistent")
	if err == nil || !bingo.IsErrDocumentNotFound(err) {
		t.Fatalf("Expected a document not found error, got: %v", err)
	}
}

func TestMiddlewareFunctionality(t *testing.T) {
	config := bingo.DriverConfiguration{
		Filename:       "testmiddleware.db",
		DeleteNoVerify: true,
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}

	defer func() {
		driver.Close()
		os.Remove("testmiddleware.db")
	}()

	coll := bingo.CollectionFrom[TestDocument](driver, "testMiddlewareCollection")

	// Before insert middleware
	coll.BeforeInsert(func(doc *TestDocument) error {
		doc.Name = "Modified"
		return nil
	})

	// Insert
	doc := TestDocument{ID: "1", Name: "Original"}
	coll.Insert(doc)

	// Find and check if middleware modified the name
	foundDoc, err := coll.FindById("1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if foundDoc.Name != "Modified" {
		t.Fatalf("Middleware did not modify the document name. Expected 'Modified', got: %v", foundDoc.Name)
	}
}

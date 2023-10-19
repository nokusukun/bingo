package bingo_test

import (
	"fmt"
	"github.com/nokusukun/bingo"
	"go.etcd.io/bbolt"
	"os"
	"strings"
	"testing"
)

type TestDocument struct {
	bingo.Document
	Name string `json:"name" validate:"required"`
}

//func (td TestDocument) Key() []byte {
//	return []byte(td.ID)
//}

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
	t.Run("should insert", func(t *testing.T) {
		// Insert
		_, err = coll.Insert(TestDocument{Document: bingo.Document{ID: "1"}, Name: "Test"})
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}

		_, err = coll.Insert(TestDocument{Name: "Fest"})
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}

		generatedId, err := coll.Insert(TestDocument{Name: "Rest"})
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
		fmt.Println("Generated Id", string(generatedId))
		coll.Driver.View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
				fmt.Println("Bucket", string(name))
				return b.ForEach(func(k, v []byte) error {
					fmt.Println("Key", string(k), "Value", string(v))
					return nil
				})
			})
		})
	})

	// should not insert the same document twice
	t.Run("should not insert the same document twice", func(t *testing.T) {
		doc := TestDocument{Document: bingo.Document{ID: "1"}, Name: "Test"}
		_, err = coll.Insert(doc)
		if err == nil {
			t.Fatalf("Expected insertion failure due to duplicate key")
		}
	})

	t.Run("should return correct key when using FindOneWithKey", func(t *testing.T) {
		foundDoc, id, err := coll.FindOneWithKey(func(doc TestDocument) bool {
			return doc.ID == "1"
		})
		if err != nil {
			t.Fatalf("Failed to find document: %v", err)
		}
		if foundDoc.Name != "Test" {
			t.Fatalf("Unexpected document data: %v", foundDoc)
		}
		if id == nil {
			t.Fatalf("Expected ID to be returned")
		}
		if string(id) != "1" {
			t.Fatalf("Unexpected ID returned: %v", id)
		}
	})

	// should not insert but not throw an error because IgnoreErrors is passed
	t.Run("should not insert but not throw an error because IgnoreErrors is passed", func(t *testing.T) {
		doc := TestDocument{Document: bingo.Document{ID: "1"}, Name: "Test"}
		id, err := coll.Insert(doc, bingo.IgnoreErrors)
		if err != nil && id != nil {
			t.Fatalf("Expected insertion to succeed due to IgnoreErrors")
		}
	})

	t.Run("should upsert data", func(t *testing.T) {
		doc := TestDocument{Document: bingo.Document{ID: "1"}, Name: "Fooby"}
		id, err := coll.Insert(doc, bingo.Upsert)
		if err != nil {
			t.Fatalf("Expected insertion to succeed due to Upsert")
		}
		if id == nil {
			t.Fatalf("Expected ID to be returned due to Upsert")
		}

		foundDoc, err := coll.FindByBytesKey(id)
		if err != nil {
			t.Fatalf("Failed to find document: %v", err)
		}
		if foundDoc.Name != "Fooby" {
			t.Fatalf("Unexpected document data: %v", foundDoc)
		}
	})

	// Find
	t.Run("should find by ID", func(t *testing.T) {
		foundDoc, err := coll.FindByKey("1")
		if err != nil {
			t.Fatalf("Failed to find document: %v", err)
		}
		if !strings.Contains("TestFooby", foundDoc.Name) {
			t.Fatalf("Unexpected document data: %v", foundDoc)
		}
	})

	t.Run("Test metadata for fields", func(t *testing.T) {
		fields, err := driver.FieldsOf("testCollection")
		if err != nil {
			t.Fatalf("Failed to read metadata: %v", err)
		}
		if fields == nil {
			t.Fatalf("Expected fields to be returned")
		}
		fmt.Println("fields", fields)
	})

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
		{Name: "Apple"},
		{Name: "Banana"},
		{Name: "Cherry"},
	}
	for _, doc := range docs {
		_, err = coll.Insert(doc)
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
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
		{Name: "Apple"},
		{Name: "Banana"},
		{Name: "Cherry"},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("Unexpected number of inserted documents: %d", len(ids))
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
		{Name: "Apple"},
		{Name: "Banana"},
		{Name: "Cherry"},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("Unexpected number of inserted documents: %d", len(ids))
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

func TestDeleteIter(t *testing.T) {
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
		{Name: "Apple"},
		{Name: "Banana"},
		{Name: "Cherry"},
		{Name: "Pineapple"},
		{Name: "Strawberry"},
		{Name: "Watermelon"},
		{Name: "Orange"},
		{Name: "Grape"},
		{Name: "Kiwi"},
		{Name: "Mango"},
		{Name: "Peach"},
		{Name: "Pear"},
		{Name: "Plum"},
		{Name: "Pomegranate"},
		{Name: "Raspberry"},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	if len(ids) != len(docs) {
		t.Fatalf("Unexpected number of inserted documents: %d", len(ids))
	}

	err = coll.DeleteIter(func(doc *TestDocument) bool {
		return doc.Name[0] == 'P'
	})
	if err != nil {
		t.Fatalf("Failed to delete documents: %v", err)
	}

	result, err := coll.Find(func(doc TestDocument) bool {
		return doc.Name[0] == 'P'
	})
	if err != nil && !bingo.IsErrDocumentNotFound(err) {
		t.Fatalf("Failed to find documents: %v", err)
	}

	if len(result) > 0 {
		t.Fatalf("Found documents that should have been deleted")
	}
}

func TestUpdateIter(t *testing.T) {
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
		{Name: "Apple"},
		{Name: "Banana"},
		{Name: "Cherry"},
		{Name: "Pineapple"},
		{Name: "Strawberry"},
		{Name: "Watermelon"},
		{Name: "Orange"},
		{Name: "Grape"},
		{Name: "Kiwi"},
		{Name: "Mango"},
		{Name: "Peach"},
		{Name: "Pear"},
		{Name: "Plum"},
		{Name: "Pomegranate"},
		{Name: "Raspberry"},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	if len(ids) != len(docs) {
		t.Fatalf("Unexpected number of inserted documents: %d", len(ids))
	}

	err = coll.UpdateIter(func(doc *TestDocument) *TestDocument {
		if strings.Contains(doc.Name, "P") {
			doc.Name = "Modified"
			return doc
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update documents: %v", err)
	}

	result, err := coll.Find(func(doc TestDocument) bool {
		return doc.Name == "Modified"
	})
	if err != nil {
		t.Fatalf("Failed to find documents: %v", err)
	}

	if len(result) == 0 {
		t.Fatalf("Failed to find documents that should have been updated")
	}

	if len(result) != 5 {
		t.Fatalf("Unexpected number of documents found: %d", len(result))
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
		{Name: "Apple"},
		{Name: "Banana"},
		{Name: "Cherry"},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	if len(ids) != len(docs) {
		t.Fatalf("Unexpected number of inserted documents: %d", len(ids))
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
	keyQuery := bingo.Query[TestDocument]{Keys: [][]byte{[]byte("1"), []byte("3")}}
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

	// Find non-existent document
	_, err = coll.FindByKey("nonexistent")
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
	doc := TestDocument{Document: bingo.Document{ID: "1"}, Name: "Original"}
	coll.Insert(doc)

	// Find and check if middleware modified the name
	foundDoc, err := coll.FindByKey("1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if foundDoc.Name != "Modified" {
		t.Fatalf("Middleware did not modify the document name. Expected 'Modified', got: %v", foundDoc.Name)
	}
}

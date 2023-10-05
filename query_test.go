package bingo

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

type MockDocument struct {
	ID   string
	Name string
}

func (md MockDocument) Key() []byte {
	return []byte(md.ID)
}

func newMockDocument(id, name string) MockDocument {
	return MockDocument{
		ID:   id,
		Name: name,
	}
}

func TestQueryExecution(t *testing.T) {
	// Test setup
	collection := &Collection[MockDocument]{}

	t.Run("Query panic on both Key and Filter set", func(t *testing.T) {
		assert.Panics(t, func() {
			collection.Query(Query[MockDocument]{Keys: []string{"1"}, Filter: func(doc MockDocument) bool { return true }})
		})
	})

	t.Run("Query returns error on no Key or Filter", func(t *testing.T) {
		result := collection.Query(Query[MockDocument]{})
		assert.Error(t, result.Error)
	})

	// Add other tests based on the different logic paths in the `Query` method.
}

func TestQueryResultFunctions(t *testing.T) {
	mockDocuments := []*MockDocument{
		&MockDocument{ID: "1", Name: "A"},
		&MockDocument{ID: "2", Name: "B"},
	}

	qr := &QueryResult[MockDocument]{Items: mockDocuments}

	t.Run("Count returns correct count", func(t *testing.T) {
		assert.Equal(t, 2, qr.Count())
	})

	t.Run("First returns first item", func(t *testing.T) {
		assert.Equal(t, mockDocuments[0], qr.First())
	})

	t.Run("Any returns true for non-empty result", func(t *testing.T) {
		assert.True(t, qr.Any())
	})

	t.Run("ForEach stops on error", func(t *testing.T) {
		qr := &QueryResult[MockDocument]{Items: mockDocuments}
		err := errors.New("sample error")
		qr.ForEach(func(doc *MockDocument) error {
			return err
		})
		assert.Equal(t, err, qr.Error)
	})

	t.Run("Filter returns filtered items", func(t *testing.T) {
		qr := &QueryResult[MockDocument]{Items: mockDocuments}
		newQr := qr.Filter(func(doc *MockDocument) bool {
			return doc.Name == "B"
		})
		assert.Equal(t, 1, newQr.Count())
		assert.Equal(t, "B", newQr.First().Name)
	})

	// Similarly add tests for `Validate`, `Delete`, and `Update` functions.
}

// Implement additional tests for other functions like Delete(), Update() etc.

func TestQueryResultJSONResponse(t *testing.T) {
	t.Run("Empty QueryResult returns empty JSON response", func(t *testing.T) {
		qr := &QueryResult[MockDocument]{}
		resp := qr.JSONResponse()
		assert.Equal(t, 0, resp["count"])
		assert.Empty(t, resp["result"])
	})

	// Add other tests based on different states of QueryResult.
}

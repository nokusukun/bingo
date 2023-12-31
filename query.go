package bingo

import (
	"fmt"
	"go.etcd.io/bbolt"
)

// Query represents a query for filtering and retrieving documents in the collection. It provides flexible options for selecting documents based on various criteria.
type Query[T DocumentSpec] struct {
	// Filter is a function that defines a filtering criteria. It should return true if a document matches the criteria and should be included in the result.
	Filter func(doc T) bool

	// Skip defines the number of documents to skip before the query starts returning results. Useful for implementing pagination.
	Skip int

	// Count specifies the maximum number of documents to be returned by the query. If set to a non-positive value, all matching documents are returned.
	Count int

	// Keys is a slice of document keys that can be used to directly retrieve specific documents from the collection. When provided, it takes precedence over the Filter function.
	Keys [][]byte
	// KeysStr is a slice of document keys that can be used to directly retrieve specific documents from the collection. When provided, it takes precedence over the Filter function.
	KeysStr []string
}

// QueryResult represents the result of a query operation in a collection. It contains the retrieved items, as well as metadata about the query.
type QueryResult[T DocumentSpec] struct {
	// Collection is a reference to the collection to which this query result belongs.
	Collection *Collection[T]

	// Items is a slice of pointers to the documents that matched the query criteria. These documents are the results of the query.
	Items []*T

	// Keys corresponds to the keys of the documents that matched the query criteria. These documents are the results of the query.
	Keys [][]byte
	// Next is the index of the last item retrieved in the query result. It helps track the position in the collection.
	// It can be used to implement pagination by passing it as the Skip value in a subsequent query.
	Next int

	// Error is an error object that may contain any errors encountered during the query operation. It represents the overall query result status.
	Error error
}

// JSONResponse returns a map that can be used to generate a JSON response for the query result.
func (qr *QueryResult[T]) JSONResponse() map[string]any {
	if !qr.Any() {
		return map[string]any{
			"result": []any{},
			"count":  0,
			"next":   0,
		}
	}
	return map[string]any{
		"result": qr.Items,
		"count":  len(qr.Items),
		"next":   qr.Next,
	}
}

// Count returns the number of items in the query result.
func (qr *QueryResult[T]) Count() int {
	return len(qr.Items)
}

// First returns the first item in the query result. If the query result is empty, it returns a nil.
func (qr *QueryResult[T]) First() *T {
	if len(qr.Items) == 0 {
		return new(T)
	}
	return qr.Items[0]
}

// Any returns true if the query result contains any items.
func (qr *QueryResult[T]) Any() bool {
	return len(qr.Items) > 0
}

// Iter iterates over the items in the query result and executes the provided function for each item. If an error is returned by the function, the iteration is stopped and the error is returned.
func (qr *QueryResult[T]) Iter(f func(doc *T) error) *QueryResult[T] {
	if qr.Error != nil {
		return qr
	}
	for _, document := range qr.Items {
		err := f(document)
		if err != nil {
			qr.Error = err
			return qr
		}
	}
	return qr
}

// Filter filters the items in the query result using the provided function. The function should return true if the item should be included in the result, and false otherwise.
func (qr *QueryResult[T]) Filter(f func(doc *T) bool) *QueryResult[T] {
	if qr.Error != nil {
		return qr
	}
	var items []*T
	for _, document := range qr.Items {
		if f(document) {
			items = append(items, document)
		}
	}
	qr.Items = items
	return qr
}

// Validate validates the query result using the provided function. If the function returns an error, the query result error is set to that error.
func (qr *QueryResult[T]) Validate(f func(qr *QueryResult[T]) error) *QueryResult[T] {
	if qr.Error != nil {
		return qr
	}
	err := f(qr)
	if err != nil {
		qr.Error = err
	}
	return qr
}

// Delete deletes the items in the query result from the collection.
func (qr *QueryResult[T]) Delete() error {
	if qr.Error != nil {
		return qr.Error
	}
	return qr.Collection.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(qr.Collection.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", qr.Collection.Name)
		}

		for _, document := range qr.Items {
			if qr.Collection.beforeDelete != nil {
				err := qr.Collection.beforeDelete(document)
				if err != nil {
					return err
				}
			}

			err := bucket.Delete((*document).Key())
			if err != nil {
				return err
			}

			if qr.Collection.afterDelete != nil {
				err := qr.Collection.afterDelete(document)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// Update updates the items in the query result in the collection.
func (qr *QueryResult[T]) Update() error {
	if qr.Error != nil {
		return qr.Error
	}
	return qr.Collection.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(qr.Collection.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", qr.Collection.Name)
		}

		for _, document := range qr.Items {
			if qr.Collection.beforeUpdate != nil {
				err := qr.Collection.beforeUpdate(document)
				if err != nil {
					return err
				}
			}

			data, err := Marshaller.Marshal(document)
			if err != nil {
				return err
			}

			err = bucket.Put((*document).Key(), data)
			if err != nil {
				return err
			}

			if qr.Collection.afterUpdate != nil {
				err := qr.Collection.afterUpdate(document)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

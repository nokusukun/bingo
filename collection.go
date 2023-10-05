package bingo

import (
	"errors"
	"fmt"
	"go.etcd.io/bbolt"
	"strings"
)

// DocumentSpec represents a document that can be stored in a collection.
type DocumentSpec interface {
	Key() []byte
}

// Collection represents a collection of documents managed by a database driver.
type Collection[DocumentType DocumentSpec] struct {
	Driver       *Driver
	Name         string
	nameBytes    []byte
	beforeUpdate func(doc *DocumentType) error
	afterUpdate  func(doc *DocumentType) error
	beforeDelete func(doc *DocumentType) error
	afterDelete  func(doc *DocumentType) error
	beforeInsert func(doc *DocumentType) error
	afterInsert  func(doc *DocumentType) error
}

// BeforeUpdate registers a function to be called before a document is updated in the collection.
func (c *Collection[T]) BeforeUpdate(f func(doc *T) error) *Collection[T] {
	c.beforeUpdate = f
	return c
}

// AfterUpdate registers a function to be called after a document is updated in the collection.
func (c *Collection[T]) AfterUpdate(f func(doc *T) error) *Collection[T] {
	c.afterUpdate = f
	return c
}

// BeforeDelete registers a function to be called before a document is deleted from the collection.
func (c *Collection[T]) BeforeDelete(f func(doc *T) error) *Collection[T] {
	c.beforeDelete = f
	return c
}

// AfterDelete registers a function to be called after a document is deleted from the collection.
func (c *Collection[T]) AfterDelete(f func(doc *T) error) *Collection[T] {
	c.afterDelete = f
	return c
}

// BeforeInsert registers a function to be called before a document is inserted into the collection.
func (c *Collection[T]) BeforeInsert(f func(doc *T) error) *Collection[T] {
	c.beforeInsert = f
	return c
}

// AfterInsert registers a function to be called after a document is inserted into the collection.
func (c *Collection[T]) AfterInsert(f func(doc *T) error) *Collection[T] {
	c.afterInsert = f
	return c
}

// CollectionFrom creates a new collection with the specified driver and name.
func CollectionFrom[T DocumentSpec](driver *Driver, name string) *Collection[T] {
	if driver.Closed {
		panic(fmt.Errorf("driver is closed"))
	}
	return &Collection[T]{
		Driver:    driver,
		Name:      name,
		nameBytes: []byte(name),
	}
}

// InsertResult represents the result of an insert operation.
type InsertResult struct {
	Success    bool
	Errors     []error
	InsertedId []byte
}

// Error returns an error object that contains all the errors encountered during the insert operation.
func (ir *InsertResult) Error() error {
	if len(ir.Errors) == 0 {
		return nil
	}
	var s []string
	for _, err := range ir.Errors {
		s = append(s, err.Error())
	}
	return fmt.Errorf(strings.Join(s, ": "))
}

func (ir *InsertResult) fail(errs ...error) {
	for _, err := range errs {
		if err != nil {
			ir.Success = false
			ir.Errors = append(ir.Errors, err)
		}
	}
}

// Insert inserts a document into the collection. If the document already exists, an error is returned.
func (c *Collection[T]) Insert(document T) (ir *InsertResult) {
	_, err := c.FindById(document.Key())
	if err != nil && errors.Is(err, ErrDocumentNotFound) {
		return c.InsertOrUpsert(document)
	}

	if err != nil && !errors.Is(err, ErrDocumentNotFound) {
		return &InsertResult{Errors: []error{err}}
	}

	return &InsertResult{Errors: []error{ErrDocumentExists, fmt.Errorf("key %v already exists", string(document.Key()))}}

}

// InsertOrUpsert inserts a document into the collection. If the document already exists, it is updated instead.
func (c *Collection[T]) InsertOrUpsert(document T) (ir *InsertResult) {
	ir = &InsertResult{
		Success: true,
	}
	if err := c.Driver.val.Struct(document); err != nil {
		ir.fail(err)
		return
	}

	if c.beforeInsert != nil {
		err := c.beforeInsert(&document)
		if err != nil {
			ir.fail(err)
			return
		}
	}

	marshal, err := Marshaller.Marshal(document)
	if err != nil {
		ir.fail(err)
		return
	}
	var idBytes []byte
	ir.fail(c.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(c.nameBytes)
		if err != nil {
			return err
		}

		key := document.Key()
		if len(key) == 0 {
			uniqueId, _ := bucket.NextSequence()
			idBytes = []byte(fmt.Sprintf("%v", uniqueId))
		} else {
			idBytes = key
		}
		return bucket.Put(idBytes, marshal)
	}))

	if c.afterInsert != nil {
		err := c.afterInsert(&document)
		if err != nil {
			ir.fail(err)
			return
		}
	}

	ir.InsertedId = idBytes
	return
}

// FindById retrieves a document from the collection by its id. If the document is not found, an error is returned.
func (c *Collection[T]) FindById(id []byte) (T, error) {
	var document T
	err := c.Driver.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
		}
		value := bucket.Get(id)
		if value == nil {
			return errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
		}
		return Unmarshaller.Unmarshal(value, &document)
	})
	return document, err
}

var stoperr = fmt.Errorf("stop")

func (c *Collection[DocumentType]) queryKeys(keys ...string) []DocumentType {
	var documents []DocumentType
	_ = c.Driver.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		for _, key := range keys {
			value := bucket.Get([]byte(key))
			if value == nil {
				continue
			}
			var document DocumentType
			err := Unmarshaller.Unmarshal(value, &document)
			if err != nil {
				continue
			}
			documents = append(documents, document)
		}
		return nil
	})
	return documents
}

func (c *Collection[T]) queryFind(q Query[T]) ([]T, int, error) {
	var documents []T
	var currentFound = 0
	var last = 0
	err := c.Driver.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		wbucket := &WrappedBucket{bucket}
		return wbucket.ReverseForEach(func(k, v []byte) error {
			last += 1
			if last <= q.Skip {
				return nil
			}

			var document T
			err := Unmarshaller.Unmarshal(v, &document)
			if err != nil {
				return err
			}
			if q.Filter(document) {
				documents = append(documents, document)
				currentFound += 1
				if q.Count > 0 && currentFound >= q.Count {
					return stoperr
				}
			}
			return nil
		})
	})
	if err != nil && !errors.Is(err, stoperr) {
		return documents, last, err
	}

	return documents, last, err
}

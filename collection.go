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
	_, err := c.FindByBytesId(document.Key())
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

func (c *Collection[T]) FindOne(filter func(doc T) bool) (T, error) {
	var empty T
	r, _, err := c.queryFind(Query[T]{
		Filter: filter,
	})

	if err != nil {
		return empty, err
	}

	if len(r) == 0 {
		return empty, errors.Join(ErrDocumentNotFound, fmt.Errorf("document not found"))
	}

	return r[0], err
}

type FindOptsFunc func(opts *findOpts)

type findOpts struct {
	Skip  int
	Count int
}

func applyOpts[T DocumentSpec](query *Query[T], opts ...FindOptsFunc) {
	options := findOpts{}
	for _, opt := range opts {
		opt(&options)
	}
	query.Skip = options.Skip
	query.Count = options.Count
}

func Skip(skip int) FindOptsFunc {
	return func(opts *findOpts) {
		opts.Skip = skip
	}
}

func Count(count int) FindOptsFunc {
	return func(opts *findOpts) {
		opts.Count = count
	}
}

func (c *Collection[T]) Find(filter func(doc T) bool, opts ...FindOptsFunc) ([]T, error) {
	q := Query[T]{
		Filter: filter,
	}
	applyOpts[T](&q, opts...)

	r, _, err := c.queryFind(q)

	if err != nil {
		return nil, err
	}

	if len(r) == 0 {
		return nil, errors.Join(ErrDocumentNotFound, fmt.Errorf("document not found"))
	}

	return r, err
}

func (c *Collection[T]) Delete(docs ...T) error {
	for _, doc := range docs {
		if err := c.DeleteOne(doc); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collection[T]) Update(docs ...T) error {
	for _, doc := range docs {
		if err := c.UpdateOne(doc); err != nil {
			return err
		}
	}
	return nil
}

// FindByBytesId retrieves a document from the collection by its id. If the document is not found, an error is returned.
func (c *Collection[T]) FindByBytesId(id []byte) (T, error) {
	var document T
	r := c.queryKeys(id)
	if len(r) == 0 {
		return document, errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
	}
	document = r[0]
	return document, nil
}

// FindByBytesIds retrieves documents from the collection by their ids. If the document is not found, an empty list is returned.
func (c *Collection[T]) FindByBytesIds(ids ...[]byte) []T {
	return c.queryKeys(ids...)
}

// FindById retrieves a document from the collection by its id. If the document is not found, an error is returned.
func (c *Collection[T]) FindById(id string) (T, error) {
	var document T
	r := c.queryKeys([]byte(id))
	if len(r) == 0 {
		return document, errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
	}
	document = r[0]
	return document, nil
}

// FindByIds retrieves documents from the collection by their ids. If the document is not found, an empty list is returned.
func (c *Collection[T]) FindByIds(ids ...string) []T {
	idsBytes := make([][]byte, len(ids))
	for i, id := range ids {
		idsBytes[i] = []byte(id)
	}
	return c.queryKeys(idsBytes...)
}

// UpdateIter updates documents in the collection that match the filter function.
// The updateFunc is called on each document that matches the filter function.
// return the document from the updateFunc to update the document, otherwise return nil to skip the document.
func (c *Collection[T]) UpdateIter(updateFunc func(*T) *T) error {

	return c.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		wbucket := &WrappedBucket{bucket}
		return wbucket.ReverseIter(func(k, v []byte) error {
			var document T
			err := Unmarshaller.Unmarshal(v, &document)
			if err != nil {
				return err
			}
			newDocument := updateFunc(&document)
			if newDocument == nil {
				return nil
			}
			if c.beforeUpdate != nil {
				err := c.beforeUpdate(newDocument)
				if err != nil {
					return err
				}
			}

			marshal, err := Marshaller.Marshal(newDocument)
			if err != nil {
				return err
			}
			err = bucket.Put(document.Key(), marshal)
			if err != nil {
				return err
			}

			if c.afterUpdate != nil {
				err := c.afterUpdate(newDocument)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// DeleteIter deletes documents from the collection that match the filter function.
// The deleteFunc is called on each document that matches the filter function.
// return true from the deleteFunc to delete the document, otherwise return false to skip the document.
func (c *Collection[T]) DeleteIter(deleteFunc func(*T) bool) error {

	return c.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		wbucket := &WrappedBucket{bucket}
		return wbucket.ReverseIter(func(k, v []byte) error {
			var document T
			err := Unmarshaller.Unmarshal(v, &document)
			if err != nil {
				return err
			}
			if !deleteFunc(&document) {
				return nil
			}
			if c.beforeDelete != nil {
				err := c.beforeDelete(&document)
				if err != nil {
					return err
				}
			}

			err = bucket.Delete(document.Key())
			if err != nil {
				return err
			}

			if c.afterDelete != nil {
				err := c.afterDelete(&document)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// UpdateOne updates a document in the collection.
func (c *Collection[T]) UpdateOne(doc T) error {
	if c.beforeUpdate != nil {
		err := c.beforeUpdate(&doc)
		if err != nil {
			return err
		}
	}

	marshal, err := Marshaller.Marshal(doc)
	if err != nil {
		return err
	}

	err = c.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		return bucket.Put(doc.Key(), marshal)
	})
	if err != nil {
		return err
	}

	if c.afterUpdate != nil {
		err := c.afterUpdate(&doc)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteOne deletes a document from the collection.
func (c *Collection[T]) DeleteOne(doc T) error {
	if c.beforeDelete != nil {
		err := c.beforeDelete(&doc)
		if err != nil {
			return err
		}
	}

	err := c.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		return bucket.Delete(doc.Key())
	})
	if err != nil {
		return err
	}

	if c.afterDelete != nil {
		err := c.afterDelete(&doc)
		if err != nil {
			return err
		}
	}
	return nil
}

var stoperr = fmt.Errorf("stop")

func (c *Collection[DocumentType]) queryKeys(keys ...[]byte) []DocumentType {
	var documents []DocumentType
	_ = c.Driver.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.nameBytes)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", c.Name)
		}
		for _, key := range keys {
			value := bucket.Get(key)
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
		return wbucket.ReverseIter(func(k, v []byte) error {
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
	} else {
		return documents, last, nil
	}
}

// Query executes the query and returns a QueryResult object that contains the results of the query.
func (c *Collection[T]) Query(q Query[T]) *QueryResult[T] {
	if q.Keys != nil && q.Filter != nil {
		panic(fmt.Errorf("cannot use both key and filter"))
	}

	if len(q.KeysStr) > 0 {
		keys := make([][]byte, len(q.KeysStr))
		for i, key := range q.KeysStr {
			keys[i] = []byte(key)
		}
		q.Keys = append(q.Keys, keys...)
	}

	result := &QueryResult[T]{
		Collection: c,
	}
	if q.Keys != nil {
		items := c.queryKeys(q.Keys...)
		for _, item := range items {
			item := item
			result.Items = append(result.Items, &item)
		}
		return result
	}

	if q.Filter != nil {
		items, last, err := c.queryFind(q)
		if err != nil {
			result.Error = errors.Join(err, fmt.Errorf("error while querying"))
		}
		result.Last = last
		for _, item := range items {
			item := item
			result.Items = append(result.Items, &item)
		}
		return result
	}

	result.Error = fmt.Errorf("no query provided")
	return result
}

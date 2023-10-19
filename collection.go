package bingo

import (
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"go.etcd.io/bbolt"
	"reflect"
)

type KeyMap map[string]any

func (KeyMap) Key() []byte {
	return nil
}

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
	OnNewId      func(count int, document *DocumentType) []byte
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

type InsertOptions struct {
	IgnoreErrors bool
	Upsert       bool
}

func IgnoreErrors(opts *InsertOptions) {
	opts.IgnoreErrors = true
}

func Upsert(opts *InsertOptions) {
	opts.Upsert = true
}

// Insert inserts a document into the collection. If upsert and ignoreErrors are not set, an error is returned if the document already exists.
// If IgnoreErrors is passed without Upsert, the document is not inserted and no error is returned if the document already exists.
func (c *Collection[T]) Insert(document T, opts ...func(options *InsertOptions)) ([]byte, error) {
	ids, err := c.inserts([]T{document}, opts...)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids[0], nil
}

// InsertMany inserts a document into the collection. If the document already exists, an error is returned.
func (c *Collection[T]) InsertMany(documents []T, opts ...func(options *InsertOptions)) ([][]byte, error) {
	return c.inserts(documents, opts...)
}

func (c *Collection[T]) inserts(docs []T, opts ...func(options *InsertOptions)) ([][]byte, error) {
	opt := &InsertOptions{}
	for _, o := range opts {
		o(opt)
	}

	var results [][]byte
	err := c.Driver.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(c.nameBytes)
		if err != nil {
			return err
		}

		for _, doc := range docs {
			id, err := c.insertWithTx(bucket, doc, opt)
			if !opt.IgnoreErrors && err != nil {
				return err
			}
			results = append(results, id)
		}
		return nil
	})

	return results, err
}

func (c *Collection[T]) insertWithTx(bucket *bbolt.Bucket, doc T, opt *InsertOptions) ([]byte, error) {
	if !opt.Upsert {
		_, err := c.FindByBytesKey(doc.Key())
		if err == nil {
			return nil, ErrDocumentExists
		}
	}

	if err := c.Driver.val.Struct(doc); err != nil {
		return nil, err
	}

	if c.beforeInsert != nil {
		err := c.beforeInsert(&doc)
		if err != nil {
			return nil, err
		}
	}

	idBytes := c.getKey(bucket, &doc)

	marshal, err := Marshaller.Marshal(doc)
	if err != nil {
		return nil, err
	}

	err = bucket.Put(idBytes, marshal)

	if err != nil {
		return nil, err
	}

	if c.afterInsert != nil {
		err := c.afterInsert(&doc)
		if err != nil {
			return nil, err
		}
	}

	return idBytes, nil
}

var node *snowflake.Node

func (c *Collection[T]) getKey(bucket *bbolt.Bucket, doc *T) []byte {
	if node == nil {
		var err error
		node, err = snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Errorf("unable to create snowflake node: %v", err))
		}
	}
	var idBytes []byte

	key := (*doc).Key()
	if len(key) == 0 {
		idBytes = []byte(node.Generate().Base58())
		if c.OnNewId != nil {
			idBytes = c.OnNewId(bucket.Stats().KeyN, doc)
		}
		reflect.ValueOf(doc).Elem().FieldByName("ID").SetString(string(idBytes))
	} else {
		idBytes = key
	}
	return idBytes
}

func (c *Collection[T]) FindOneWithKey(filter func(doc T) bool) (T, []byte, error) {
	var empty T
	r, keys, _, err := c.queryFind(Query[T]{
		Filter: filter,
	})

	if err != nil {
		return empty, nil, err
	}

	if len(r) == 0 {
		return empty, nil, errors.Join(ErrDocumentNotFound, fmt.Errorf("document not found"))
	}

	return r[0], keys[0], err
}

func (c *Collection[T]) FindOne(filter func(doc T) bool) (T, error) {
	r, _, err := c.FindOneWithKey(filter)
	return r, err
}

type IterOptsFunc func(opts *iterOpts)

type iterOpts struct {
	Skip  int
	Count int
}

func applyOpts[T DocumentSpec](query *Query[T], opts ...IterOptsFunc) {
	options := iterOpts{}
	for _, opt := range opts {
		opt(&options)
	}
	query.Skip = options.Skip
	query.Count = options.Count
}

func Skip(skip int) IterOptsFunc {
	return func(opts *iterOpts) {
		opts.Skip = skip
	}
}

func Count(count int) IterOptsFunc {
	return func(opts *iterOpts) {
		opts.Count = count
	}
}

func (c *Collection[T]) FindWithKeys(filter func(doc T) bool, opts ...IterOptsFunc) ([]T, [][]byte, error) {
	q := Query[T]{
		Filter: filter,
	}
	applyOpts[T](&q, opts...)

	r, keys, _, err := c.queryFind(q)

	if err != nil {
		return nil, nil, err
	}

	if len(r) == 0 {
		return nil, nil, errors.Join(ErrDocumentNotFound, fmt.Errorf("document not found"))
	}

	return r, keys, err
}

func (c *Collection[T]) Find(filter func(doc T) bool, opts ...IterOptsFunc) ([]T, error) {
	r, _, err := c.FindWithKeys(filter, opts...)

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

// Deprecated: FindByBytesId retrieves a document from the collection by its id. If the document is not found, an error is returned.
// Use FindByBytesKey instead
func (c *Collection[T]) FindByBytesId(id []byte) (T, error) {
	var document T
	r := c.queryKeys(id)
	if len(r) == 0 {
		return document, errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
	}
	document = r[0]
	return document, nil
}

// FindByBytesKey retrieves a document from the collection by its id. If the document is not found, an error is returned.
func (c *Collection[T]) FindByBytesKey(id []byte) (T, error) {
	var document T
	r := c.queryKeys(id)
	if len(r) == 0 {
		return document, errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
	}
	document = r[0]
	return document, nil
}

// Deprecated: FindByBytesIds retrieves documents from the collection by their ids. If the document is not found, an empty list is returned.
// Use FindByBytesKeys instead
func (c *Collection[T]) FindByBytesIds(ids ...[]byte) []T {
	return c.queryKeys(ids...)
}

// FindByBytesKeys retrieves documents from the collection by their ids. If the document is not found, an empty list is returned.
func (c *Collection[T]) FindByBytesKeys(ids ...[]byte) []T {
	return c.queryKeys(ids...)
}

// Deprecated: FindById retrieves a document from the collection by its id. If the document is not found, an error is returned.
// Use FindByKey instead
func (c *Collection[T]) FindById(id string) (T, error) {
	var document T
	r := c.queryKeys([]byte(id))
	if len(r) == 0 {
		return document, errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
	}
	document = r[0]
	return document, nil
}

// FindByKey retrieves a document from the collection by its id. If the document is not found, an error is returned.
func (c *Collection[T]) FindByKey(id string) (T, error) {
	var document T
	r := c.queryKeys([]byte(id))
	if len(r) == 0 {
		return document, errors.Join(ErrDocumentNotFound, fmt.Errorf("document with id %v not found", string(id)))
	}
	document = r[0]
	return document, nil
}

// Deprecated: FindByIds retrieves documents from the collection by their ids. If the document is not found, an empty list is returned.
// Use FindByKeys instead
func (c *Collection[T]) FindByIds(ids ...string) []T {
	idsBytes := make([][]byte, len(ids))
	for i, id := range ids {
		idsBytes[i] = []byte(id)
	}
	return c.queryKeys(idsBytes...)
}

// FindByKeys retrieves documents from the collection by their ids. If the document is not found, an empty list is returned.
func (c *Collection[T]) FindByKeys(ids ...string) []T {
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

func (c *Collection[T]) queryFind(q Query[T]) ([]T, [][]byte, int, error) {
	var documents []T
	var keys [][]byte
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
				keys = append(keys, k)
				currentFound += 1
				if q.Count > 0 && currentFound >= q.Count {
					return stoperr
				}
			}
			return nil
		})
	})
	if err != nil && !errors.Is(err, stoperr) {
		return documents, keys, last, err
	} else {
		return documents, keys, last, nil
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
		for i, item := range items {
			item := item
			result.Items = append(result.Items, &item)
			result.Keys = append(result.Keys, q.Keys[i])
		}
		return result
	}

	if q.Filter != nil {
		items, keys, last, err := c.queryFind(q)
		if err != nil {
			result.Error = errors.Join(err, fmt.Errorf("error while querying"))
		}
		result.Next = last
		for i, item := range items {
			item := item
			result.Items = append(result.Items, &item)
			result.Keys = append(result.Keys, keys[i])
		}
		return result
	}

	result.Error = fmt.Errorf("no query provided")
	return result
}

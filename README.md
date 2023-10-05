# Bingo: A Generics First Database Wrapper

Bingo is a Golang library that provides a convenient wrapper for the `bbolt` database. 

Through the use of generics, it includes capabilities for CRUD operations, validation, preprocessing, and more.

## Features:

- **Simple CRUD**: Easily create, read, update, and delete documents in your `bbolt` database.
- **Validation**: Uses `go-playground/validator` for struct validation.
- **Querying**: Advanced querying capabilities using filters.
- **Hooks**: Execute functions before and after certain operations (Insert, Update, Delete).

## Installation

Install the library using `go get`:

```bash
go get -u github.com/nokusukun/bingodb
```


## Usage:

### 1. Initialize the Driver

Create a new driver instance:

```go
config := bingo.DriverConfiguration{
    DeleteNoVerify: false,
    Filename:       "mydb.db",
}
driver, err := bingo.NewDriver(config)
```


### 2. Define your document type

```go
type User struct {
	Username string `json:"username,omitempty" validate:"required,min=3,max=64"`
	Email    string `json:"email,omitempty" validate:"required,email"`
	Password string `json:"password,omitempty" preprocessor:"password-prep" validate:"required,min=6,max=64"`
}

func (u User) Key() []byte {
	return []byte(u.Username)
}

func (u *User) CheckPassword(password string) bool {
    current := u.Password
    if strings.HasPrefix(current, "hash:") {
        current = strings.TrimPrefix(current, "hash:")
        return bcrypt.CompareHashAndPassword([]byte(current), []byte(password)) == nil
    }
    return current == password
}

func (u *User) EnsureHashedPassword() error {
    if strings.HasPrefix("hash:", u.Password) {
        return nil
    }
    hashed, err := HashPassword(u.Password)
    if err != nil {
        return err
    }
    u.Password = fmt.Sprintf("hash:%s", hashed)
    return nil
}
```

### 3. Create a collection for your document type

```go
users := bingo.CollectionFrom[User](driver, "users")
```

You can also add hooks:

```go
users.BeforeUpdate(func(doc *User) error {
	return doc.EnsureHashedPassword()
})
```

### 4. CRUD Operations

**Inserting documents:**

```go
err = users.Insert(User{
	Username: "test",
	Password: "test123",
	Email:    "random@rrege.com",
}).Error()
if err != nil {
	panic(err)
}
```

**Querying documents:**

```go
result := users.Query(bingo.Query[User]{
	Filter: func(doc User) bool {
		return doc.Username == "test"
	},
})
if !result.Any() {
	panic("No user found")
}
```

### Example

The given example demonstrates the creation of a user collection, inserting a user, querying for the user, and checking if the password matches.

## More on Querying

### Setting Up

```go
package main

import (
	"fmt"
	"log"

	"github.com/nokusukun/bingodb"
)


type User struct {
	Username string `json:"username,omitempty" validate:"required,min=3,max=64"`
	Email    string `json:"email,omitempty" validate:"required,email"`
	Password string `json:"password,omitempty" preprocessor:"password-prep" validate:"required,min=6,max=64"`
	Active   bool   `json:"active,omitempty"`
}

func (u User) Key() []byte {
	return []byte(u.Username)
}

func (u *User) CheckPassword(password string) bool {
	current := u.Password
	if strings.HasPrefix(current, "hash:") {
		current = strings.TrimPrefix(current, "hash:")
		return bcrypt.CompareHashAndPassword([]byte(current), []byte(password)) == nil
	}
	return current == password
}

func (u *User) EnsureHashedPassword() error {
	if strings.HasPrefix("hash:", u.Password) {
		return nil
	}
	hashed, err := HashPassword(u.Password)
	if err != nil {
		return err
	}
	u.Password = fmt.Sprintf("hash:%s", hashed)
	return nil
}

func main() {
	config := bingo.DriverConfiguration{
		DeleteNoVerify: true,
		Filename:       "test.db",
	}
	driver, err := bingo.NewDriver(config)
	if err != nil {
		log.Fatalf("Failed to create new driver: %v", err)
	}

	userCollection := bingo.CollectionFrom[User](driver, "users")

	//... your operations ...
}
```


### Inserting a Document

```go
user := User{
	Username: "john_doe",
	Email:    "john.doe@example.com",
	Password: "password1234",
	Active:  true,
}

// Ensure password is hashed
_ = user.EnsureHashedPassword()

insertResult := userCollection.Insert(user)
if insertResult.Error() != nil {
	log.Fatalf("Failed to insert user: %v", insertResult.Error())
}
```

### Querying for Documents

```go
result := userCollection.Query(bingo.Query[User]{
    Filter: func(doc User) bool {
        return doc.Username == "john_doe"
    },
    Count: 1, // Only get the first result
})
if result.Any() {
	firstUser := result.First()
	fmt.Println("Found user:", firstUser.Username, firstUser.Email)
} else {
	fmt.Println("User not found")
}
```

### Pagination
    
```go
result := userCollection.Query(bingo.Query[User]{
    Filter: func(doc User) bool {
        return doc.Active 
    },
    Count: 10, // Get 10 results
    Skip: 150, // Skip n Users, this value is returned by the previous query as QueryResult.Next
})
```

### Validating a Query
The `Validate` method allows you to validate the query result before performing any operations on it. This is useful for checking if a document exists before updating or deleting it.

```go

newText := "Hello World!"

err := Posts.Query(bingo.Query[Post]{
    Keys: []string{c.Param("id")}, // Get only the post with the given ID
}).Validate(func(qr *bingo.QueryResult[Post]) error {
    if !qr.Any() { 
        return fmt.Errorf("404::post not found") // Return a premature error if no post was found
    }
    return nil
}).ForEach(func(doc *Post) error {
    if doc.Author != username {
        return fmt.Errorf("401::you are not the author of this post")
    }
    doc.Content = newText
    doc.Edited = time.Now().Unix()
    return nil
}).Update()

```

### Further Filtering a Query result
If for some reason you need to further filter the query result, you can use the `Filter` method:

```go
newText := "The contents of this post has been deleted"

err := Posts.Query(bingo.Query[Post]{
    Filter: func(doc Post) bool {
        return doc.Author == username
    }, 
}).Validate(func(qr *bingo.QueryResult[Post]) error {
    if !qr.Any() { 
        return fmt.Errorf("404::posts not found") // Return a premature error if no post was found
    }
    return nil
}).Filter(func (doc Post) bool {
    return doc.Edited == 0 // Only get posts that have not been edited
}).ForEach(func(doc *Post) error {
    doc.Content = newText
    doc.Edited = time.Now().Unix()
    return nil
}).Update()
```

### Updating a Document

```go
err := userCollection.Query(bingo.Query[User]{
    Filter: func(doc User) bool {
        return doc.Username == "john_doe"
    },
    Count: 1, // Only get the first result
}).Validate(func(qr *bingo.QueryResult[Post]) error {
    if !qr.Any() {
        return fmt.Errorf("404::user not found") // Return a premature error if no post was found
    }
    return nil
}).ForEach(func(doc *User) error {
	doc.Email = "new.email@example.com"
	return nil
}).Update()

if err != nil {
	log.Fatalf("Failed to update user: %v", err)
}
```

### Deleting a Document

```go
err := userCollection.Query(bingo.Query[User]{
    Filter: func(doc User) bool {
            return doc.Username == "john_doe"
        },
        Count: 1, // Only get the first result
}).Validate(func(qr *bingo.QueryResult[Post]) error {
    if !qr.Any() {
        return fmt.Errorf("404::user not found") // Return a premature error if no post was found
    }
    return nil
}).Delete()

if err != nil {
	log.Fatalf("Failed to delete user: %v", err)
}
```

### Custom Operations with Middleware

If you want to have custom operations before or after inserting, updating, or deleting users, you can define them as:

```go
userCollection.BeforeInsert(func(u *User) error {
	return u.EnsureHashedPassword()
})

userCollection.AfterInsert(func(u *User) error {
	fmt.Println("User inserted:", u.Username)
	return nil
})

userCollection.BeforeUpdate(func(u *User) error {
	return u.EnsureHashedPassword()
})

userCollection.AfterUpdate(func(u *User) error {
	fmt.Println("User updated:", u.Username)
	return nil
})

userCollection.BeforeDelete(func(u *User) error {
	fmt.Println("Deleting user:", u.Username)
	return nil
})
```

### Error Handling

The library provides helper functions to check for specific errors:

```go
err := users.Insert(newUser)
if bingo.IsErrDocumentExists(err) {
	fmt.Println("Document already exists!")
} else if bingo.IsErrDocumentNotFound(err) {
	fmt.Println("Document not found!")
}
```

## Important Methods

### `NewDriver()`
This method initializes a new connection to the database.

### `CollectionFrom()`
This is a factory method to create a new collection for a specific document type.

### `Insert()`
Inserts a document into the collection.

### `Query()`
Query documents using a custom filter.


## Error Handling

Special error types are provided for common error scenarios:

- `bingo.ErrDocumentNotFound`: When a document is not found in the collection.
- `bingo.ErrDocumentExists`: When attempting to insert a document with an existing key.

Helper functions like `IsErrDocumentNotFound` and `IsErrDocumentExists` are available for easy error checking.

## Safety Measures

For destructive operations like `Drop`, safety checks are in place. By default, you need to set environment variables to permit such operations:

```bash
export BINGO_ALLOW_DROP_MY_COLLECTION_NAME=true
```

## Dependencies

Bingo relies on the following third-party packages:

- [github.com/go-playground/validator/v10](https://github.com/go-playground/validator)
- [github.com/json-iterator/go](https://github.com/json-iterator/go)
- [go.etcd.io/bbolt](https://github.com/etcd-io/bbolt)

## Contributing

If you'd like to contribute to the development of Bingo, please submit a pull request or open an issue.

## License

This library is released under the MIT License.

---

Please make sure to adhere to the terms of use and licensing of the third-party dependencies if you decide to use this library.

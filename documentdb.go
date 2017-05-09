//
// This project start as a fork of `github.com/nerdylikeme/go-documentdb` version
// but changed, and may be changed later
//
// Goal: add the full functionality of documentdb, align with the other sdks
// and make it more testable
//
package documentdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrNotFound = errors.New("not found")
)

func IsExists(err error) bool {
	if e, ok := err.(*RequestError); ok && e.Code == "Conflict" {
		return true
	}
	return false
}

// Id setter
func Doc(id string) Document {
	return Document{
		Resource: Resource{
			Id: id,
		},
	}
}

type Config struct {
	MasterKey string
}

type DocumentDB struct {
	client Clienter
}

// Create DocumentDBClient
func New(url string, config Config) *DocumentDB {
	client := &Client{}
	client.Url = url
	client.Config = config
	return &DocumentDB{client}
}

func selectByID(id string) string {
	return fmt.Sprintf("SELECT * FROM ROOT r WHERE r.id='%s'", id)
}

func (c *DocumentDB) CreateDB(id string) (*DB, error) {
	d, err := c.CreateDatabase(map[string]string{"id": id})
	if err != nil {
		return nil, err
	}
	return &DB{c: c, Database: *d}, nil
}

func (c *DocumentDB) CreateDBIfNotExists(id string) (*DB, error) {
	d, err := c.CreateDB(id)
	if IsExists(err) {
		return c.DB(id)
	}
	return d, err
}

func (c *DocumentDB) DB(id string) (*DB, error) {
	dbs, err := c.QueryDatabases(selectByID(id))
	if err != nil {
		return nil, err
	} else if len(dbs) == 0 {
		return nil, ErrNotFound
	}
	return &DB{c: c, Database: dbs[0]}, nil
}

type DB struct {
	c *DocumentDB
	Database
}

func (db *DB) CreateCollection(id string) (*Col, error) {
	c, err := db.c.CreateCollection(db.Self, map[string]string{"id": id})
	if err != nil {
		return nil, err
	}
	return &Col{db: db, Collection: *c}, nil
}

func (db *DB) CreateCollectionIfNotExists(id string) (*Col, error) {
	c, err := db.CreateCollection(id)
	if IsExists(err) {
		return db.C(id)
	}
	return c, err
}

func (db *DB) C(id string) (*Col, error) {
	colls, err := db.c.QueryCollections(db.Self, selectByID(id))
	if err != nil {
		return nil, err
	} else if len(colls) == 0 {
		return nil, ErrNotFound
	}
	return &Col{db: db, Collection: colls[0]}, nil
}

type Col struct {
	db *DB
	Collection
}

func (c *Col) CreateProc(id, fnc string) (*Proc, error) {
	proc, err := c.db.c.CreateStoredProcedure(c.Self, map[string]string{
		"id": id, "body": fnc,
	})
	if err != nil {
		return nil, err
	}
	return &Proc{c: c, Sproc: *proc}, nil
}

func (c *Col) Proc(id string) (*Proc, error) {
	procs, err := c.db.c.QueryStoredProcedures(c.Self, selectByID(id))
	if err != nil {
		return nil, err
	} else if len(procs) == 0 {
		return nil, ErrNotFound
	}
	return &Proc{c: c, Sproc: procs[0]}, nil
}

type Proc struct {
	c *Col
	Sproc
}

func (p *Proc) Execute(out interface{}, args ...interface{}) error {
	var params interface{}
	if len(args) != 0 {
		params = args
	}
	var data json.RawMessage
	err := p.c.db.c.ExecuteStoredProcedure(p.Self, params, &data)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// TODO: Add `requestOptions` arguments
// Read database by self link
func (c *DocumentDB) ReadDatabase(link string) (db *Database, err error) {
	err = c.client.Read(link, &db)
	if err != nil {
		return nil, err
	}
	return
}

// Read collection by self link
func (c *DocumentDB) ReadCollection(link string) (coll *Collection, err error) {
	err = c.client.Read(link, &coll)
	if err != nil {
		return nil, err
	}
	return
}

// Read document by self link
func (c *DocumentDB) ReadDocument(link string, doc interface{}) (err error) {
	err = c.client.Read(link, &doc)
	return
}

// Read sporc by self link
func (c *DocumentDB) ReadStoredProcedure(link string) (sproc *Sproc, err error) {
	err = c.client.Read(link, &sproc)
	if err != nil {
		return nil, err
	}
	return
}

// Read udf by self link
func (c *DocumentDB) ReadUserDefinedFunction(link string) (udf *UDF, err error) {
	err = c.client.Read(link, &udf)
	if err != nil {
		return nil, err
	}
	return
}

// Read all databases
func (c *DocumentDB) ReadDatabases() (dbs []Database, err error) {
	return c.QueryDatabases("")
}

// Read all collections by db selflink
func (c *DocumentDB) ReadCollections(db string) (colls []Collection, err error) {
	return c.QueryCollections(db, "")
}

// Read all sprocs by collection self link
func (c *DocumentDB) ReadStoredProcedures(coll string) (sprocs []Sproc, err error) {
	return c.QueryStoredProcedures(coll, "")
}

// Read all udfs by collection self link
func (c *DocumentDB) ReadUserDefinedFunctions(coll string) (udfs []UDF, err error) {
	return c.QueryUserDefinedFunctions(coll, "")
}

// Read all collection documents by self link
// TODO: use iterator for heavy transactions
func (c *DocumentDB) ReadDocuments(coll string, docs interface{}) (err error) {
	return c.QueryDocuments(coll, "", docs)
}

// Read all databases that satisfy a query
func (c *DocumentDB) QueryDatabases(query string) (dbs []Database, err error) {
	data := struct {
		Databases []Database `json:"Databases,omitempty"`
		Count     int        `json:"_count,omitempty"`
	}{}
	if len(query) > 0 {
		err = c.client.Query("dbs", query, &data)
	} else {
		err = c.client.Read("dbs", &data)
	}
	if dbs = data.Databases; err != nil {
		dbs = nil
	}
	return
}

// Read all db-collection that satisfy a query
func (c *DocumentDB) QueryCollections(db, query string) (colls []Collection, err error) {
	data := struct {
		Collections []Collection `json:"DocumentCollections,omitempty"`
		Count       int          `json:"_count,omitempty"`
	}{}
	if len(query) > 0 {
		err = c.client.Query(db+"colls/", query, &data)
	} else {
		err = c.client.Read(db+"colls/", &data)
	}
	if colls = data.Collections; err != nil {
		colls = nil
	}
	return
}

// Read all collection `sprocs` that satisfy a query
func (c *DocumentDB) QueryStoredProcedures(coll, query string) (sprocs []Sproc, err error) {
	data := struct {
		Sprocs []Sproc `json:"StoredProcedures,omitempty"`
		Count  int     `json:"_count,omitempty"`
	}{}
	if len(query) > 0 {
		err = c.client.Query(coll+"sprocs/", query, &data)
	} else {
		err = c.client.Read(coll+"sprocs/", &data)
	}
	if sprocs = data.Sprocs; err != nil {
		sprocs = nil
	}
	return
}

// Read all collection `udfs` that satisfy a query
func (c *DocumentDB) QueryUserDefinedFunctions(coll, query string) (udfs []UDF, err error) {
	data := struct {
		Udfs  []UDF `json:"UserDefinedFunctions,omitempty"`
		Count int   `json:"_count,omitempty"`
	}{}
	if len(query) > 0 {
		err = c.client.Query(coll+"udfs/", query, &data)
	} else {
		err = c.client.Read(coll+"udfs/", &data)
	}
	if udfs = data.Udfs; err != nil {
		udfs = nil
	}
	return
}

// Read all documents in a collection that satisfy a query
func (c *DocumentDB) QueryDocuments(coll, query string, docs interface{}) (err error) {
	data := struct {
		Documents interface{} `json:"Documents,omitempty"`
		Count     int         `json:"_count,omitempty"`
	}{Documents: docs}
	if len(query) > 0 {
		err = c.client.Query(coll+"docs/", query, &data)
	} else {
		err = c.client.Read(coll+"docs/", &data)
	}
	return
}

// Create database
func (c *DocumentDB) CreateDatabase(body interface{}) (db *Database, err error) {
	err = c.client.Create("dbs", body, &db, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Create collection
func (c *DocumentDB) CreateCollection(db string, body interface{}) (coll *Collection, err error) {
	err = c.client.Create(db+"colls/", body, &coll, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Create stored procedure
func (c *DocumentDB) CreateStoredProcedure(coll string, body interface{}) (sproc *Sproc, err error) {
	err = c.client.Create(coll+"sprocs/", body, &sproc, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Create user defined function
func (c *DocumentDB) CreateUserDefinedFunction(coll string, body interface{}) (udf *UDF, err error) {
	err = c.client.Create(coll+"udfs/", body, &udf, nil)
	if err != nil {
		return nil, err
	}
	return
}

func (c *DocumentDB) createDocument(coll string, doc interface{}, headers map[string]string) error {
	id := reflect.ValueOf(doc).Elem().FieldByName("Id")
	if id.IsValid() && id.String() == "" {
		id.SetString(uuid())
	}
	return c.client.Create(coll+"docs/", doc, &doc, headers)
}

// Create document
func (c *DocumentDB) CreateDocument(coll string, doc interface{}) error {
	return c.createDocument(coll, doc, nil)
}

// Create document
func (c *DocumentDB) UpsertDocument(coll string, doc interface{}) error {
	headers := map[string]string{
		HEADER_UPSERT: "true",
	}
	return c.createDocument(coll, doc, headers)
}

// TODO: DRY, but the sdk want that[mm.. maybe just client.Delete(self_link)]
// Delete database
func (c *DocumentDB) DeleteDatabase(link string) error {
	return c.client.Delete(link)
}

// Delete collection
func (c *DocumentDB) DeleteCollection(link string) error {
	return c.client.Delete(link)
}

// Delete collection
func (c *DocumentDB) DeleteDocument(link string) error {
	return c.client.Delete(link)
}

// Delete stored procedure
func (c *DocumentDB) DeleteStoredProcedure(link string) error {
	return c.client.Delete(link)
}

// Delete user defined function
func (c *DocumentDB) DeleteUserDefinedFunction(link string) error {
	return c.client.Delete(link)
}

// Replace database
func (c *DocumentDB) ReplaceDatabase(link string, body interface{}) (db *Database, err error) {
	err = c.client.Replace(link, body, &db)
	if err != nil {
		return nil, err
	}
	return
}

// Replace document
func (c *DocumentDB) ReplaceDocument(link string, doc interface{}) error {
	return c.client.Replace(link, doc, &doc)
}

// Replace stored procedure
func (c *DocumentDB) ReplaceStoredProcedure(link string, body interface{}) (sproc *Sproc, err error) {
	err = c.client.Replace(link, body, &sproc)
	if err != nil {
		return nil, err
	}
	return
}

// Replace stored procedure
func (c *DocumentDB) ReplaceUserDefinedFunction(link string, body interface{}) (udf *UDF, err error) {
	err = c.client.Replace(link, body, &udf)
	if err != nil {
		return nil, err
	}
	return
}

// Execute stored procedure
func (c *DocumentDB) ExecuteStoredProcedure(link string, params, body interface{}) (err error) {
	err = c.client.Execute(link, params, &body)
	return
}

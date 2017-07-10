//
// This project start as a fork of `github.com/nerdylikeme/go-documentdb` version
// but changed, and may be changed later
//
// Goal: add the full functionality of documentdb, align with the other sdks
// and make it more testable
//
package documentdb

import (
	"errors"
	"net/http"
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
	client := &Client{
		Url:    url,
		Config: config,
		Client: http.DefaultClient,
	}
	return &DocumentDB{client}
}

func selectByID(id string) *Query {
	return &Query{
		Text:   "SELECT * FROM ROOT r WHERE r.id = @id",
		Params: []QueryParam{{Name: "@id", Value: id}},
	}
}

func (c *DocumentDB) CreateDB(id string) (*DB, error) {
	d, err := c.CreateDatabase(map[string]string{"id": id})
	if err != nil {
		return nil, err
	}
	return &DB{c: c, Database: *d}, nil
}

func (c *DocumentDB) CreateDBIfNotExists(id string) (*DB, error) {
	db, err := c.DB(id)
	if err == ErrNotFound {
		if db, err = c.CreateDB(id); IsExists(err) {
			db, err = c.DB(id)
		}
	}
	return db, err
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

func (db *DB) Delete() error {
	return db.c.DeleteDatabase(db.Self)
}

func (db *DB) CreateCollection(id string, col *Collection) (*Col, error) {
	if col == nil {
		col = &Collection{}
	}
	col.Id = id
	c, err := db.c.CreateCollection(db.Self, col)
	if err != nil {
		return nil, err
	}
	return &Col{db: db, Collection: *c}, nil
}

func (db *DB) CreateCollectionIfNotExists(id string, col *Collection) (*Col, error) {
	c, err := db.C(id)
	if err == ErrNotFound {
		if c, err = db.CreateCollection(id, col); IsExists(err) {
			c, err = db.C(id)
		}
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

func (c *Col) Delete() error {
	return c.db.c.DeleteCollection(c.Self)
}

func (c *Col) QueryDocuments(qu *Query, out interface{}) (string, error) {
	return c.db.c.QueryDocuments(c.Self, qu, out)
}

func (c *Col) CreateDocument(doc interface{}) error {
	return c.db.c.CreateDocument(c.Self, doc)
}

func (c *Col) UpsertDocument(doc interface{}, etag string) error {
	return c.db.c.UpsertDocument(c.Self, doc, etag)
}

func (c *Col) DeleteDocumentByLink(link string) error {
	return c.db.c.DeleteDocument(link)
}

func (c *Col) CreateProc(id, fnc string) (*Proc, error) {
	p := &Proc{c: c, Sproc: Sproc{Body: fnc}}
	p.Id = id
	if err := c.db.c.CreateStoredProcedure(c.Self, &p.Sproc); err != nil {
		return nil, err
	}
	return p, nil
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
	return p.c.db.c.ExecuteStoredProcedure(p.Self, params, out)
}

// TODO: Add `requestOptions` arguments
// Read database by self link
func (c *DocumentDB) ReadDatabase(link string) (db *Database, err error) {
	_, err = c.client.Query(link, nil, &db)
	if err != nil {
		return nil, err
	}
	return
}

// Read collection by self link
func (c *DocumentDB) ReadCollection(link string) (coll *Collection, err error) {
	_, err = c.client.Query(link, nil, &coll)
	if err != nil {
		return nil, err
	}
	return
}

// Read document by self link
func (c *DocumentDB) ReadDocument(link string, doc interface{}) (err error) {
	_, err = c.client.Query(link, nil, &doc)
	return
}

// Read sporc by self link
func (c *DocumentDB) ReadStoredProcedure(link string) (sproc *Sproc, err error) {
	_, err = c.client.Query(link, nil, &sproc)
	if err != nil {
		return nil, err
	}
	return
}

// Read udf by self link
func (c *DocumentDB) ReadUserDefinedFunction(link string) (udf *UDF, err error) {
	_, err = c.client.Query(link, nil, &udf)
	if err != nil {
		return nil, err
	}
	return
}

// Read all databases
func (c *DocumentDB) ReadDatabases() (dbs []Database, err error) {
	return c.QueryDatabases(nil)
}

// Read all collections by db selflink
func (c *DocumentDB) ReadCollections(db string) (colls []Collection, err error) {
	return c.QueryCollections(db, nil)
}

// Read all sprocs by collection self link
func (c *DocumentDB) ReadStoredProcedures(coll string) (sprocs []Sproc, err error) {
	return c.QueryStoredProcedures(coll, nil)
}

// Read all udfs by collection self link
func (c *DocumentDB) ReadUserDefinedFunctions(coll string) (udfs []UDF, err error) {
	return c.QueryUserDefinedFunctions(coll, nil)
}

// Read all collection documents by self link
// TODO: use iterator for heavy transactions
func (c *DocumentDB) ReadDocuments(coll string, ctoken string, docs interface{}) (token string, err error) {
	var q *Query
	if ctoken != "" {
		q = &Query{Token: ctoken}
	}
	return c.QueryDocuments(coll, q, docs)
}

// Read all databases that satisfy a query
func (c *DocumentDB) QueryDatabases(query *Query) (dbs []Database, err error) {
	var data struct {
		Databases []Database `json:"Databases,omitempty"`
		Count     int        `json:"_count,omitempty"`
	}
	_, err = c.client.Query("dbs", query, &data)
	if dbs = data.Databases; err != nil {
		dbs = nil
	}
	return
}

// Read all db-collection that satisfy a query
func (c *DocumentDB) QueryCollections(db string, query *Query) (colls []Collection, err error) {
	var data struct {
		Collections []Collection `json:"DocumentCollections,omitempty"`
		Count       int          `json:"_count,omitempty"`
	}
	_, err = c.client.Query(db+"colls/", query, &data)
	if colls = data.Collections; err != nil {
		colls = nil
	}
	return
}

// Read all collection `sprocs` that satisfy a query
func (c *DocumentDB) QueryStoredProcedures(coll string, query *Query) (sprocs []Sproc, err error) {
	var data struct {
		Sprocs []Sproc `json:"StoredProcedures,omitempty"`
		Count  int     `json:"_count,omitempty"`
	}
	_, err = c.client.Query(coll+"sprocs/", query, &data)
	if sprocs = data.Sprocs; err != nil {
		sprocs = nil
	}
	return
}

// Read all collection `udfs` that satisfy a query
func (c *DocumentDB) QueryUserDefinedFunctions(coll string, query *Query) (udfs []UDF, err error) {
	var data struct {
		Udfs  []UDF `json:"UserDefinedFunctions,omitempty"`
		Count int   `json:"_count,omitempty"`
	}
	_, err = c.client.Query(coll+"udfs/", query, &data)
	if udfs = data.Udfs; err != nil {
		udfs = nil
	}
	return
}

// Read all documents in a collection that satisfy a query
func (c *DocumentDB) QueryDocuments(coll string, query *Query, docs interface{}) (token string, err error) {
	data := struct {
		Documents interface{} `json:"Documents,omitempty"`
		Count     int         `json:"_count,omitempty"`
	}{Documents: docs}
	return c.client.Query(coll+"docs/", query, &data)
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
func (c *DocumentDB) CreateStoredProcedure(coll string, sproc *Sproc) error {
	return c.client.Create(coll+"sprocs/", sproc, sproc, nil)
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
	rv := reflect.ValueOf(doc)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if id := rv.FieldByName("Id"); id.IsValid() && id.String() == "" {
		id.SetString(uuid())
	}
	return c.client.Create(coll+"docs/", doc, &doc, headers)
}

// Create document
func (c *DocumentDB) CreateDocument(coll string, doc interface{}) error {
	return c.createDocument(coll, doc, nil)
}

// Create document
func (c *DocumentDB) UpsertDocument(coll string, doc interface{}, etag string) error {
	headers := map[string]string{
		HEADER_UPSERT: "true",
	}
	if etag != "" {
		headers[HEADER_IF_MATCH] = etag
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
	err = c.client.Execute(link, params, body)
	return
}

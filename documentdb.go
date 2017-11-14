//
// This project start as a fork of `github.com/nerdylikeme/go-documentdb` version
// but changed, and may be changed later
//
// Goal: add the full functionality of documentdb, align with the other sdks
// and make it more testable
//
package documentdb

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
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
	MasterKey  string
	MaxRetries int
}

type DocumentDB struct {
	client Clienter
}

// Create DocumentDBClient
func New(url string, config Config) *DocumentDB {
	client := &Client{
		Url:    strings.Trim(url, "/"),
		Config: config,
		Client: http.DefaultClient,
	}
	return &DocumentDB{client}
}

func IdQuery(id string) *Query {
	return &Query{
		Text:   "SELECT * FROM ROOT r WHERE r.id = @id",
		Params: []QueryParam{{Name: "@id", Value: id}},
	}
}

func (c *DocumentDB) CreateDB(ctx context.Context, id string) (*DB, error) {
	d, err := c.CreateDatabase(ctx, map[string]string{"id": id})
	if err != nil {
		return nil, err
	}
	return &DB{c: c, Database: *d}, nil
}

func (c *DocumentDB) CreateDBIfNotExists(ctx context.Context, id string) (*DB, error) {
	db, err := c.DB(ctx, id)
	if err == ErrNotFound {
		if db, err = c.CreateDB(ctx, id); IsExists(err) {
			db, err = c.DB(ctx, id)
		}
	}
	return db, err
}

func (c *DocumentDB) DB(ctx context.Context, id string) (*DB, error) {
	dbs, err := c.QueryDatabases(ctx, IdQuery(id))
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

func (db *DB) Delete(ctx context.Context) error {
	return db.c.DeleteDatabase(ctx, db.Self)
}

func (db *DB) CreateCollection(ctx context.Context, id string, col *Collection) (*Col, error) {
	if col == nil {
		col = &Collection{}
	}
	col.Id = id
	c, err := db.c.CreateCollection(ctx, db.Self, col)
	if err != nil {
		return nil, err
	}
	return &Col{db: db, Collection: *c}, nil
}

func (db *DB) CreateCollectionIfNotExists(ctx context.Context, id string, col *Collection) (*Col, error) {
	c, err := db.C(ctx, id)
	if err == ErrNotFound {
		if c, err = db.CreateCollection(ctx, id, col); IsExists(err) {
			c, err = db.C(ctx, id)
		}
	}
	return c, err
}

func (db *DB) C(ctx context.Context, id string) (*Col, error) {
	colls, err := db.c.QueryCollections(ctx, db.Self, IdQuery(id))
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

func (c *Col) ctx(ctx context.Context) context.Context {
	return context.WithValue(ctx, collKey{}, string(c.Collection.Id))
}
func (c *Col) Delete(ctx context.Context) error {
	return c.db.c.DeleteCollection(c.ctx(ctx), c.Self)
}

func (c *Col) QueryDocuments(ctx context.Context, qu *Query, out interface{}) (string, error) {
	return c.db.c.QueryDocuments(c.ctx(ctx), c.Self, qu, out)
}

func (c *Col) CreateDocument(ctx context.Context, doc interface{}) (*Document, error) {
	return c.db.c.CreateDocument(c.ctx(ctx), c.Self, doc)
}

func (c *Col) UpdateDocument(ctx context.Context, doc interface{}, etag string) (*Document, error) {
	return c.db.c.UpdateDocument(c.ctx(ctx), c.Self, doc, etag)
}

func (c *Col) UpsertDocument(ctx context.Context, doc interface{}, etag string) (*Document, error) {
	return c.db.c.UpsertDocument(c.ctx(ctx), c.Self, doc, etag)
}

func (c *Col) DeleteDocumentByLink(ctx context.Context, link string, etag string) error {
	return c.db.c.DeleteDocument(c.ctx(ctx), link, etag)
}

func (c *Col) CreateProc(ctx context.Context, id, fnc string) (*Proc, error) {
	p := &Proc{c: c, Sproc: Sproc{Body: fnc}}
	p.Id = id
	if err := c.db.c.CreateStoredProcedure(c.ctx(ctx), c.Self, &p.Sproc); err != nil {
		return nil, err
	}
	return p, nil
}

func (c *Col) Proc(ctx context.Context, id string) (*Proc, error) {
	procs, err := c.db.c.QueryStoredProcedures(c.ctx(ctx), c.Self, IdQuery(id))
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

func (p *Proc) Execute(ctx context.Context, out interface{}, args ...interface{}) error {
	var params interface{}
	if len(args) != 0 {
		params = args
	}
	ctx = context.WithValue(ctx, sprocKey{}, string(p.Id))
	return p.c.db.c.ExecuteStoredProcedure(ctx, p.Self, params, out)
}

// TODO: Add `requestOptions` arguments
// Read database by self link
func (c *DocumentDB) ReadDatabase(ctx context.Context, link string) (db *Database, err error) {
	_, err = c.client.Query(ctx, link, nil, &db)
	if err != nil {
		return nil, err
	}
	return
}

// Read collection by self link
func (c *DocumentDB) ReadCollection(ctx context.Context, link string) (coll *Collection, err error) {
	_, err = c.client.Query(ctx, link, nil, &coll)
	if err != nil {
		return nil, err
	}
	return
}

// Read document by self link
func (c *DocumentDB) ReadDocument(ctx context.Context, link string, doc interface{}) (err error) {
	_, err = c.client.Query(ctx, link, nil, &doc)
	return
}

// Read sporc by self link
func (c *DocumentDB) ReadStoredProcedure(ctx context.Context, link string) (sproc *Sproc, err error) {
	_, err = c.client.Query(ctx, link, nil, &sproc)
	if err != nil {
		return nil, err
	}
	return
}

// Read udf by self link
func (c *DocumentDB) ReadUserDefinedFunction(ctx context.Context, link string) (udf *UDF, err error) {
	_, err = c.client.Query(ctx, link, nil, &udf)
	if err != nil {
		return nil, err
	}
	return
}

// Read all databases
func (c *DocumentDB) ReadDatabases(ctx context.Context) (dbs []Database, err error) {
	return c.QueryDatabases(ctx, nil)
}

// Read all collections by db selflink
func (c *DocumentDB) ReadCollections(ctx context.Context, db string) (colls []Collection, err error) {
	return c.QueryCollections(ctx, db, nil)
}

// Read all sprocs by collection self link
func (c *DocumentDB) ReadStoredProcedures(ctx context.Context, coll string) (sprocs []Sproc, err error) {
	return c.QueryStoredProcedures(ctx, coll, nil)
}

// Read all udfs by collection self link
func (c *DocumentDB) ReadUserDefinedFunctions(ctx context.Context, coll string) (udfs []UDF, err error) {
	return c.QueryUserDefinedFunctions(ctx, coll, nil)
}

// Read all collection documents by self link
// TODO: use iterator for heavy transactions
func (c *DocumentDB) ReadDocuments(ctx context.Context, coll string, ctoken string, docs interface{}) (token string, err error) {
	var q *Query
	if ctoken != "" {
		q = &Query{Token: ctoken}
	}
	return c.QueryDocuments(ctx, coll, q, docs)
}

// Read all databases that satisfy a query
func (c *DocumentDB) QueryDatabases(ctx context.Context, query *Query) (dbs []Database, err error) {
	var data struct {
		Databases []Database `json:"Databases,omitempty"`
		Count     int        `json:"_count,omitempty"`
	}
	_, err = c.client.Query(ctx, "dbs", query, &data)
	if dbs = data.Databases; err != nil {
		dbs = nil
	}
	return
}

// Read all db-collection that satisfy a query
func (c *DocumentDB) QueryCollections(ctx context.Context, db string, query *Query) (colls []Collection, err error) {
	var data struct {
		Collections []Collection `json:"DocumentCollections,omitempty"`
		Count       int          `json:"_count,omitempty"`
	}
	_, err = c.client.Query(ctx, db+"colls/", query, &data)
	if colls = data.Collections; err != nil {
		colls = nil
	}
	return
}

// Read all collection `sprocs` that satisfy a query
func (c *DocumentDB) QueryStoredProcedures(ctx context.Context, coll string, query *Query) (sprocs []Sproc, err error) {
	var data struct {
		Sprocs []Sproc `json:"StoredProcedures,omitempty"`
		Count  int     `json:"_count,omitempty"`
	}
	_, err = c.client.Query(ctx, coll+"sprocs/", query, &data)
	if sprocs = data.Sprocs; err != nil {
		sprocs = nil
	}
	return
}

// Read all collection `udfs` that satisfy a query
func (c *DocumentDB) QueryUserDefinedFunctions(ctx context.Context, coll string, query *Query) (udfs []UDF, err error) {
	var data struct {
		Udfs  []UDF `json:"UserDefinedFunctions,omitempty"`
		Count int   `json:"_count,omitempty"`
	}
	_, err = c.client.Query(ctx, coll+"udfs/", query, &data)
	if udfs = data.Udfs; err != nil {
		udfs = nil
	}
	return
}

// Read all documents in a collection that satisfy a query
func (c *DocumentDB) QueryDocuments(ctx context.Context, coll string, query *Query, docs interface{}) (token string, err error) {
	data := struct {
		Documents interface{} `json:"Documents,omitempty"`
		Count     int         `json:"_count,omitempty"`
	}{Documents: docs}
	return c.client.Query(ctx, coll+"docs/", query, &data)
}

// Create database
func (c *DocumentDB) CreateDatabase(ctx context.Context, body interface{}) (db *Database, err error) {
	err = c.client.Create(ctx, "dbs", body, &db, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Create collection
func (c *DocumentDB) CreateCollection(ctx context.Context, db string, body interface{}) (coll *Collection, err error) {
	err = c.client.Create(ctx, db+"colls/", body, &coll, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Create stored procedure
func (c *DocumentDB) CreateStoredProcedure(ctx context.Context, coll string, sproc *Sproc) error {
	return c.client.Create(ctx, coll+"sprocs/", sproc, sproc, nil)
}

// Create user defined function
func (c *DocumentDB) CreateUserDefinedFunction(ctx context.Context, coll string, body interface{}) (udf *UDF, err error) {
	err = c.client.Create(ctx, coll+"udfs/", body, &udf, nil)
	if err != nil {
		return nil, err
	}
	return
}

func (c *DocumentDB) createDocument(ctx context.Context, coll string, doc interface{}, headers map[string]string) (*Document, error) {
	rv := reflect.ValueOf(doc)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if id := rv.FieldByName("Id"); id.IsValid() && id.String() == "" {
		id.SetString(uuid())
	}
	var document Document
	if err := c.client.Create(ctx, coll+"docs/", doc, &document, headers); err != nil {
		return nil, err
	}
	return &document, nil
}

// Create document
func (c *DocumentDB) CreateDocument(ctx context.Context, coll string, doc interface{}) (*Document, error) {
	return c.createDocument(ctx, coll, doc, nil)
}

func (c *DocumentDB) UpdateDocument(ctx context.Context, coll string, doc interface{}, etag string) (*Document, error) {
	rv := reflect.ValueOf(doc)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	id := rv.FieldByName("Id")
	if !id.IsValid() || id.String() == "" {
		id = rv.FieldByName("ID")
		if !id.IsValid() || id.String() == "" {
			return nil, errors.New("document doesn't have id")
		}
	}

	var docs []Document
	_, err := c.QueryDocuments(ctx, coll, IdQuery(id.String()), &docs)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, ErrNotFound
	}

	headers := make(map[string]string)
	if etag != "" {
		headers[HEADER_IF_MATCH] = etag
	}
	return c.ReplaceDocument(ctx, docs[0].Self, doc, headers)
}

// Create document
func (c *DocumentDB) UpsertDocument(ctx context.Context, coll string, doc interface{}, etag string) (*Document, error) {
	headers := map[string]string{
		HEADER_UPSERT: "true",
	}
	if etag != "" {
		headers[HEADER_IF_MATCH] = etag
	}
	return c.createDocument(ctx, coll, doc, headers)
}

// TODO: DRY, but the sdk want that[mm.. maybe just client.Delete(self_link)]
// Delete database
func (c *DocumentDB) DeleteDatabase(ctx context.Context, link string) error {
	return c.client.Delete(ctx, link, nil)
}

// Delete collection
func (c *DocumentDB) DeleteCollection(ctx context.Context, link string) error {
	return c.client.Delete(ctx, link, nil)
}

// Delete collection
func (c *DocumentDB) DeleteDocument(ctx context.Context, link string, etag string) error {
	headers := make(map[string]string, 0)
	if etag != "" {
		headers[HEADER_IF_MATCH] = etag
	}
	return c.client.Delete(ctx, link, headers)
}

// Delete stored procedure
func (c *DocumentDB) DeleteStoredProcedure(ctx context.Context, link string) error {
	return c.client.Delete(ctx, link, nil)
}

// Delete user defined function
func (c *DocumentDB) DeleteUserDefinedFunction(ctx context.Context, link string) error {
	return c.client.Delete(ctx, link, nil)
}

// Replace database
func (c *DocumentDB) ReplaceDatabase(ctx context.Context, link string, body interface{}) (db *Database, err error) {
	err = c.client.Replace(ctx, link, body, &db, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Replace document
func (c *DocumentDB) ReplaceDocument(ctx context.Context, link string, doc interface{}, headers map[string]string) (*Document, error) {
	var document Document
	if err := c.client.Replace(ctx, link, doc, &doc, headers); err != nil {
		return nil, err
	}
	return &document, nil
}

// Replace stored procedure
func (c *DocumentDB) ReplaceStoredProcedure(ctx context.Context, link string, body interface{}) (sproc *Sproc, err error) {
	err = c.client.Replace(ctx, link, body, &sproc, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Replace stored procedure
func (c *DocumentDB) ReplaceUserDefinedFunction(ctx context.Context, link string, body interface{}) (udf *UDF, err error) {
	err = c.client.Replace(ctx, link, body, &udf, nil)
	if err != nil {
		return nil, err
	}
	return
}

// Execute stored procedure
func (c *DocumentDB) ExecuteStoredProcedure(ctx context.Context, link string, params, body interface{}) (err error) {
	err = c.client.Execute(ctx, link, params, body)
	return
}

package documentdb

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type ClientStub struct {
	mock.Mock
}

func (c *ClientStub) Query(ctx context.Context, link string, query *Query, ret interface{}) (string, error) {
	c.Called(link, query)
	return "", nil
}

func (c *ClientStub) Create(ctx context.Context, link string, body, ret interface{}, headers map[string]string) error {
	c.Called(link, body)
	return nil
}

func (c *ClientStub) Delete(ctx context.Context, link string, headers map[string] string) error {
	c.Called(link)
	return nil
}

func (c *ClientStub) Replace(ctx context.Context, link string, body, ret interface{}) error {
	c.Called(link, body)
	return nil
}

func (c *ClientStub) Execute(ctx context.Context, link string, body, ret interface{}) error {
	c.Called(link, body)
	return nil
}

func TestNew(t *testing.T) {
	assert := assert.New(t)
	client := New("url", Config{"config"})
	assert.IsType(client, &DocumentDB{}, "Should return DocumentDB object")
}

// TODO: Test failure
func TestReadDatabase(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "self_link", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadDatabase(ctx, "self_link")
	client.AssertCalled(t, "Query", "self_link", (*Query)(nil))
}

func TestReadCollection(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "self_link", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadCollection(ctx, "self_link")
	client.AssertCalled(t, "Query", "self_link", (*Query)(nil))
}

func TestReadDocument(t *testing.T) {
	type MyDocument struct {
		Document
		// Your external fields
		Name    string `json:"name,omitempty"`
		Email   string `json:"email,omitempty"`
		IsAdmin bool   `json:"isAdmin,omitempty"`
	}
	var doc MyDocument
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "self_link_doc", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadDocument(ctx, "self_link_doc", &doc)
	client.AssertCalled(t, "Query", "self_link_doc", (*Query)(nil))
}

func TestReadStoredProcedure(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "self_link", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadStoredProcedure(ctx, "self_link")
	client.AssertCalled(t, "Query", "self_link", (*Query)(nil))
}

func TestReadUserDefinedFunction(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "self_link", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadUserDefinedFunction(ctx, "self_link")
	client.AssertCalled(t, "Query", "self_link", (*Query)(nil))
}

func TestReadDatabases(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "dbs", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadDatabases(ctx)
	client.AssertCalled(t, "Query", "dbs", (*Query)(nil))
}

func TestReadCollections(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	dbLink := "dblink/"
	client.On("Query", dbLink+"colls/", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadCollections(ctx, dbLink)
	client.AssertCalled(t, "Query", dbLink+"colls/", (*Query)(nil))
}

func TestReadStoredProcedures(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	collLink := "colllink/"
	client.On("Query", collLink+"sprocs/", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadStoredProcedures(ctx, collLink)
	client.AssertCalled(t, "Query", collLink+"sprocs/", (*Query)(nil))
}

func TestReadUserDefinedFunctions(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	collLink := "colllink/"
	client.On("Query", collLink+"udfs/", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadUserDefinedFunctions(ctx, collLink)
	client.AssertCalled(t, "Query", collLink+"udfs/", (*Query)(nil))
}

func TestReadDocuments(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	collLink := "colllink/"
	client.On("Query", collLink+"docs/", (*Query)(nil)).Return("", nil)
	ctx := context.Background()
	c.ReadDocuments(ctx, collLink, "", struct{}{})
	client.AssertCalled(t, "Query", collLink+"docs/", (*Query)(nil))
}

func TestQueryDatabases(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "dbs", NewQuery("SELECT * FROM ROOT r", nil)).Return(nil)
	ctx := context.Background()
	c.QueryDatabases(ctx, NewQuery("SELECT * FROM ROOT r", nil))
	client.AssertCalled(t, "Query", "dbs", NewQuery("SELECT * FROM ROOT r", nil))
}

func TestQueryCollections(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "db_self_link/colls/", &Query{Text: "SELECT * FROM ROOT r"}).Return(nil)
	ctx := context.Background()
	c.QueryCollections(ctx, "db_self_link/", &Query{Text: "SELECT * FROM ROOT r"})
	client.AssertCalled(t, "Query", "db_self_link/colls/", &Query{Text: "SELECT * FROM ROOT r"})
}

func TestQueryStoredProcedures(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "colls_self_link/sprocs/", &Query{Text: "SELECT * FROM ROOT r"}).Return(nil)
	ctx := context.Background()
	c.QueryStoredProcedures(ctx, "colls_self_link/", &Query{Text: "SELECT * FROM ROOT r"})
	client.AssertCalled(t, "Query", "colls_self_link/sprocs/", &Query{Text: "SELECT * FROM ROOT r"})
}

func TestQueryUserDefinedFunctions(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Query", "colls_self_link/udfs/", &Query{Text: "SELECT * FROM ROOT r"}).Return(nil)
	ctx := context.Background()
	c.QueryUserDefinedFunctions(ctx, "colls_self_link/", &Query{Text: "SELECT * FROM ROOT r"})
	client.AssertCalled(t, "Query", "colls_self_link/udfs/", &Query{Text: "SELECT * FROM ROOT r"})
}

func TestQueryDocuments(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	collLink := "coll_self_link/"
	client.On("Query", collLink+"docs/", &Query{Text: "SELECT * FROM ROOT r"}).Return(nil)
	ctx := context.Background()
	c.QueryDocuments(ctx, collLink, &Query{Text: "SELECT * FROM ROOT r"}, struct{}{})
	client.AssertCalled(t, "Query", collLink+"docs/", &Query{Text: "SELECT * FROM ROOT r"})
}

func TestCreateDatabase(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Create", "dbs", "{}").Return(nil)
	ctx := context.Background()
	c.CreateDatabase(ctx, "{}")
	client.AssertCalled(t, "Create", "dbs", "{}")
}

func TestCreateCollection(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Create", "dbs/colls/", "{}").Return(nil)
	ctx := context.Background()
	c.CreateCollection(ctx, "dbs/", "{}")
	client.AssertCalled(t, "Create", "dbs/colls/", "{}")
}

func TestCreateStoredProcedure(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Create", "dbs/colls/sprocs/", &Sproc{Body: `{"id":"fn"}`}).Return(nil)
	ctx := context.Background()
	c.CreateStoredProcedure(ctx, "dbs/colls/", &Sproc{Body: `{"id":"fn"}`})
	client.AssertCalled(t, "Create", "dbs/colls/sprocs/", &Sproc{Body: `{"id":"fn"}`})
}

func TestCreateUserDefinedFunction(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Create", "dbs/colls/udfs/", `{"id":"fn"}`).Return(nil)
	ctx := context.Background()
	c.CreateUserDefinedFunction(ctx, "dbs/colls/", `{"id":"fn"}`)
	client.AssertCalled(t, "Create", "dbs/colls/udfs/", `{"id":"fn"}`)
}

func TestCreateDocument(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	// TODO: test error situation, without id, etc...
	var doc Document
	client.On("Create", "dbs/colls/docs/", &doc).Return(nil)
	ctx := context.Background()
	c.CreateDocument(ctx, "dbs/colls/", &doc)
	client.AssertCalled(t, "Create", "dbs/colls/docs/", &doc)
	assert.NotEqual(t, doc.Id, "")
}

func TestDeleteResource(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}

	client.On("Delete", "self_link_db").Return(nil)
	ctx := context.Background()
	c.DeleteDatabase(ctx, "self_link_db")
	client.AssertCalled(t, "Delete", "self_link_db")

	client.On("Delete", "self_link_coll").Return(nil)
	c.DeleteCollection(ctx, "self_link_coll")
	client.AssertCalled(t, "Delete", "self_link_coll")

	client.On("Delete", "self_link_doc").Return(nil)
	c.DeleteDocument(ctx, "self_link_doc", "")
	client.AssertCalled(t, "Delete", "self_link_doc")

	client.On("Delete", "self_link_sproc").Return(nil)
	c.DeleteDocument(ctx, "self_link_sproc", "")
	client.AssertCalled(t, "Delete", "self_link_sproc")

	client.On("Delete", "self_link_udf").Return(nil)
	c.DeleteDocument(ctx, "self_link_udf", "")
	client.AssertCalled(t, "Delete", "self_link_udf")
}

func TestReplaceDatabase(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Replace", "db_link", "{}").Return(nil)
	ctx := context.Background()
	c.ReplaceDatabase(ctx, "db_link", "{}")
	client.AssertCalled(t, "Replace", "db_link", "{}")
}

func TestReplaceDocument(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Replace", "doc_link", "{}").Return(nil)
	ctx := context.Background()
	c.ReplaceDocument(ctx, "doc_link", "{}")
	client.AssertCalled(t, "Replace", "doc_link", "{}")
}

func TestReplaceStoredProcedure(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Replace", "sproc_link", "{}").Return(nil)
	ctx := context.Background()
	c.ReplaceStoredProcedure(ctx, "sproc_link", "{}")
	client.AssertCalled(t, "Replace", "sproc_link", "{}")
}

func TestReplaceUserDefinedFunction(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Replace", "udf_link", "{}").Return(nil)
	ctx := context.Background()
	c.ReplaceUserDefinedFunction(ctx, "udf_link", "{}")
	client.AssertCalled(t, "Replace", "udf_link", "{}")
}

func TestExecuteStoredProcedure(t *testing.T) {
	client := &ClientStub{}
	c := &DocumentDB{client}
	client.On("Execute", "sproc_link", "{}").Return(nil)
	ctx := context.Background()
	c.ExecuteStoredProcedure(ctx, "sproc_link", "{}", struct{}{})
	client.AssertCalled(t, "Execute", "sproc_link", "{}")
}

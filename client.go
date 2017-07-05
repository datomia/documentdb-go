package documentdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
)

var DebugQueries = false

var (
	ErrPreconditionFailed = errors.New("precondition failed")
)

type QueryParam struct {
	Name  string      `json:"name"` // should contain a @ character
	Value interface{} `json:"value"`
}

type Query struct {
	Text   string       `json:"query"`
	Params []QueryParam `json:"parameters,omitempty"`
	Token  string       `json:"-"` // continuation token
}

// NewQuery create a query with given parameters.
//
// Example:
//	NewQuery(
// 		`SELECT * FROM root r WHERE (r.id = @id)`,
//		map[string]interface{}{"@id": "foo"},
//	)
func NewQuery(qu string, params map[string]interface{}) *Query {
	q := &Query{Text: qu}
	q.Params = make([]QueryParam, 0, len(params))
	for name, val := range params {
		q.Params = append(q.Params, QueryParam{Name: name, Value: val})
	}
	return q
}

type Clienter interface {
	Delete(link string) error
	Query(link string, qu *Query, ret interface{}) (token string, err error)
	Create(link string, body, ret interface{}, headers map[string]string) error
	Replace(link string, body, ret interface{}) error
	Execute(link string, body, ret interface{}) error
}

type Client struct {
	Url    string
	Config Config
	Client *http.Client
}

// Delete resource by self link
func (c *Client) Delete(link string) error {
	_, err := c.method("DELETE", link, nil, &bytes.Buffer{}, nil)
	return err
}

// Query resource or read it by self link.
func (c *Client) Query(link string, query *Query, out interface{}) (string, error) {
	var (
		method = "GET"
		r      io.Reader
		n      int
	)
	if query != nil && query.Text != "" {
		data, err := json.Marshal(query)
		if err != nil {
			return "", err
		}
		n = len(data)
		r = bytes.NewReader(data)
		method = "POST"
	}
	hr, err := http.NewRequest(method, path(c.Url, link), r)
	if err != nil {
		return "", err
	}
	req := ResourceRequest(link, hr)
	if err = req.DefaultHeaders(c.Config.MasterKey); err != nil {
		return "", err
	}
	tok := ""
	if query != nil {
		tok = query.Token
	}
	req.QueryHeaders(n, tok)
	resp, err := c.do(req, out)
	if err != nil {
		if DebugQueries {
			log.Println("docdb:", query.Text, query.Params, "cost:", resp.Header.Get(HEADER_CHARGE), "RU", "error:", err)
		}
		return "", err
	}
	if DebugQueries {
		log.Println("docdb:", query.Text, query.Params, "cost:", resp.Header.Get(HEADER_CHARGE), "RU")
	}
	return resp.Header.Get(HEADER_CONTINUATION), err
}

// Create resource
func (c *Client) Create(link string, body, ret interface{}, headers map[string]string) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method("POST", link, ret, buf, headers)
	return err
}

// Replace resource
func (c *Client) Replace(link string, body, ret interface{}) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method("PUT", link, ret, buf, nil)
	return err
}

// Replace resource
// TODO: DRY, move to methods instead of actions(POST, PUT, ...)
func (c *Client) Execute(link string, body, ret interface{}) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method("POST", link, ret, buf, nil)
	return err
}

// Private generic method resource
func (c *Client) method(method, link string, ret interface{}, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequest(method, path(c.Url, link), body)
	if err != nil {
		return nil, err
	}
	r := ResourceRequest(link, req)
	for k, v := range headers {
		r.Header.Add(k, v)
	}
	if err = r.DefaultHeaders(c.Config.MasterKey); err != nil {
		return nil, err
	}
	return c.do(r, ret)
}

// Private Do function, DRY
func (c *Client) do(r *Request, data interface{}) (*http.Response, error) {
	cli := c.Client
	if cli == nil {
		cli = http.DefaultClient
	}
	resp, err := cli.Do(r.Request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 412 {
		return nil, ErrPreconditionFailed
	}
	if resp.StatusCode >= 300 {
		err = &RequestError{}
		readJson(resp.Body, &err)
		return resp, err
	}
	if data == nil {
		return resp, nil
	}
	return resp, readJson(resp.Body, data)
}

// Generate link
func path(url string, args ...string) (link string) {
	args = append([]string{url}, args...)
	link = strings.Join(args, "/")
	return
}

// Read json response to given interface(struct, map, ..)
func readJson(reader io.Reader, data interface{}) error {
	return json.NewDecoder(reader).Decode(data)
}

// Stringify body data
func stringify(body interface{}) (bt []byte, err error) {
	switch t := body.(type) {
	case string:
		bt = []byte(t)
	case []byte:
		bt = t
	default:
		bt, err = json.Marshal(t)
	}
	return
}

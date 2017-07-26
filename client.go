package documentdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

var (
	ResponseHook func(ctx context.Context, method string, headers map[string][]string)
)

type queryKey struct{}
type sprocKey struct{}
type collKey struct{}

func CtxQuery(ctx context.Context) *Query {
	q, _ := ctx.Value(queryKey{}).(*Query)
	return q
}

func CtxSproc(ctx context.Context) string {
	s, _ := ctx.Value(sprocKey{}).(string)
	return s
}

func CtxCollection(ctx context.Context) string {
	s, _ := ctx.Value(collKey{}).(string)
	return s
}

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
	Delete(ctx context.Context, link string) error
	Query(ctx context.Context, link string, qu *Query, ret interface{}) (token string, err error)
	Create(ctx context.Context, link string, body, ret interface{}, headers map[string]string) error
	Replace(ctx context.Context, link string, body, ret interface{}) error
	Execute(ctx context.Context, link string, body, ret interface{}) error
}

type Client struct {
	Url    string
	Config Config
	Client *http.Client
}

// Delete resource by self link
func (c *Client) Delete(ctx context.Context, link string) error {
	_, err := c.method(ctx, "DELETE", link, nil, &bytes.Buffer{}, nil)
	return err
}

// Query resource or read it by self link.
func (c *Client) Query(ctx context.Context, link string, query *Query, out interface{}) (string, error) {
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
	hr = hr.WithContext(context.WithValue(ctx, queryKey{}, query))
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
		return "", err
	}
	return resp.Header.Get(HEADER_CONTINUATION), err
}

// Create resource
func (c *Client) Create(ctx context.Context, link string, body, ret interface{}, headers map[string]string) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method(ctx, "POST", link, ret, buf, headers)
	return err
}

// Replace resource
func (c *Client) Replace(ctx context.Context, link string, body, ret interface{}) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method(ctx, "PUT", link, ret, buf, nil)
	return err
}

// Replace resource
// TODO: DRY, move to methods instead of actions(POST, PUT, ...)
func (c *Client) Execute(ctx context.Context, link string, body, ret interface{}) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method(ctx, "POST", link, ret, buf, nil)
	return err
}

// Private generic method resource
func (c *Client) method(ctx context.Context, method, link string, ret interface{}, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequest(method, path(c.Url, link), body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
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
	if ResponseHook != nil {
		ResponseHook(r.Context(), r.Request.Method, resp.Header)
	}
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

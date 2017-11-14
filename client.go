package documentdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var (
	ResponseHook  func(ctx context.Context, method string, headers map[string][]string)
	IgnoreContext bool
	errRetry      = errors.New("retry")
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
	Delete(ctx context.Context, link string, headers map[string]string) error
	Query(ctx context.Context, link string, qu *Query, ret interface{}) (token string, err error)
	Create(ctx context.Context, link string, body, ret interface{}, headers map[string]string) error
	Replace(ctx context.Context, link string, body, ret interface{}, headers map[string]string) error
	Execute(ctx context.Context, link string, body, ret interface{}) error
}

type Client struct {
	Url    string
	Config Config
	Client *http.Client
}

// Delete resource by self link
func (c *Client) Delete(ctx context.Context, link string, headers map[string]string) error {
	_, err := c.method(ctx, "DELETE", link, nil, &bytes.Buffer{}, headers)
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
	ctx = context.WithValue(ctx, queryKey{}, query)
	req := ResourceRequest(link, hr)
	if err = req.DefaultHeaders(c.Config.MasterKey); err != nil {
		return "", err
	}
	tok := ""
	if query != nil {
		tok = query.Token
	}
	req.QueryHeaders(n, tok)
	resp, err := c.do(ctx, req, out)
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
func (c *Client) Replace(ctx context.Context, link string, body, ret interface{}, headers map[string]string) error {
	data, err := stringify(body)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	_, err = c.method(ctx, "PUT", link, ret, buf, headers)
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
	r := ResourceRequest(link, req)
	for k, v := range headers {
		r.Header.Add(k, v)
	}
	if err = r.DefaultHeaders(c.Config.MasterKey); err != nil {
		return nil, err
	}
	return c.do(ctx, r, ret)
}

func retriable(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable
}

func (c *Client) checkResponse(ctx context.Context, r *Request, resp *http.Response) error {
	if retriable(resp.StatusCode) {
		r.RetryCount++
		if r.RetryCount <= c.Config.MaxRetries {
			delay := backoffDelay(r.RetryCount)
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
				return errRetry
			}
		}
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		return ErrPreconditionFailed
	}
	if resp.StatusCode >= 300 {
		err := &RequestError{}
		readJson(resp.Body, &err)
		return err
	}

	return nil
}

// Private Do function, DRY
func (c *Client) do(ctx context.Context, r *Request, data interface{}) (*http.Response, error) {
	cli := c.Client
	if cli == nil {
		cli = http.DefaultClient
	}
	if !IgnoreContext {
		r.Request = r.Request.WithContext(ctx)
	}
	// save body to be able to retry the request
	b, err := ioutil.ReadAll(r.Request.Body)
	if err != nil {
		return nil, err
	}
	for {
		r.Request.Body = ioutil.NopCloser(bytes.NewReader(b))
		resp, err := cli.Do(r.Request)
		if err != nil {
			return nil, err
		}
		if ResponseHook != nil {
			ResponseHook(ctx, r.Request.Method, resp.Header)
		}
		if err := c.checkResponse(ctx, r, resp); err == errRetry {
			resp.Body.Close()
			continue
		} else if err != nil {
			resp.Body.Close()
			return resp, err
		}
		defer resp.Body.Close()

		if data == nil {
			return resp, nil
		}
		return resp, readJson(resp.Body, data)
	}
}

func backoffDelay(retryCount int) time.Duration {
	minTime := 300

	if retryCount > 13 {
		retryCount = 13
	} else if retryCount > 8 {
		retryCount = 8
	}

	delay := (1 << uint(retryCount)) * (rand.Intn(minTime) + minTime)
	return time.Duration(delay) * time.Millisecond
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

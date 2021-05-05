package httpio

import (
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
)

func TestHttpio(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Httpio Suite")
}

type httpMock struct {
	mutex    *sync.Mutex
	expected []*request
	server   *ghttp.Server
}

type request struct {
	url    *url.URL
	header http.Header
	method string

	responseCode    int
	responseBody    []byte
	responseHeaders map[string][]string
}

func newHTTPMock(s *ghttp.Server) *httpMock {
	return &httpMock{
		mutex:    new(sync.Mutex),
		expected: []*request{},
		server:   s,
	}
}

func (h *httpMock) finish() {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if len(h.expected) > 0 {
	    Fail("unmatched expectations")
    }
	h.expected = []*request{}
}

func (h *httpMock) expect(method string, expectUrl *url.URL, header http.Header) *httpMock {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.expected = append(h.expected, &request{
		url:             expectUrl,
		method:          method,
		header:          header,
		responseHeaders: make(map[string][]string),
	})
	return h
}

func (h *httpMock) response(statusCode int, body []byte, headers map[string][]string) *httpMock {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	req := h.expected[len(h.expected)-1]

	for k, v := range headers {
		req.responseHeaders[k] = v
	}

	req.responseBody = body
	req.responseCode = statusCode

	h.server.AppendHandlers(h.ServeHTTP)
	return h
}

func (h *httpMock) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if len(h.expected) == 0 {
		Fail("no expected requests")
	}

	// We don't care about the UA
	req.Header.Del("User-Agent")
	var (
		matched bool
		idx     int
	)
	for i, r := range h.expected {
		if req.Method == r.method && req.URL.String() == r.url.Path && reflect.DeepEqual(req.Header, r.header) {
			for k, v := range r.responseHeaders {
				for _, s := range v {
					w.Header().Set(k, s)
				}
			}

			if r.responseCode != 0 {
				w.WriteHeader(r.responseCode)
			}

			w.Write(r.responseBody)
			matched = true
			idx = i
			break
		}
	}

	if matched {
		h.expected = append(h.expected[:idx], h.expected[idx+1:]...)
		return
	}
	Fail("request did not match any expected request")
}

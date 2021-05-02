package httpio

import (
	"net/http"
	"net/url"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestHttpio(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Httpio Suite")
}

type handler struct {
	url    *url.URL
	header http.Header
	method string

	responseCode    int
	responseBody    []byte
	responseHeaders map[string][]string
}

func newMockHandler() *handler {
	return &handler{responseHeaders: make(map[string][]string)}
}

func (h *handler) expect(method string, expectUrl *url.URL, header http.Header) {
	h.url = expectUrl
	h.header = header
	h.method = method
}

func (h *handler) response(statusCode int, body []byte, headers map[string][]string) {
	for k, v := range headers {
		h.responseHeaders[k] = v
	}

	h.responseBody = body
	h.responseCode = statusCode
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	Expect(req.Method).To(Equal(h.method))
	Expect(req.URL.String()).To(Equal(h.url.Path))

	// Don't care about the UA header.
	req.Header.Del("User-Agent")
	Expect(req.Header).To(Equal(h.header))

	for k, v := range h.responseHeaders {
		for _, s := range v {
			w.Header().Set(k, s)
		}
	}

	if h.responseCode != 0 {
		w.WriteHeader(h.responseCode)
	}

	w.Write(h.responseBody)
}

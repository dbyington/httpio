package httpio

import (
	"context"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
)

var _ = Describe("io", func() {
	var (
		server      *ghttp.Server
		mockHandler *handler
	)

	AfterSuite(func() {
		server.Close()
	})

	Context("Options", func() {
		var (
			options *Options

			testOptions *Options
		)

		BeforeEach(func() {
			server = ghttp.NewServer()
			mockHandler = newMockHandler()
			server.AppendHandlers(ghttp.CombineHandlers(mockHandler.ServeHTTP))
		})

		Context(".headURL", func() {
			var (
				expectLen int64
				expectUrl *url.URL
				etag      string
				err       error
				l         int64
			)

			JustBeforeEach(func() {
				l, etag, err = options.headURL(nil)
			})

			BeforeEach(func() {
				options = &Options{client: &http.Client{}}
				expectUrl, _ = url.Parse(server.URL() + "/foo")
			})

			Context("when the server does not support range reads", func() {
				BeforeEach(func() {
					options.url = expectUrl.String()
					mockHandler.expect(http.MethodHead, expectUrl, http.Header{})
					mockHandler.response(http.StatusBadRequest, nil, nil)
				})

				It("should return the error", func() {
					Ω(err).To(MatchError("range reads not supported"))
				})

				It("should return a zero length", func() {
					Ω(l).To(BeZero())
				})

				It("should return an empty etag", func() {
					Ω(etag).To(BeEmpty())
				})
			})

			Context("when the server supports range reads", func() {
				BeforeEach(func() {
					expectLen = 42
					expectLenString := fmt.Sprintf("%d", expectLen)
					options.url = expectUrl.String()
					mockHandler.expect(http.MethodHead, expectUrl, http.Header{})
					h := map[string][]string{
						"accept-ranges":  {"bytes"},
						"content-length": {expectLenString},
					}
					mockHandler.response(http.StatusBadRequest, nil, h)
				})

				It("should not error", func() {
					Ω(err).ToNot(HaveOccurred())
				})

				It("should return the content length", func() {
					Ω(l).To(Equal(expectLen))
				})
			})

		})

		Context(".WithClient", func() {
			var (
				c *http.Client
				o Option
			)

			JustBeforeEach(func() {
				o = WithClient(c)
				o(testOptions)
			})

			Context("with a nil client", func() {
				BeforeEach(func() {
					c = nil
					testOptions = &Options{
						client: new(http.Client),
					}
				})

				It("should set the client to nil", func() {
					Expect(testOptions.client).To(BeNil())
				})
			})

			Context("with a client instance", func() {
				BeforeEach(func() {
					c = &http.Client{}
					testOptions = &Options{}
				})

				It("should set the option client", func() {
					Expect(testOptions.client).To(Equal(c))
				})
			})
		})

		Context(".WithURL", func() {
			var (
				u string
				o Option
			)

			JustBeforeEach(func() {
				o = WithURL(u)
				o(testOptions)
			})

			Context("with a url string", func() {
				BeforeEach(func() {
					u = "somestring"
					testOptions = &Options{}
				})

				It("should set the option client", func() {
					Expect(testOptions.url).To(Equal(u))
				})
			})
		})

		Context(".validateUrl", func() {
			var (
				u   string
				err error
			)

			JustBeforeEach(func() {
				err = testOptions.validateUrl()
			})

			Context("with a no scheme", func() {
				BeforeEach(func() {
					u = "google.com/q"
					testOptions = &Options{url: u}
				})

				It("should return an error", func() {
					Expect(err).To(MatchError(ErrInvalidURLScheme))
				})
			})

			Context("with a no host", func() {
				BeforeEach(func() {
					u = "http:///foo"
					testOptions = &Options{url: u}
				})

				It("should return an error", func() {
					Expect(err).To(MatchError(ErrInvalidURLHost))
				})
			})

			Context("with a valid url string", func() {
				BeforeEach(func() {
					u = "https://google.com/q"
					testOptions = &Options{url: u}
				})

				It("should not return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("should leave the url", func() {
					Expect(testOptions.url).To(Equal(u))
				})
			})
		})

		Context(".hashURL", func() {
			var (
				hashScheme uint
				hashResult hash.Hash
				err        error

				expectUrl *url.URL
			)

			JustBeforeEach(func() {
				hashResult, err = options.hashURL(hashScheme)
			})

			BeforeEach(func() {
				expectUrl, _ = url.Parse(server.URL() + "/foo")
				options = &Options{
					client: &http.Client{},
					url:    expectUrl.String(),
				}
			})

			Context("with a client request error", func() {
				var expectErr string

				BeforeEach(func() {
					mockHandler.expect(http.MethodGet, expectUrl, http.Header{"Accept-Encoding": []string{"gzip"}})
					mockHandler.response(http.StatusBadRequest, nil, nil)
					expectErr = fmt.Sprintf("Error requesting %s, received code: 400 Bad Request", expectUrl.String())
				})

				It("should return an error", func() {
					Expect(err).To(MatchError(expectErr))
				})

				It("should return a nil interface", func() {
					Expect(hashResult).To(BeNil())
				})
			})
		})
	})

	Context("ReadAtCloser", func() {
		var (
			options      *Options
			readAtCloser *ReadAtCloser
		)

		BeforeEach(func() {
			mockHandler = newMockHandler()
			server = ghttp.NewServer()
			server.AppendHandlers(ghttp.CombineHandlers(mockHandler.ServeHTTP))

			options = &Options{
				client: &http.Client{},
			}
			readAtCloser = &ReadAtCloser{options: options}
		})

		Context(".Close", func() {
			var (
				ctx context.Context
				err error
			)

			JustBeforeEach(func() {
				err = readAtCloser.Close()
			})

			Context("with cancel func set", func() {
				BeforeEach(func() {
					ctx, readAtCloser.cancel = context.WithCancel(context.Background())
				})

				It("should cancel the context", func() {
					Ω(ctx.Err()).To(MatchError("context canceled"))
				})

				It("should not return an error", func() {
					Ω(err).ToNot(HaveOccurred())
				})
			})
		})

		Context(".ReadAt", func() {
			var (
				readLen   int
				err       error
				expectUrl *url.URL

				target []byte
				start  int64

				fullBody = []byte("This is the full body of my message")
			)

			JustBeforeEach(func() {
				readLen, err = readAtCloser.ReadAt(target, start)
			})

			BeforeEach(func() {
				readAtCloser.contentLength = int64(len(fullBody))
				expectUrl, _ = url.Parse(server.URL() + "/foo")
				readAtCloser.options.url = expectUrl.String()
			})

			Context("when the read will overrun the content", func() {
				BeforeEach(func() {
					readLength := len(fullBody)
					target = make([]byte, readLength)
					start = 5
					mockHandler.expect(http.MethodGet, expectUrl, rangeHead(int(start), int(start)+readLength))
					mockHandler.response(http.StatusPartialContent, fullBody, nil)
				})

				It("should return an error", func() {
					Expect(err).To(MatchError(io.ErrUnexpectedEOF))
				})

				It("should return a length", func() {
					Expect(readLen).To(Equal(len(fullBody) - int(start)))
				})
			})

			Context("when the request receives an error", func() {
				var readSize = 5

				BeforeEach(func() {
					start = 0
					target = make([]byte, readSize)
					mockHandler.expect(http.MethodGet, expectUrl, rangeHead(int(start), readSize))
					mockHandler.response(http.StatusBadRequest, nil, nil)
				})

				It("should return an error", func() {
					Expect(err).To(MatchError(ErrRangeReadNotSatisfied))
				})

				It("should return a zero length", func() {
					Expect(readLen).To(BeZero())
				})
			})

			Context("when the read succeeds", func() {
				var readSize = 5

				BeforeEach(func() {
					start = 5
					target = make([]byte, readSize)
					mockHandler.expect(http.MethodGet, expectUrl, rangeHead(int(start), int(start)+readSize))
					mockHandler.response(http.StatusPartialContent, fullBody[start:start+int64(readSize)], nil)
				})

				It("should return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("should return a zero length", func() {
					Expect(readLen).To(Equal(readSize))
				})
			})
		})
	})
})

func rangeHead(start, end int) map[string][]string {
	r := fmt.Sprintf("bytes=%d-%d", start, end)
	return map[string][]string{
		"Range": {r},
	}
}

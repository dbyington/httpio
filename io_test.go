package httpio

import (
    "context"
    "fmt"
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
        var options *Options

        BeforeEach(func() {
            server = ghttp.NewServer()
            mockHandler = newMockHandler()
            server.AppendHandlers(ghttp.CombineHandlers(mockHandler.ServeHTTP))
            options = &Options{client: &http.Client{}}
        })

        Context(".headURL", func() {
            var (
                expectLen int64
                expectUrl *url.URL
                err error
                len int64
            )

            JustBeforeEach(func() {
                len, err = options.headURL()
            })

            BeforeEach(func() {
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
                    Ω(len).To(BeZero())
                })
            })

            Context("when the server supports range reads", func() {
                BeforeEach(func() {
                    expectLen = 42
                    expectLenString := fmt.Sprintf("%d", expectLen)
                    options.url = expectUrl.String()
                    mockHandler.expect(http.MethodHead, expectUrl, http.Header{})
                    h := map[string][]string{
                        "accept-ranges": {"bytes"},
                        "content-length": {expectLenString},
                    }
                    mockHandler.response(http.StatusBadRequest, nil, h)
                })

                It("should not error", func() {
                    Ω(err).ToNot(HaveOccurred())
                })

                It("should return the content length", func() {
                    Ω(len).To(Equal(expectLen))
                })
            })

        })
    })

	Context("ReadAtCloser", func() {
		var (
		    options *Options
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
                readLen int
                err error
                expectUrl *url.URL

                target []byte
                start int64

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
                })

                It("should return an error", func() {
                    Expect(err).To(MatchError(ErrReadFromSource))
                })

                It("should return a zero length", func() {
                    Expect(readLen).To(BeZero())
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

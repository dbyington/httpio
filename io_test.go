package httpio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
	"github.com/sirupsen/logrus"
)

const testDataDir = `./testdata`

var _ = Describe("io", func() {
	var (
		server   *ghttp.Server
		mockHTTP *httpMock
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
			mockHTTP = newHTTPMock(server)
		})

		AfterEach(func() {
			mockHTTP.finish()
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
				options = &Options{
					client: &http.Client{},
					logger: newNoopLogger(),
				}
				expectUrl, _ = url.Parse(server.URL() + "/foo")
			})

			Context("when the server does not support range reads", func() {
				BeforeEach(func() {
					options.url = expectUrl.String()
					mockHTTP.expect(http.MethodHead, expectUrl, http.Header{}).
						response(http.StatusOK, nil, nil)

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

			Context("when the file is not found", func() {
				BeforeEach(func() {
					options.url = expectUrl.String()
					mockHTTP.expect(http.MethodHead, expectUrl, http.Header{}).
						response(http.StatusNotFound, nil, nil)

				})

				It("should return the error", func() {
					Ω(err).To(Equal(RequestError{StatusCode: http.StatusNotFound, Url: expectUrl.String()}))
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
					mockHTTP.expect(http.MethodHead, expectUrl, http.Header{})
					h := map[string][]string{
						"accept-ranges":  {"bytes"},
						"content-length": {expectLenString},
					}
					mockHTTP.response(http.StatusOK, nil, h)
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
						logger: newNoopLogger(),
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
					testOptions = &Options{logger: newNoopLogger()}
				})

				It("should set the option client", func() {
					Expect(testOptions.url).To(Equal(u))
				})
			})
		})

		Context(".WithContext", func() {
			var (
				inCTX context.Context
				o     Option
			)

			JustBeforeEach(func() {
				o = WithContext(inCTX)
				o(testOptions)
			})

			Context("with a context", func() {
				BeforeEach(func() {
					inCTX = context.Background()
					testOptions = &Options{logger: newNoopLogger()}
				})

				It("should set the option context", func() {
					Expect(testOptions.ctx).To(Equal(inCTX))
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
					testOptions = &Options{
						url:    u,
						logger: newNoopLogger(),
					}
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
					logger: newNoopLogger(),
				}
			})

			Context("with a client request error", func() {
				var expectErr error

				BeforeEach(func() {
					mockHTTP.expect(http.MethodGet, expectUrl, http.Header{"Accept-Encoding": []string{"gzip"}})
					mockHTTP.response(http.StatusBadRequest, nil, nil)
					expectErr = RequestError{StatusCode: http.StatusBadRequest, Url: expectUrl.String()}
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
			ctx          context.Context
			cancel       context.CancelFunc
		)

		BeforeEach(func() {
			server = ghttp.NewServer()
			mockHTTP = newHTTPMock(server)

			ctx, cancel = context.WithCancel(context.Background())

			options = &Options{
				client: &http.Client{},
				logger: newNoopLogger(),
			}

			readAtCloser = &ReadAtCloser{
				ctx:               ctx,
				cancel:            cancel,
				concurrentReaders: make(chan struct{}, MaxConcurrentReaders),
				log:               options.logger.WithField("testing", "ignored"),
				options:           options,
				readerWG:          &sync.WaitGroup{},
				mutex:             sync.Mutex{},
				readers:           make(map[string]*readAtCloseRead),
			}
		})

		AfterEach(func() {
			mockHTTP.finish()
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
					mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(int(start), int(start)+readLength))
					mockHTTP.response(http.StatusPartialContent, fullBody, nil)
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
					mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(int(start), readSize))
					mockHTTP.response(http.StatusBadRequest, nil, nil)
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
					mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(int(start), int(start)+readSize))
					mockHTTP.response(http.StatusPartialContent, fullBody[start:start+int64(readSize)], nil)
				})

				It("should return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("should return a zero length", func() {
					Expect(readLen).To(Equal(readSize))
				})
			})
		})

		Context(".HashURL", func() {
			var (
				expectUrl  *url.URL
				hashSchema uint
				chunkSize  int64
				fullBody   []byte

				err    error
				hashes []hash.Hash

				foe            error
				fullFile       *os.File
				fullFileName   = "/random.file"
				split10KHashes = []string{
					`cdd10328cfc79888d777d985600a2ae5e7d56ca3c775277f44cef8df2d08575b`,
					`6c9d7e7343c6d565f68bd9dcb197446a4df2aa5930834051e43e27dedab07938`,
					`9df40b5253fa05c7a487527ce83fd12cfacb9da0580c96ba1086dc6ef937a6b3`}

				split5KHashes = []string{
					`8b4a3f34b5af54bc3c0c414af025e9ba6ddb2ae79c9e5fa05374dc6c6ebff6e1`,
					`96d31d79d90e6432021a35b893f084a2fa761a2b88ef332c3aa3dd6bcd38b5f0`,
					`a15188beed7c98ab7c8d2037a98ccba1bcc92e7b5198b47371da02a814179e97`,
					`1ca7e47ba82ce81dfb1ebf79a5783b03498a7727cbb582ee9f65466fa74c5a07`,
					`9df40b5253fa05c7a487527ce83fd12cfacb9da0580c96ba1086dc6ef937a6b3`}
			)
			JustBeforeEach(func() {
				hashes, err = readAtCloser.HashURL(hashSchema)
			})

			BeforeEach(func() {
				expectUrl, _ = url.Parse(server.URL() + "/foo")
				readAtCloser.options.url = expectUrl.String()

				fullFile, foe = os.Open(testDataDir + fullFileName)
				Expect(foe).ToNot(HaveOccurred())
				fullBody, foe = ioutil.ReadAll(fullFile)
				Expect(foe).ToNot(HaveOccurred())

				readAtCloser.contentLength = int64(len(fullBody))
				readAtCloser.options.hashChunkSize = chunkSize
				hashSchema = sha256.Size

			})

			Context("with zero chunk size", func() {
				var expectHash hash.Hash

				BeforeEach(func() {
					expectHash = sha256.New()
					expectHash.Write(fullBody)
					readAtCloser.options.hashChunkSize = 0
					mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(0, len(fullBody)))
					mockHTTP.response(http.StatusPartialContent, fullBody, nil)
				})

				It("should not error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("should return slice of one", func() {
					Expect(len(hashes)).To(Equal(1))
				})

				It("should return the expected hash", func() {
					Expect(hashes[0].Sum(nil)).To(Equal(expectHash.Sum(nil)))
				})
			})

			Context("with a chunk size greater than the content", func() {
				var expectHash hash.Hash

				BeforeEach(func() {
					expectHash = sha256.New()
					expectHash.Write(fullBody)
					readAtCloser.options.hashChunkSize = int64(len(fullBody) + 1)
					mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(0, len(fullBody)))
					mockHTTP.response(http.StatusPartialContent, fullBody, nil)
				})

				It("should not error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("should return slice of one", func() {
					Expect(len(hashes)).To(Equal(1))
				})

				It("should return the expected hash", func() {
					Expect(hashes[0].Sum(nil)).To(Equal(expectHash.Sum(nil)))
				})
			})

			Context("with a chunk size less than the content", func() {
				Context("with an even split of the content into chunks", func() {
					BeforeEach(func() {
						chunkSize = 5 * 1 << 10
						readAtCloser.options.hashChunkSize = chunkSize

						for i := range split5KHashes {
							start := i * int(chunkSize)
							end := start + int(chunkSize)
							mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(start, end))
							mockHTTP.response(http.StatusPartialContent, fullBody[start:end], nil)
						}
					})

					It("should not error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("should return the expected count of hashes", func() {
						Expect(len(hashes)).To(Equal(len(split5KHashes)))
					})

					It("should return the expected hashes", func() {
						for i := range hashes {
							Expect(fmt.Sprintf("%x", hashes[i].Sum(nil))).To(Equal(split5KHashes[i]))
						}
					})
				})

				Context("with an uneven split of the content into chunks", func() {
					BeforeEach(func() {
						chunkSize = 10 * 1 << 10
						readAtCloser.options.hashChunkSize = chunkSize

						for i := range split10KHashes {
							start := i * int(chunkSize)
							end := start + int(chunkSize)

							// We expect the last chunk to be over the end and the expectation is that we'll only read to the end, since we know the length already.
							if end > len(fullBody) {
								end = len(fullBody)
							}

							mockHTTP.expect(http.MethodGet, expectUrl, rangeHead(start, end))
							mockHTTP.response(http.StatusPartialContent, fullBody[start:end], nil)
						}
					})

					It("should not error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("should return the expected count of hashes", func() {
						Expect(len(hashes)).To(Equal(len(split10KHashes)))
					})

					It("should return the expected hashes", func() {
						for i := range hashes {
							Expect(fmt.Sprintf("%x", hashes[i].Sum(nil))).To(Equal(split10KHashes[i]))
						}
					})
				})

			})
		})
	})
})

var _ = Describe("checkErrSlice", func() {
	var (
		es []error

		err error
	)

	JustBeforeEach(func() {
		err = checkErrSlice(es)
	})

	Context("with no errors in slice", func() {
		BeforeEach(func() {
			es = make([]error, 0)
		})

		It("should not return an error", func() {
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("with one error in slice", func() {
		BeforeEach(func() {
			es = []error{nil, errors.New("test error"), nil}
		})

		It("should return an error", func() {
			Expect(err).To(MatchError("test error"))
		})
	})

	Context("with multiple errors in slice", func() {
		BeforeEach(func() {
			es = []error{nil, errors.New("test error 2"), errors.New("test error 1"), nil}
		})

		It("should return an error", func() {
			Expect(err).To(MatchError("test error 2: test error 1"))
		})
	})
})

func rangeHead(start, end int) map[string][]string {
	r := fmt.Sprintf("bytes=%d-%d", start, end)
	return map[string][]string{
		"Range": {r},
	}
}

type noopWriter struct{}

func newNoopLogger() *logrus.Logger {
	noopLog := logrus.New()
	noopLog.SetOutput(&noopWriter{})
	return noopLog
}
func (*noopWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

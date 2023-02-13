package httpio

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dbyington/httpio/tracing"
	"github.com/sirupsen/logrus"
)

// Possible errors
var (
	ErrInvalidURLHost        = errors.New("invalid url host")
	ErrInvalidURLScheme      = errors.New("invalid url scheme")
	ErrReadFailed            = errors.New("read failed")
	ErrReadFromSource        = errors.New("read from source")
	ErrRangeReadNotSupported = errors.New("range reads not supported")
	ErrRangeReadNotSatisfied = errors.New("range not satisfied")

	ErrHeaderEtag          = errors.New("etag header differs")
	ErrHeaderContentLength = errors.New("content length differs")
	headerErrs             = map[string]error{
		"Etag":           ErrHeaderEtag,
		"Content-Length": ErrHeaderContentLength,
	}
)

const loggerKey = "logger"

// MaxConcurrentReaders limits the number of concurrent reads.
const MaxConcurrentReaders = 5

// RequestError fulfills the error type for reporting more specific errors with http requests.
type RequestError struct {
	StatusCode int
	Url        string
}

func getHeaderErr(h string) error {
	e, ok := headerErrs[h]
	if !ok {
		return fmt.Errorf("retrieving header '%s'", h)
	}
	return e
}

// Error returns the string of a RequestError.
func (r RequestError) Error() string {
	return fmt.Sprintf("Error requesting %s, received code: %d", r.Url, r.StatusCode)
}

// HTTPStatus returns the http code from the RequestError.
func (r RequestError) HTTPStatus() (int, bool) {
	if r.StatusCode < 200 {
		return 0, false
	}

	return r.StatusCode, true
}

// ReadSizeLimit is the maximum size buffer chunk to operate on.
const ReadSizeLimit = 32768

// Options contains the parts to create and use a ReadCloser or ReadAtCloser
type Options struct {
	client               *http.Client
	ctx                  context.Context
	expectHeaders        map[string]string
	hashChunkSize        int64
	logger               *logrus.Logger
	maxConcurrentReaders int64
	url                  string
}

// Option is a func type used to pass options to the New* funcs.
type Option func(*Options)

// ReadCloser contains the required parts to implement a io.ReadCloser on a URL.
type ReadCloser struct {
	options *Options

	cancel context.CancelFunc
}

type readClient interface {
	do(req *http.Request) (*http.Response, error)
}

type readAtCloseRead struct {
	client    readClient
	ctx       context.Context
	cancelCTX context.CancelFunc
	log       logrus.FieldLogger
	id        string
}

func (r *ReadAtCloser) newReader() *readAtCloseRead {
	ctx, cancel := context.WithCancel(r.ctx)
	reader := &readAtCloseRead{
		client:    r,
		ctx:       ctx,
		cancelCTX: cancel,
		id:        randomString(),
	}
	reader.log = r.log.WithField("readerId", reader.id)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.readers[reader.id] = reader

	return reader
}

func (r *readAtCloseRead) cancel() {
	r.cancelCTX()
}

// ReadAtCloser contains the required options and metadata to implement io.ReadAtCloser on a URL.
// Use the Options to configure the ReadAtCloser with an http.Client, URL, and any additional http.Header values that should be sent with the request.
type ReadAtCloser struct {
	options       *Options
	contentLength int64
	etag          string

	ctx               context.Context
	cancel            context.CancelFunc
	concurrentReaders chan struct{}
	log               logrus.FieldLogger
	mutex             sync.Mutex
	readers           map[string]*readAtCloseRead
	readerWG          *sync.WaitGroup
}

func (r *ReadAtCloser) do(req *http.Request) (resp *http.Response, err error) {
	r.log.Infoln("attempting to get concurrent reader slot")
	r.concurrentReaders <- struct{}{}
	r.log.Infoln("concurrent reader slot acquired, making request...")

	defer func() {
		<-r.concurrentReaders
		status := "completed successfully"
		if err != nil {
			status = "failed"
		}
		r.log.Infof("client request %s. reader slot freed", status)
	}()

	return r.options.client.Do(req)
}

func (r *ReadAtCloser) finishReader(id string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	reader, ok := r.readers[id]
	if !ok {
		r.log.Warnf("reader %s not found", id)
		return
	}

	reader.cancel()
	delete(r.readers, id)
}

// NewReadAtCloser validates the options provided and returns a new a *ReadAtCloser after validating the URL. The URL validation includes basic scheme and hostnane checks.
func NewReadAtCloser(opts ...Option) (r *ReadAtCloser, err error) {
	o := &Options{expectHeaders: make(map[string]string)}
	for _, opt := range opts {
		opt(o)
	}

	o.ensureOptions()

	if err := o.validateUrl(); err != nil {
		return nil, err
	}

	maxReaders := o.maxConcurrentReaders
	if maxReaders == 0 {
		maxReaders = MaxConcurrentReaders
	}

	contentLength, etag, err := o.headURL(o.expectHeaders)
	if err != nil {
		return nil, err
	}

	useCTX := context.Background()
	if o.ctx != nil {
		useCTX = o.ctx
	}

	ctx, cancel := context.WithCancel(useCTX)

	return &ReadAtCloser{
		cancel:            cancel,
		concurrentReaders: make(chan struct{}, maxReaders),
		contentLength:     contentLength,
		ctx:               ctx,
		etag:              etag,
		log:               o.logger.WithField("traceID", randomString(12)),
		options:           o,
		readerWG:          &sync.WaitGroup{},
		readers:           make(map[string]*readAtCloseRead),
	}, nil
}

// WithClient is an Option func which allows the user to supply their own instance of an http.Client. If not supplied a new generic http.Client is created.
func WithClient(c *http.Client) Option {
	return func(o *Options) {
		o.client = c
	}
}

// WithContext allows supplying a context for the ReadAtCloser to use.
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.ctx = ctx
	}
}

func WithLogger(l *logrus.Logger) Option {
	return func(o *Options) {
		o.logger = l
	}
}

// WithURL (REQUIRED) is an Option func used to supply the full url string; scheme, host, and path, to be read.
func WithURL(url string) Option {
	return func(o *Options) {
		o.url = url
	}
}

// WithMaxConcurrentReaders is an Option to set the maximum number of concurrent go funcs performing Reads in any Reader. If not supplied the default MaxConcurrentReaders is used.
func WithMaxConcurrentReaders(r int64) Option {
	return func(o *Options) {
		o.maxConcurrentReaders = r
	}
}

// WithExpectHeaders is an Option func used to specify any expected response headers from the server.
func WithExpectHeaders(e map[string]string) Option {
	return func(o *Options) {
		o.expectHeaders = e
	}
}

// WithHashChunkSize is an Option func to specify the size to chunk content at when hashing the content.
func WithHashChunkSize(s int64) Option {
	return func(o *Options) {
		o.hashChunkSize = s
	}
}

func (o *Options) ensureOptions() {
	o.ensureClient()
	o.ensureLogger()
}

func (o *Options) ensureClient() {
	if o.client == nil {
		o.client = &http.Client{
			Timeout: time.Second * 60,
			Transport: &http.Transport{
				IdleConnTimeout:       time.Second * 30,
				ResponseHeaderTimeout: time.Second * 10,
				MaxIdleConns:          25,
			},
		}
	}
}

func (o *Options) ensureLogger() {
	if o.logger == nil {
		o.logger = logrus.New()
		o.logger.Level = logrus.InfoLevel
	}
}

func (o *Options) validateUrl() error {
	u, err := url.Parse(o.url)
	if err != nil {
		return err
	}

	if u.Scheme == "" {
		return ErrInvalidURLScheme
	}

	if u.Hostname() == "" {
		return ErrInvalidURLHost
	}

	return nil
}

func (o *Options) headURL(expectHeaders map[string]string) (int64, string, error) {
	head, err := o.client.Head(o.url)
	if err != nil {
		return 0, "", err
	}

	if head.StatusCode != http.StatusOK {
		return 0, "", RequestError{StatusCode: head.StatusCode, Url: o.url}
	}

	logHeaders(o.logger.WithField("method", http.MethodHead), head.Header)

	if head.Header.Get("accept-ranges") != "bytes" {
		return 0, "", ErrRangeReadNotSupported
	}

	for k, v := range expectHeaders {
		if sent := head.Header.Get(k); sent != v {
			return 0, "", getHeaderErr(k)
		}
	}

	// This is really annoying, but we have to strip the quotes around the string.
	etag := strings.Trim(head.Header.Get("Etag"), "\"")
	return head.ContentLength, etag, nil
}

func (o *Options) hashURL(hashSize uint) (hash.Hash, error) {
	res, err := o.client.Get(o.url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 399 {
		return nil, RequestError{StatusCode: res.StatusCode, Url: o.url}
	}

	logHeaders(o.logger.WithField("method", http.MethodGet), res.Header)

	switch hashSize {
	case sha256.Size:
		return sha256SumReader(res.Body)
	default:
		return md5SumReader(res.Body)
	}
}

// HashURL takes the hash scheme as a uint (either sha256.Size or md5.Size) and the chunk size, and returns the hashed URL body in the supplied scheme as a slice of hash.Hash.
// When the chunk size is less than the length of the content, the URL will be read with multiple, concurrent range reads to create the slice of hash.Hash.
// Specifying a chunkSize <= 0 is translated to "do not chunk" and the entire content will be hashed as one chunk.
// The size and capacity of the returned slice of hash.Hash is equal to the number of chunks calculated based on the content length divided by the chunkSize, or 1 if chunkSize is equal to, or less than 0.
func (r *ReadAtCloser) HashURL(scheme uint) ([]hash.Hash, error) {
	r.log.Infoln("starting hash url")
	// Quickly copy these out and release the lock.
	r.mutex.Lock()
	cl := r.contentLength
	chunkSize := r.options.hashChunkSize
	r.mutex.Unlock()

	if chunkSize <= 0 {
		chunkSize = cl
	}
	var chunks int64

	// If chunkSize is greater than the content length reset it to the available length and set number of chunks to 1.
	// Otherwise, we need to divide the length by the number of chunks and round up. The final chunkSize will be the sum of the remainder.
	if chunkSize > cl {
		chunkSize = cl
		chunks = 1
	} else {
		chunks = int64(math.Ceil(float64(cl) / float64(chunkSize)))
	}

	var hasher func(reader io.Reader) (hash.Hash, error)

	switch scheme {
	case sha256.Size:
		hasher = sha256SumReader
	default:
		hasher = md5SumReader
	}

	hashes := make([]hash.Hash, chunks, chunks)
	hashErrs := make([]error, chunks)
	wg := sync.WaitGroup{}

	defer func(t time.Time) { r.log.Debugf("completed hashing in %s", time.Since(t)) }(time.Now())
	r.log.Debugf("hashing %d chunks", chunks)

	for i := int64(0); i < chunks; i++ {
		// The remaining size is the smallest of the chunkSize or the chunkSize times the number of chunks already read.
		remaining := chunkSize
		if remaining > cl-(i*chunkSize) {
			remaining = cl - (i * chunkSize)
		}

		// If remaining ends up less than 0 then the math above to determine the number of chunks is incorrect or something bad happened.
		if remaining < 0 {
			return nil, errors.New("failed to properly calculate the hash chunk count")
		}

		start := chunkSize * i

		wg.Add(1)
		go func(w *sync.WaitGroup, idx, start, size int64, rac *ReadAtCloser) {
			t := time.Now()
			rac.log.Debugf("reading chunk %d", idx)
			defer w.Done()
			b := make([]byte, size)
			if _, err := rac.ReadAt(b, start); err != nil {
				hashErrs[idx] = err
				if err != io.ErrUnexpectedEOF {
					return
				}
			}
			rac.log.Debugf("finished reading chunk in %s", time.Since(t))

			reader := bytes.NewReader(b[:])
			h, err := hasher(reader)
			if err != nil {
				rac.log.Errorf("reading chunk %d: %s", idx, err)
				hashErrs[idx] = err
				return
			}

			hashes[idx] = h
			rac.log.Debugf("finished hashing chunk %d in %s", idx, time.Since(t))
		}(&wg, i, start, remaining, r)
	}

	wg.Wait()
	if err := checkErrSlice(hashErrs); err != nil {
		return hashes, err
	}
	return hashes, nil
}

func checkErrSlice(es []error) (err error) {
	for _, e := range es {
		if e != nil {
			if err != nil {
				err = fmt.Errorf("%s: %s", err, e)
				continue
			}
			err = e
		}
	}
	return
}

// Length returns the reported ContentLength of the URL body.
func (r *ReadAtCloser) Length() int64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.contentLength
}

// Etag returns the last read Etag from the URL being operated on by the configured ReadAtCloser.
func (r *ReadAtCloser) Etag() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.etag
}

func (r *ReadAtCloser) ChunkSize() int64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.options.hashChunkSize
}

func (r *ReadAtCloser) URL() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.options.url
}

// ReadAt satisfies the io.ReaderAt interface. It requires the ReadAtCloser be previously configured.
func (r *ReadAtCloser) ReadAt(b []byte, start int64) (n int, err error) {
	end := start + int64(len(b))
	logger := r.log.WithField("ReadAtID", randomString(5))
	logger.Tracef("reading from %d to %d", start, end)

	r.readerWG.Add(1)
	defer r.readerWG.Done()

	reader := r.newReader()
	defer r.finishReader(reader.id)
	req, err := http.NewRequestWithContext(reader.ctx, http.MethodGet, r.options.url, nil)
	if err != nil {
		return 0, err
	}

	requestRange := fmt.Sprintf("bytes=%d-%d", start, end)
	logger.Debugf("requesting range: '%s'", requestRange)
	req.Header.Add("Range", requestRange)

	tracingClient := tracing.NewClient(r.log)
	req = tracingClient.TraceRequest(req)

	res, err := reader.client.do(req)
	if err != nil {
		return 0, err
	}

	logger.Debugf("request answered with proto %s", res.Proto)
	logger.Debugf("request answered with (%d) %s", res.StatusCode, res.Status)
	logHeaders(logger.WithField("method", http.MethodGet), res.Header)
	tracingClient.LogTimers()

	if res.StatusCode != http.StatusPartialContent && res.StatusCode != http.StatusOK {
		return 0, ErrRangeReadNotSatisfied
	}

	bt := make([]byte, len(b))
	bt, err = ioutil.ReadAll(res.Body)
	if err = res.Body.Close(); err != nil {
		r.log.Errorf("while closing request body: %s", err)
	}

	copy(b, bt)

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.contentLength < end {
		return int(res.ContentLength - start), io.ErrUnexpectedEOF
	}

	l := int64(len(b))
	if l > res.ContentLength {
		r.log.Debugf("read more than expected (%d bytes), resetting to expected ContentLength: %d\n", len(b), res.ContentLength)
		l = res.ContentLength
	}
	logger.Debugf("read %d bytes from body", l)
	return int(l), nil
}

// Close cancels the client context and closes any idle connections.
func (r *ReadAtCloser) Close() error {
	// Ensure a cancellable context has been created else r.cancel will be nil.
	if r.cancel != nil {
		r.cancel()
	}

	r.options.client.CloseIdleConnections()

	r.readerWG.Wait()
	return nil
}

// HashURL takes the hash scheme size (sha256.Size or md5.Size) and returns the hashed URL body in the supplied scheme as a hash.Hash interface.
func (r *ReadCloser) HashURL(size uint) (hash.Hash, error) {
	return r.options.hashURL(size)
}

// Read fulfills the io.Reader interface. The ReadCloser must be previously configured. The body of the configured URL is read into p, up to len(p). If the length of p is greater than the ContentLength of the body the length returned will be ContentLength.
func (r *ReadCloser) Read(p []byte) (n int, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.options.url, nil)
	if err != nil {
		return 0, err
	}

	res, err := r.options.client.Do(req)
	if err != nil {
		return 0, err
	}

	if res.StatusCode != http.StatusOK {
		return 0, ErrReadFailed
	}

	bt := make([]byte, len(p))
	bt, err = ioutil.ReadAll(res.Body)

	copy(p, bt)

	l := int64(len(p))
	if l > res.ContentLength {
		l = res.ContentLength
	}
	return int(l), nil
}

// Close cancels the client context and closes any idle connections.
func (r *ReadCloser) Close() error {
	// Ensure a cancellable context has been created else r.cancel will be nil.
	if r.cancel != nil {
		r.cancel()
	}

	r.options.client.CloseIdleConnections()
	return nil
}

// sha256SumReader reads from r until and calculates the sha256 sum, until r is exhausted.
func sha256SumReader(r io.Reader) (hash.Hash, error) {
	shaSum := sha256.New()

	buf := make([]byte, ReadSizeLimit)
	if _, err := io.CopyBuffer(shaSum, r, buf); err != nil {
		return nil, err
	}

	return shaSum, nil
}

// md5SumReader reads from r until and calculates the md5 sum, until r is exhausted.
func md5SumReader(r io.Reader) (hash.Hash, error) {
	md5sum := md5.New()
	buf := make([]byte, ReadSizeLimit)
	if _, err := io.CopyBuffer(md5sum, r, buf); err != nil {
		return nil, err
	}

	return md5sum, nil
}

func randomString(x ...int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	size := rand.Intn(25) + 25
	if len(x) > 0 {
		size = x[0]
	}

	s := make([]rune, size)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func logHeaders(l logrus.FieldLogger, h map[string][]string) {
	l.Infoln("response headers")
	for key, values := range h {
		l.Infof("%s: %+v", key, values)
	}
}

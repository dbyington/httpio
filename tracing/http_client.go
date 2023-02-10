package tracing

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"time"

	"github.com/sirupsen/logrus"
)

type Client struct {
	clientTrace *httptrace.ClientTrace
	logger      logrus.FieldLogger

	timers *timers
}

type timers struct {
	start                time.Time
	GetConn              time.Duration
	GotConn              time.Duration
	PutIdleConn          time.Duration
	GotFirstResponseByte time.Duration
	Got100Continue       time.Duration
	Got1xxResponse       time.Duration
	DNSStart             time.Duration
	DNSDone              time.Duration
	ConnectStart         time.Duration
	ConnectDone          time.Duration
	TLSHandshakeStart    time.Duration
	TLSHandshakeDone     time.Duration
	WroteHeaderField     time.Duration
	WroteHeaders         time.Duration
	Wait100Continue      time.Duration
	WroteRequest         time.Duration
}

func (t *timers) Update(timer string) {
	switch timer {
	case "GetConn":
		t.start = time.Now()
		t.GetConn = time.Since(t.start)
	case "GotConn":
		t.GotConn = time.Since(t.start)
	case "PutIdleConn":
		t.PutIdleConn = time.Since(t.start)
	case "GotFirstResponseByte":
		t.GotFirstResponseByte = time.Since(t.start)
	case "Got100Continue":
		t.Got100Continue = time.Since(t.start)
	case "Got1xxResponse":
		t.Got1xxResponse = time.Since(t.start)
	case "DnsStart":
		t.DNSStart = time.Since(t.start)
	case "DnsDone":
		t.DNSDone = time.Since(t.start)
	case "ConnectStart":
		t.ConnectStart = time.Since(t.start)
	case "ConnectDone":
		t.ConnectDone = time.Since(t.start)
	case "TlsHandshakeStart":
		t.TLSHandshakeStart = time.Since(t.start)
	case "TlsHandshakeDone":
		t.TLSHandshakeDone = time.Since(t.start)
	case "WroteHeaderField":
		t.WroteHeaderField = time.Since(t.start)
	case "WroteHeaders":
		t.WroteHeaders = time.Since(t.start)
	case "Wait100Continue":
		t.Wait100Continue = time.Since(t.start)
	case "WroteRequest":
		t.WroteRequest = time.Since(t.start)
	}
}

func NewClient(fl logrus.FieldLogger) *Client {
	c := &Client{
		logger: fl,
		timers: new(timers),
	}

	c.clientTrace = &httptrace.ClientTrace{
		GetConn:              c.GetConn,
		GotConn:              c.GotConn,
		PutIdleConn:          c.PutIdleConn,
		GotFirstResponseByte: c.GotFirstResponseByte,
		Got100Continue:       c.Got100Continue,
		Got1xxResponse:       c.Got1xxResponse,
		DNSStart:             c.DNSStart,
		DNSDone:              c.DNSDone,
		ConnectStart:         c.ConnectStart,
		ConnectDone:          c.ConnectDone,
		TLSHandshakeStart:    c.TLSHandshakeStart,
		TLSHandshakeDone:     c.TLSHandshakeDone,
		WroteHeaderField:     c.WroteHeaderField,
		WroteHeaders:         c.WroteHeaders,
		Wait100Continue:      c.Wait100Continue,
		WroteRequest:         c.WroteRequest,
	}

	return c
}

func (c *Client) TraceRequest(req *http.Request) *http.Request {
	tracingContext := httptrace.WithClientTrace(req.Context(), c.clientTrace)
	return req.WithContext(tracingContext)
}

func (c *Client) LogTimers() {
	c.logger.Infof("\nGetConn: %s\nGotConn: %s\nPutIdleConn: %s\nGotFirstResponseByte: %s\nGot100Continue: %s\nGot1xxResponse: %s\nDNSStart: %s\nDNSDone: %s\nConnectStart: %s\nConnectDone: %s\nTLSHandshakeStart: %s\nTLSHandshakeDone: %s\nWroteHeaderField: %s\nWroteHeaders: %s\nWait100Continue: %s\nWroteRequest: %s\n",
		c.timers.GetConn,
		c.timers.GotConn,
		c.timers.PutIdleConn,
		c.timers.GotFirstResponseByte,
		c.timers.Got100Continue,
		c.timers.Got1xxResponse,
		c.timers.DNSStart,
		c.timers.DNSDone,
		c.timers.ConnectStart,
		c.timers.ConnectDone,
		c.timers.TLSHandshakeStart,
		c.timers.TLSHandshakeDone,
		c.timers.WroteHeaderField,
		c.timers.WroteHeaders,
		c.timers.Wait100Continue,
		c.timers.WroteRequest)
}

func (c *Client) GetConn(port string) {
	c.timers.Update("GetConn")
}

func (c *Client) GotConn(httptrace.GotConnInfo) {
	c.timers.Update("GotConn")
}

func (c *Client) PutIdleConn(error) {
	c.timers.Update("PutIdleConn")
}

func (c *Client) GotFirstResponseByte() {
	c.timers.Update("GotFirstResponseByte")
}

func (c *Client) Got100Continue() {
	c.timers.Update("Got100Continue")
}

func (c *Client) Got1xxResponse(code int, header textproto.MIMEHeader) error {
	c.timers.Update("Got1xxResponse")
	return nil
}

func (c *Client) DNSStart(info httptrace.DNSStartInfo) {
	c.timers.Update("DNSStart")
}

func (c *Client) DNSDone(info httptrace.DNSDoneInfo) {
	c.timers.Update("DNSDone")
}

func (c *Client) ConnectStart(string, string) {
	c.timers.Update("ConnectStart")
}

func (c *Client) ConnectDone(network string, addr string, err error) {
	c.timers.Update("ConnectDone")
}

func (c *Client) TLSHandshakeStart() {
	c.timers.Update("TLSHandshakeStart")
}

func (c *Client) TLSHandshakeDone(state tls.ConnectionState, err error) {
	c.timers.Update("TLSHandshakeDone")
}

func (c *Client) WroteHeaderField(key string, value []string) {
	c.timers.Update("WroteHeaderField")
}

func (c *Client) WroteHeaders() {
	c.timers.Update("WroteHeaders")
}

func (c *Client) Wait100Continue() {
	c.timers.Update("Wait100Continue")
}

func (c *Client) WroteRequest(info httptrace.WroteRequestInfo) {
	c.timers.Update("WroteRequest")
}

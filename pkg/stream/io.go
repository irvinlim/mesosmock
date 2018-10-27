package stream

import (
	"context"
	"net/http"

	"github.com/mesos/mesos-go/api/v1/lib/recordio"
)

// Writer allows writing to streams in RecordIO format,
// automatically flushing the buffer after each write.
type Writer struct {
	writer  *recordio.Writer
	flusher http.Flusher

	ctx context.Context
}

// NewWriter creates a new stream.Writer instance.
func NewWriter(w http.ResponseWriter) *Writer {
	flusher, ok := w.(http.Flusher)
	if !ok {
		panic("expected http.ResponseWriter to be an http.Flusher")
	}

	return &Writer{
		writer:  recordio.NewWriter(w),
		flusher: flusher,
	}
}

// Context returns the writers's context. To change the context, use
// WithContext.
//
// The returned context is always non-nil; it defaults to the
// background context.
func (w Writer) Context() context.Context {
	if w.ctx != nil {
		return w.ctx
	}

	return context.Background()
}

// WithContext returns a shallow copy of w with its context changed
// to ctx. The provided ctx must be non-nil.
func (w *Writer) WithContext(ctx context.Context) *Writer {
	if ctx == nil {
		panic("nil context")
	}

	w2 := new(Writer)
	*w2 = *w
	w2.ctx = ctx
	return w2
}

// WriteFrame writes a frame to the wrapped writer instance
// using RecordIO format.
func (w Writer) WriteFrame(frame []byte) {
	select {
	case <-w.Context().Done():
		return
	default:
		w.writer.WriteFrame(frame)
		w.flusher.Flush()
	}
}

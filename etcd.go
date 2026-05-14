package watch

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdWatcher struct {
	logger *zap.Logger
	client *clientv3.Client
	prefix string

	ch    chan []string
	err   atomic.Pointer[error]
	start func() <-chan []string
	close func()
}

var _ Watcher = (*EtcdWatcher)(nil)

// NewEtcdWatcher watches all keys under prefix; each value is treated as an
// address ("host:port"). Emits the sorted set of current values on every change.
func NewEtcdWatcher(ctx context.Context, log *zap.Logger, endpoints []string, prefix string) (*EtcdWatcher, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      log.With(zap.String("module", "etcd_watcher")),
	})
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	w := &EtcdWatcher{
		logger: log,
		client: cli,
		prefix: prefix,
		ch:     make(chan []string, 1),
	}
	w.start = sync.OnceValue(func() <-chan []string {
		go w.run(runCtx)
		return w.ch
	})
	w.close = sync.OnceFunc(func() {
		cancel()
		_ = cli.Close()
	})

	return w, nil
}

func (w *EtcdWatcher) run(ctx context.Context) {
	defer close(w.ch)

	resp, err := w.client.Get(ctx, w.prefix, clientv3.WithPrefix())
	if err != nil {
		w.err.Store(&err)
		w.logger.Error("etcd initial get failed", zap.String("prefix", w.prefix), zap.Error(err))
		return
	}

	state := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		state[string(kv.Key)] = string(kv.Value)
	}
	coalesce(w.ch, slices.Sorted(maps.Values(state)))

	watchCh := w.client.Watch(ctx, w.prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(resp.Header.Revision+1),
	)
	for wresp := range watchCh {
		if err := wresp.Err(); err != nil {
			w.err.Store(&err)
			w.logger.Error("etcd watch error", zap.String("prefix", w.prefix), zap.Error(err))
			return
		}
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				state[string(ev.Kv.Key)] = string(ev.Kv.Value)
			case clientv3.EventTypeDelete:
				delete(state, string(ev.Kv.Key))
			}
		}
		coalesce(w.ch, slices.Sorted(maps.Values(state)))
	}
}

func (w *EtcdWatcher) Watch() <-chan []string { return w.start() }

func (w *EtcdWatcher) Err() error {
	if p := w.err.Load(); p != nil {
		return *p
	}
	return nil
}

func (w *EtcdWatcher) Close() { w.close() }

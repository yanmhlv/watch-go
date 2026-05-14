package watch

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"go.uber.org/zap"
)

type Watcher interface {
	Watch() <-chan []string
}

type ConsulWatcher struct {
	logger     *zap.Logger
	plan       *watch.Plan
	consulAddr string

	ch    chan []string
	err   atomic.Pointer[error]
	start func() <-chan []string
	stop  func() bool
	close func()
}

var _ Watcher = (*ConsulWatcher)(nil)

type zapWriter struct {
	logger *zap.Logger
}

func (w *zapWriter) Write(data []byte) (int, error) {
	w.logger.Warn(string(data))
	return len(data), nil
}

func NewConsulWatcher(ctx context.Context, log *zap.Logger, consulAddr, name, dc, tag string) (*ConsulWatcher, error) {
	plan, err := watch.Parse(map[string]any{
		"type":        "service",
		"service":     name,
		"datacenter":  dc,
		"tag":         tag,
		"passingonly": true,
	})
	if err != nil {
		return nil, err
	}

	w := &ConsulWatcher{
		logger:     log,
		plan:       plan,
		consulAddr: consulAddr,
		ch:         make(chan []string, 1),
	}
	plan.LogOutput = &zapWriter{log.With(zap.String("module", "consul_watcher"))}
	plan.Handler = func(_ uint64, data any) {
		entries, ok := data.([]*api.ServiceEntry)
		if !ok {
			log.Warn("unexpected watch payload", zap.String("type", fmt.Sprintf("%T", data)))
			return
		}
		addrs := make([]string, 0, len(entries))
		for _, srv := range entries {
			host := srv.Service.Address
			if host == "" {
				host = srv.Node.Address
			}
			addrs = append(addrs, net.JoinHostPort(host, strconv.Itoa(srv.Service.Port)))
		}
		coalesce(w.ch, addrs)
	}

	w.start = sync.OnceValue(func() <-chan []string {
		go func() {
			defer close(w.ch)
			if runErr := plan.Run(consulAddr); runErr != nil {
				w.err.Store(&runErr)
				log.Error("watch plan exited", zap.String("consul_address", consulAddr), zap.Error(runErr))
			}
		}()
		return w.ch
	})
	w.stop = context.AfterFunc(ctx, func() { plan.Stop() })
	w.close = sync.OnceFunc(func() {
		if w.stop() {
			plan.Stop()
		}
	})

	return w, nil
}

func (w *ConsulWatcher) Watch() <-chan []string {
	return w.start()
}

func (w *ConsulWatcher) Err() error {
	if p := w.err.Load(); p != nil {
		return *p
	}
	return nil
}

func (w *ConsulWatcher) Close() {
	w.close()
}

// coalesce sends addrs on a cap=1 channel, replacing any unread snapshot.
// Safe only with a single producer.
func coalesce(ch chan []string, addrs []string) {
	select {
	case ch <- addrs:
	default:
		select {
		case <-ch:
		default:
		}
		ch <- addrs
	}
}

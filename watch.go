package watch

import (
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"go.uber.org/zap"
)

type (
	Address = string

	Watcher interface {
		Watch() (<-chan []Address, error)
	}

	ConsulWatcher struct {
		logger     *zap.Logger
		plan       *watch.Plan
		consulAddr Address

		once sync.Once

		ch  chan []Address
		err error
	}

	logger struct {
		logger *zap.Logger
	}
)

func (w *logger) Write(data []byte) (int, error) {
	w.logger.Warn(string(data))
	return len(data), nil
}

func NewConsulWatcher(log *zap.Logger, consulAddr, name, dc, tag string) (*ConsulWatcher, error) {
	plan, err := watch.Parse(map[string]interface{}{
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
		consulAddr: consulAddr,
		plan:       plan,
		ch:         make(chan []Address),
	}
	w.plan.LogOutput = &logger{log.With(zap.String("module", "consul_watcher"))}
	w.plan.Handler = func(i uint64, data interface{}) {
		var addrs []Address
		for _, srv := range data.([]*api.ServiceEntry) {
			host := srv.Service.Address
			if host == "" {
				host = srv.Node.Address
			}

			addrs = append(addrs, net.JoinHostPort(host, strconv.Itoa(srv.Service.Port)))
		}
		w.ch <- addrs
	}

	return w, nil
}

func (w *ConsulWatcher) Watch() (<-chan []Address, error) {
	w.once.Do(func() {
		go func() {
			defer func() { close(w.ch) }()
			if err := w.plan.Run(w.consulAddr); err != nil {
				w.err = err
				w.logger.Error("running watch plan error", zap.String("consul_address", w.consulAddr), zap.Error(w.err))
			}
		}()
	})

	return w.ch, w.err
}

package watch

import (
	"net"
	"strconv"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
	"go.uber.org/zap"
)

type (
	WatchPlan struct {
		logger *zap.Logger
		plan   *watch.Plan
	}

	LogWriter struct {
		logger *zap.Logger
	}

	Address = string
)

func (w *LogWriter) Write(data []byte) (int, error) {
	w.logger.Warn(string(data))
	return len(data), nil
}

func (c *WatchPlan) Stop() {
	c.plan.Stop()
}

func (w *WatchPlan) Watch(log *zap.Logger, consulAddr, name, dc, tag string) (<-chan []Address, error) {
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

	ch := make(chan []Address)

	w.plan = plan
	w.plan.LogOutput = &LogWriter{log.With(zap.String("module", "consul_watcher"))}
	w.plan.Handler = func(i uint64, data interface{}) {
		var addrs Addresses
		for _, srv := range data.([]*api.ServiceEntry) {
			host := srv.Service.Address
			if host != "" {
				host = srv.Node.Address
			}

			addrs = append(addrs, net.JoinHostPort(host, strconv.Itoa(srv.Service.Port)))
		}
		ch <- addrs
	}

	go func() {
		if err := w.plan.Run(consulAddr); err != nil {
			w.logger.Error("running watch plan error", zap.String("consul_address", consulAddr), zap.Error(err))
		}
		close(ch)
	}()

	return ch, nil
}

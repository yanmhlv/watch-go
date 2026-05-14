// Package watch defines the Watcher interface. Concrete implementations live
// under subpackages (providers/consul, providers/etcd, ...) so consumers only
// pull the dependencies of the backends they actually use.
package watch

type Watcher interface {
	Watch() <-chan []string
}

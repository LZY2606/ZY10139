# TTLCache - an in-memory cache with item expiration and generics

[![Go Reference](https://pkg.go.dev/badge/github.com/jellydator/ttlcache/v3.svg)](https://pkg.go.dev/github.com/jellydator/ttlcache/v3)
[![Build Status](https://github.com/jellydator/ttlcache/actions/workflows/go.yml/badge.svg)](https://github.com/jellydator/ttlcache/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/jellydator/ttlcache/badge.svg?branch=v3)](https://coveralls.io/github/jellydator/ttlcache?branch=v3)

## Features
- Simple API built with type parameters (generics)
- Per-item or cache-wide TTL with automatic deletion of expired items
- Automatic expiration time extension on each `Get` call (can be disabled)
- `Loader` interface that may be used to load/lazily initialize missing
  cache items, with optional duplicate call suppression
- Batch reads via `GetMany`, a context-aware `ValueLoader`, and an
  optional generation guard that prevents stale loads from replacing a
  cleared or concurrently updated cache
- Capacity limits based on the number of items or their custom-calculated cost
- Event handlers (insertion, update, and eviction)
- Metrics
- Thread safety

## Installation
```
go get github.com/jellydator/ttlcache/v3
```

## Usage
All cache operations are provided by the `Cache` type, which represents
a single in-memory data store. To create a new instance of it, the
`ttlcache.New()` function needs to be called:
```go
func main() {
	cache := ttlcache.New[string, string]()
}
```

By default, items never expire and are never removed automatically.
Expiration is enabled by setting a default TTL with the `ttlcache.WithTTL()`
option and starting the automatic cleanup process with the `cache.Start()`
method. Since `cache.Start()` blocks until `cache.Stop()` is called, it is
usually launched on a separate goroutine:
```go
func main() {
	cache := ttlcache.New[string, string](
		ttlcache.WithTTL[string, string](30 * time.Minute),
	)

	go cache.Start() // starts automatic expired item deletion
	defer cache.Stop()
}
```

Automatic cleanup suits most applications, but some may need to control
the exact timing of expired item deletion. For example, a system may
want to delete such items only when its resource load is at its lowest
(e.g., after midnight, when the number of users/HTTP requests drops).
In cases like these, the `cache.DeleteExpired()` method can be called
periodically instead of starting the cleanup process:
```go
func main() {
	cache := ttlcache.New[string, string](
		ttlcache.WithTTL[string, string](30 * time.Minute),
	)

	for {
		time.Sleep(4 * time.Hour)
		cache.DeleteExpired()
	}
}
```

The data stored in the cache can be inserted, retrieved, checked, and
deleted with `Set`, `Get`, `Has`, `Delete`, and other related methods.
Each new item receives a TTL: a specific duration, `ttlcache.DefaultTTL`
to use the cache's default one, or `ttlcache.NoTTL` to never expire:
```go
func main() {
	cache := ttlcache.New[string, string](
		ttlcache.WithTTL[string, string](30 * time.Minute),
	)

	// insert data
	cache.Set("first", "value1", ttlcache.DefaultTTL)
	cache.Set("second", "value2", ttlcache.NoTTL)
	cache.Set("third", "value3", time.Minute)

	// retrieve data
	item := cache.Get("first")
	fmt.Println(item.Value(), item.ExpiresAt())

	// check whether data exists
	ok := cache.Has("third")

	// delete data
	cache.Delete("second")
	cache.DeleteExpired()
	cache.DeleteAll()

	// retrieve data if it exists, insert it otherwise
	item, found := cache.GetOrSet("fourth", "value4", ttlcache.WithTTL[string, string](time.Minute))

	// retrieve and delete data
	item, present := cache.GetAndDelete("fourth")
}
```

The `cache.OnInsertion()`, `cache.OnUpdate()`, and `cache.OnEviction()`
methods subscribe to the cache's events. The subscribed functions are
executed on separate goroutines, so they never block the cache's
operations, and each subscription method returns a function that can
be called to unsubscribe:
```go
func main() {
	cache := ttlcache.New[string, string](
		ttlcache.WithTTL[string, string](30 * time.Minute),
		ttlcache.WithCapacity[string, string](300),
	)

	cache.OnInsertion(func(ctx context.Context, item *ttlcache.Item[string, string]) {
		fmt.Println(item.Value(), item.ExpiresAt())
	})
	cache.OnUpdate(func(ctx context.Context, item *ttlcache.Item[string, string]) {
		fmt.Println(item.Value(), item.ExpiresAt())
	})
	unsubscribe := cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, string]) {
		if reason == ttlcache.EvictionReasonCapacityReached {
			fmt.Println(item.Key(), item.Value())
		}
	})

	cache.Set("first", "value1", ttlcache.DefaultTTL)
	cache.DeleteAll()

	// stop receiving eviction events
	unsubscribe()
}
```

A custom or existing implementation of the `ttlcache.Loader` interface
can be used to load or lazily initialize data on cache misses. The
`Get` method calls the loader whenever the requested item is not found
and returns whatever the loader returns:
```go
func main() {
	loader := ttlcache.LoaderFunc[string, string](
		func(c *ttlcache.Cache[string, string], key string) *ttlcache.Item[string, string] {
			// load from file/make an HTTP request
			item := c.Set(key, "value from file", ttlcache.DefaultTTL)
			return item
		},
	)
	cache := ttlcache.New[string, string](
		ttlcache.WithLoader[string, string](loader),
	)

	item := cache.Get("key from file")
}
```

When multiple goroutines request the same missing item at once, the
loader normally runs once for each of them. Wrapping it with
`ttlcache.NewSuppressedLoader()` ensures that only one load operation
is in-flight for a given key at a time, with all callers receiving
its result:
```go
func main() {
	loader := ttlcache.LoaderFunc[string, string](
		func(c *ttlcache.Cache[string, string], key string) *ttlcache.Item[string, string] {
			// load from file/make an HTTP request
			item := c.Set(key, "value from file", ttlcache.DefaultTTL)
			return item
		},
	)
	cache := ttlcache.New[string, string](
		ttlcache.WithLoader[string, string](ttlcache.NewSuppressedLoader(loader, nil)),
	)

	item := cache.Get("key from file")
}
```

### Batch reads with `GetMany`
`GetMany` retrieves an ordered list of keys in a single call, with
per-key options and an upper bound on concurrent load executions:
```go
results := cache.GetMany(ctx,
	[]string{"user:1", "user:2", "user:1"},
	&ttlcache.GetManyOptions[string, string]{
		MaxConcurrency: 8,
	},
)
// results is aligned with the input: results[0] and results[2] share the
// same item because "user:1" is looked up and loaded at most once.
for _, r := range results {
	if r.Miss() {
		// not found
	} else if r.Err != nil {
		// loader error
	} else {
		_ = r.Item.Value()
	}
}
```

Semantics:
- Results are aligned by position. Each unique key is looked up and
  loaded at most once per call; the same `*Item` is returned for every
  repeated position.
- The hit/miss decision, touches and hit/miss metrics are a snapshot
  taken while the keys are first examined, in first-occurrence order.
  Loaded values, load errors, insertion/update metrics and evictions
  caused by an insertion are decided when each load completes, so the
  relative order of effects for different keys is unspecified under
  concurrency.
- When the passed context is canceled, no new loads are started and
  unresolved results report the context error. A load shared with other
  callers is not canceled because one waiter gave up; it is canceled
  only after every waiter abandons it.
- `GetMany` coordinates in-flight loads of the same key across calls,
  matching the singleflight behavior of `NewSuppressedLoader`, and the
  two compose without nesting deadlocks for different keys.

### ValueLoader and the generation guard
`ValueLoader` is a context-aware loader used by `GetMany`. Unlike
`Loader`, it only returns the value and its TTL; the cache performs the
insertion, so load errors can be reported and stale results can be
rejected. Return `ttlcache.ErrNotFound` for an ordinary miss:
```go
loader := ttlcache.ValueLoaderFunc[string, string](
	func(ctx context.Context, c *ttlcache.Cache[string, string], key string) (string, time.Duration, error) {
		if key == "missing" {
			return "", 0, ttlcache.ErrNotFound
		}
		return fetch(ctx, key), ttlcache.DefaultTTL, nil
	},
)
cache := ttlcache.New[string, string](
	ttlcache.WithValueLoader[string, string](loader),
	ttlcache.WithGenerationGuard[string, string](),
)
```

With `WithGenerationGuard`, the cache keeps a monotonically increasing
generation that advances on `DeleteAll`, on `Start` after a `Stop`, and
on explicit `ResetGeneration` calls. A load captures the generation when
it starts. When it completes, its value is returned to the callers that
waited for it, but it is not written back when:
- the generation advanced while the load was in-flight, or
- the key was concurrently set, deleted or evicted after the lookup
  snapshot.

The returned item is then a detached item that is not part of the cache.
A concurrent `Set` of the same key always wins over an in-flight load,
even within the same generation.

Without the generation guard, the historical last-writer-wins behavior
is preserved (including across `DeleteAll`/`ResetGeneration`), and a
plain `Loader` configured with `WithLoader` always keeps its legacy
behavior: it inserts the value itself, so neither the guard nor
concurrent sets can prevent its write-back. The single-key `Get` method
is unaffected by these additions and keeps its existing behavior.

The cache's capacity can also be restricted by criteria other than the
number of items. The `ttlcache.WithMaxCost()` option assigns each item
a cost, calculated by a custom function, and evicts the least recently
used items whenever the total cost exceeds the given limit. The
following example limits the memory used by cached entries to ~5KiB:
```go
func main() {
	cache := ttlcache.New[string, string](
		ttlcache.WithMaxCost[string, string](5120, func(item ttlcache.CostItem[string, string]) uint64 {
			// Note: the calculation below does not include the memory
			// used by the internal structures or the string metadata of
			// the key and the value.
			return uint64(len(item.Key) + len(item.Value))
		}),
	)

	cache.Set("first", "value1", ttlcache.DefaultTTL)
}
```

## Examples
See the [examples](https://github.com/jellydator/ttlcache/tree/v3/examples)
directory for complete applications demonstrating how to use `ttlcache`.

## Projects using TTLCache
Below is a list of some well-known projects that use `ttlcache`:
- [TiDB](https://github.com/pingcap/tidb): An open-source, cloud-native,
  distributed SQL database designed for high availability, scalability,
  and strong consistency.
- [HashiCorp Vault](https://github.com/hashicorp/vault): A tool for secrets
  management, encryption as a service, and privileged access management.
- [File Browser](https://github.com/filebrowser/filebrowser): A file
  managing interface that can be used to upload, delete, preview and
  edit files within a specified directory.
- [Tailscale](https://github.com/tailscale/tailscale): The easiest,
  most secure way to use WireGuard and 2FA.
- [authentik](https://github.com/goauthentik/authentik): An open-source
  Identity Provider (IdP) for modern SSO.
- [Navidrome](https://github.com/navidrome/navidrome): Your personal
  streaming service.
- [LiveKit](https://github.com/livekit/livekit): An end-to-end realtime
  stack for connecting humans and AI.
- [Owncast](https://github.com/owncast/owncast): A self-hosted live
  video streaming and chat server.
- [OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib):
  The contrib repository for the OpenTelemetry Collector.
- [Datadog Agent](https://github.com/DataDog/datadog-agent): The main
  repository for the Datadog Agent.
- [Erigon](https://github.com/erigontech/erigon): An Ethereum
  implementation on the efficiency frontier.
- [Microsoft Retina](https://github.com/microsoft/retina): An eBPF
  distributed networking observability tool for Kubernetes.
- [ByteDance Elkeid](https://github.com/bytedance/Elkeid): An open-source
  security solution for hosts, containers, K8s, and serverless workloads.
- [Polygon Bor](https://github.com/0xPolygon/bor): The official Go
  implementation of the Polygon blockchain.
- [Azure Service Operator](https://github.com/Azure/azure-service-operator):
  A Kubernetes operator that allows Azure resources to be created
  using kubectl.

...and [thousands more](https://github.com/jellydator/ttlcache/network/dependents).

## License
[MIT](LICENSE)

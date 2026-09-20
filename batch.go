package ttlcache

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// ContextLoader is an optional extension of Loader for loaders that
// support cancellation, can return an error, and allow the cache to
// manage the loaded item's insertion.
//
// Implementations should:
//   - Honor ctx only while awaiting their turn to execute. The ctx is
//     deliberately detached from the calling context (see
//     context.WithoutCancel), so canceling a single waiting request
//     never cancels a load that other callers are waiting for.
//   - Return the value associated with key, or the zero value and a
//     nil error when the item genuinely does not exist. The returned
//     ttl may be NoTTL, a specific duration, or DefaultTTL to use the
//     cache's default TTL.
//   - Return a non-nil error when the load fails; the error is
//     propagated to every waiting position and nothing is inserted.
//
// Unlike Load, LoadContext must not insert the item into the cache
// itself. When the generation guard is enabled (see
// WithGenerationGuard), the cache commits the returned value only if
// the generation is still current and the key was not Set, deleted,
// or evicted while the load was in-flight. A stale result is still
// returned to the callers that waited for it, but it is never written
// to the current cache.
//
// When the generation guard is disabled, the value is committed using
// the legacy last-write-wins semantics, i.e. a concurrent Set of the
// same key can be overwritten exactly as with the plain Loader
// interface.
type ContextLoader[K comparable, V any] interface {
	LoadContext(ctx context.Context, c *Cache[K, V], key K) (value V, ttl time.Duration, err error)
}

// ContextLoaderFunc adapts an ordinary function to the ContextLoader
// interface. It also satisfies the Loader interface, so it can be
// supplied wherever a Loader is accepted (Get, GetAndDelete,
// SuppressedLoader); its Load adapter does not insert the result.
type ContextLoaderFunc[K comparable, V any] func(ctx context.Context, c *Cache[K, V], key K) (V, time.Duration, error)

// LoadContext calls the wrapped function.
func (f ContextLoaderFunc[K, V]) LoadContext(ctx context.Context, c *Cache[K, V], key K) (V, time.Duration, error) {
	return f(ctx, c, key)
}

// Load executes the wrapped function and returns the item without
// inserting it. Cache commits always go through LoadContext; this
// adapter exists only so a ContextLoaderFunc can be supplied wherever
// a Loader is accepted (e.g. SuppressedLoader wrapping).
func (f ContextLoaderFunc[K, V]) Load(c *Cache[K, V], key K) *Item[K, V] {
	value, ttl, err := f(context.Background(), c, key)
	if err != nil || reflect.DeepEqual(value, *new(V)) {
		return nil
	}

	if ttl == DefaultTTL {
		ttl = c.options.ttl
	}

	return NewItemWithOpts(key, value, ttl, c.options.itemOpts...)
}

// GetResult is the outcome of a single GetMany entry.
//
// The three states are mutually exclusive:
//   - Hit or successful load: Item is non-nil, Err is nil.
//   - Miss: Item is nil and Err is nil. The key was absent and no
//     loader produced a value (no loader configured, the loader found
//     nothing, or a stale-generation result was discarded).
//   - Load error: Item is nil and Err is non-nil. This includes the
//     calling context's error when the context was canceled before a
//     new load could start.
type GetResult[K comparable, V any] struct {
	// Key is the key this result belongs to.
	Key K

	// Item holds the retrieved or loaded item. It is nil on misses
	// and load errors. When the generation guard discards a stale
	// load, Item still carries the loaded value as a transient item
	// that is not stored in the cache.
	Item *Item[K, V]

	// Err holds the loader error, or the context error when the
	// operation did not start because the context was canceled.
	Err error
}

// Hit reports whether the result contains an item.
func (r GetResult[K, V]) Hit() bool {
	return r.Item != nil
}

// getManyResult is the internal outcome of loading a unique key.
type getManyResult[K comparable, V any] struct {
	item *Item[K, V]
	err  error
}

// loadItem runs the configured loader for key without holding the
// items' mutex. The miss metric must already have been recorded by
// the caller.
//
// With a ContextLoader, the generation baseline is captured when the
// load starts and the cache commits the returned value, so the
// generation guard can apply. Legacy Loader implementations insert
// items themselves, preserving the historical behavior of Get.
func (c *Cache[K, V]) loadItem(ctx context.Context, key K, loader Loader[K, V]) (*Item[K, V], error) {
	if cl, ok := loader.(ContextLoader[K, V]); ok {
		if sl, ok := loader.(*SuppressedLoader[K, V]); ok {
			// SuppressedLoader captures the generation, loads, and
			// commits once inside singleflight, so all waiters share
			// that outcome (including a transient stale result)
			// without committing again.
			return sl.loadSuppressed(context.WithoutCancel(ctx), c, key)
		}

		return c.executeContextLoad(ctx, key, cl)
	}

	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return loader.Load(c, key), nil
}

// executeContextLoad captures the generation, runs a context-aware
// load without the cache mutex held, and commits its result through
// commitLoaded. It is also safe to invoke from inside a singleflight
// leader so that all waiters share one capture, one load, and one
// commit.
func (c *Cache[K, V]) executeContextLoad(ctx context.Context, key K, cl ContextLoader[K, V]) (*Item[K, V], error) {
	gen, baseline := c.captureGeneration(key)

	value, ttl, err := cl.LoadContext(context.WithoutCancel(ctx), c, key)
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(value, *new(V)) {
		return nil, nil
	}

	return c.commitLoaded(key, value, ttl, gen, baseline), nil
}

// executeSuppressedLoad runs the wrapped loader once for all
// singleflight waiters. ContextLoader implementations capture the
// generation and commit through the cache; plain Loader
// implementations execute their legacy self-inserting Load instead.
func (c *Cache[K, V]) executeSuppressedLoad(ctx context.Context, key K, loader Loader[K, V]) (*Item[K, V], error) {
	if cl, ok := loader.(ContextLoader[K, V]); ok {
		return c.executeContextLoad(ctx, key, cl)
	}

	return loader.Load(c, key), nil
}

// captureGeneration returns the current generation and the unexpired
// item currently stored under key (nil when absent or expired). It is
// invoked immediately before a load starts.
func (c *Cache[K, V]) captureGeneration(key K) (uint64, *Item[K, V]) {
	c.items.mu.RLock()
	defer c.items.mu.RUnlock()

	var baseline *Item[K, V]
	if elem, ok := c.items.values[key]; ok {
		item := elem.Value.(*Item[K, V])
		if !item.isExpiredUnsafe() {
			baseline = item
		}
	}

	return c.items.generation, baseline
}

// storedItem returns the unexpired item currently stored under key
// without touching it or updating metrics.
func (c *Cache[K, V]) storedItem(key K) *Item[K, V] {
	c.items.mu.RLock()
	defer c.items.mu.RUnlock()

	elem, ok := c.items.values[key]
	if !ok {
		return nil
	}

	item := elem.Value.(*Item[K, V])
	if item.isExpiredUnsafe() {
		return nil
	}

	return item
}

// commitLoaded inserts a loaded value while the load is still current.
// When the generation advanced, or the key was Set, deleted, or
// evicted while the load was in-flight, the value is returned as a
// transient item that is not stored in the cache.
func (c *Cache[K, V]) commitLoaded(key K, value V, ttl time.Duration, gen uint64, baseline *Item[K, V]) *Item[K, V] {
	c.items.mu.Lock()

	stale := c.options.enableGenerationGuard && c.items.generation != gen

	// Within a current generation, ensure a concurrent Set/Delete is
	// not clobbered by a finishing load. Without the guard, the legacy
	// last-write-wins behavior applies unconditionally.
	if c.options.enableGenerationGuard && !stale {
		var current *Item[K, V]
		if elem, ok := c.items.values[key]; ok {
			current = elem.Value.(*Item[K, V])
		}

		stale = current != baseline
	}

	if stale {
		c.items.mu.Unlock()

		// Hand the value to the original waiters without mutating
		// the cache. Item options (e.g. cost functions) are still
		// applied, but the item is never linked into the LRU list or
		// the expiration queue.
		resolvedTTL := ttl
		if resolvedTTL == DefaultTTL {
			resolvedTTL = c.options.ttl
		}

		return NewItemWithOpts(key, value, resolvedTTL, c.options.itemOpts...)
	}

	item := c.set(key, value, ttl)
	c.items.mu.Unlock()

	return item
}

// GetMany retrieves multiple keys in one call.
//
// The opts slice provides per-entry options and may be nil when no
// entry overrides anything; otherwise it must have the same length as
// keys. As with Get, only the loader (WithLoader) and touch
// (WithDisableTouchOnHit) options are honored. Repeated keys use the
// options of their first occurrence.
//
// Lookup semantics are a point-in-time snapshot: the cache is locked
// once while every distinct key is looked up, touched, and counted as
// a hit or miss, in the order of first occurrence. Repeated keys
// perform exactly one lookup and at most one load, and the same result
// is copied to every position. Everything decided by load completions,
// i.e. insertion/update/eviction events and their order, and capacity
// or cost eviction, is determined as loads finish, exactly as if the
// corresponding Get calls completed in that order.
//
// At most maxConcurrency distinct missing keys are loaded at once; a
// value of 0 or less means unlimited. When ctx is canceled, no new
// loads start and their entries report the context error. Loads that
// already started always run to completion; a load shared with other
// callers is not terminated just because one waiter leaves.
func (c *Cache[K, V]) GetMany(ctx context.Context, keys []K, opts [][]Option[K, V], maxConcurrency int) []GetResult[K, V] {
	if opts != nil && len(opts) != len(keys) {
		panic("ttlcache: GetMany options length does not match keys length")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	results := make([]GetResult[K, V], len(keys))

	type unique struct {
		key       K
		loader    Loader[K, V]
		positions []int
	}

	uniques := make([]unique, 0, len(keys))
	uniqueIndex := make(map[K]int, len(keys))

	// Snapshot phase: distinct keys are looked up under a single lock
	// acquisition, in first-occurrence order.
	var hits, misses uint64

	c.items.mu.Lock()
	for i, key := range keys {
		if _, ok := uniqueIndex[key]; ok {
			continue
		}

		getOpts := options[K, V]{
			loader:            c.options.loader,
			disableTouchOnHit: c.options.disableTouchOnHit,
		}
		if opts != nil {
			getOpts = applyOptions(getOpts, opts[i]...)
		}

		elem := c.get(key, !getOpts.disableTouchOnHit, false)

		idx := len(uniques)
		uniqueIndex[key] = idx
		uniques = append(uniques, unique{key: key, loader: getOpts.loader})

		results[i] = GetResult[K, V]{Key: key}
		if elem != nil {
			results[i].Item = elem.Value.(*Item[K, V])
			hits++
		} else {
			misses++
		}
	}
	c.items.mu.Unlock()

	c.metricsMu.Lock()
	c.metrics.Hits += hits
	c.metrics.Misses += misses
	c.metricsMu.Unlock()

	// Map every position to its unique entry and propagate snapshot
	// hit results to repeated positions.
	for i, key := range keys {
		idx := uniqueIndex[key]
		uniques[idx].positions = append(uniques[idx].positions, i)
	}
	for i, key := range keys {
		idx := uniqueIndex[key]
		first := uniques[idx].positions[0]
		if i != first && results[first].Item != nil {
			results[i] = results[first]
		}
	}

	// Load phase: distinct misses are loaded concurrently, bounded by
	// maxConcurrency. The loader runs without the cache mutex held.
	var (
		wg     sync.WaitGroup
		sem    chan struct{}
		loaded = make([]getManyResult[K, V], len(uniques))
	)

	if maxConcurrency > 0 {
		sem = make(chan struct{}, maxConcurrency)
	}

	for idx := range uniques {
		first := uniques[idx].positions[0]
		if results[first].Item != nil || uniques[idx].loader == nil {
			continue
		}

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			if err := ctx.Err(); err != nil {
				loaded[idx] = getManyResult[K, V]{err: err}
				return
			}

			if sem != nil {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					loaded[idx] = getManyResult[K, V]{err: ctx.Err()}
					return
				}
				defer func() { <-sem }()
			}

			item, err := c.loadItem(ctx, uniques[idx].key, uniques[idx].loader)
			loaded[idx] = getManyResult[K, V]{item: item, err: err}
		}(idx)
	}

	wg.Wait()

	// Copy each unique result to all of its positions.
	for idx := range uniques {
		first := uniques[idx].positions[0]
		if results[first].Item != nil {
			continue
		}

		res := GetResult[K, V]{
			Key:  uniques[idx].key,
			Item: loaded[idx].item,
			Err:  loaded[idx].err,
		}
		for _, pos := range uniques[idx].positions {
			results[pos] = res
		}
	}

	return results
}

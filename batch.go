package ttlcache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// ErrNotFound is a sentinel error that may be returned by a ValueLoader
// to indicate that no value is associated with the requested key. It is
// equivalent to a cache miss: the GetMany result has neither an item nor
// an error.
var ErrNotFound = errors.New("ttlcache: item not found")

// ValueLoader is a context-aware variant of the Loader interface.
//
// Unlike Loader, whose implementations are responsible for inserting
// the loaded value into the cache themselves, a ValueLoader only returns
// the loaded value together with its TTL; the cache performs the
// insertion. This allows the cache to apply the generation guard and to
// avoid overwriting values that were set after the load had started.
//
// A returned ttl of DefaultTTL (0) makes the item use the cache-wide
// default TTL. NoTTL (-1) makes the item never expire.
// Returning ErrNotFound indicates that the item does not exist and is
// reported as a miss. Any other error is reported in the GetMany result.
//
// The provided context is canceled when all callers waiting for the
// load abandon it. Implementations are encouraged to respect the
// context, but even when it is ignored the cache will not insert a
// stale result.
//
// An implementation must not start another load for the same key (e.g.
// by calling Get or GetMany with a loader for that key), just like a
// suppressed Loader must not re-enter a load for the same key.
type ValueLoader[K comparable, V any] interface {
	Load(ctx context.Context, c *Cache[K, V], key K) (value V, ttl time.Duration, err error)
}

// ValueLoaderFunc is an adapter that allows ordinary functions to be
// used as ValueLoaders.
type ValueLoaderFunc[K comparable, V any] func(context.Context, *Cache[K, V], K) (V, time.Duration, error)

// Load calls the wrapped function.
func (fn ValueLoaderFunc[K, V]) Load(ctx context.Context, c *Cache[K, V], key K) (V, time.Duration, error) {
	return fn(ctx, c, key)
}

// GetResult represents the outcome of a GetMany request for a single
// key.
type GetResult[K comparable, V any] struct {
	// Key is the key that was requested.
	Key K

	// Item is the retrieved item. It is nil on a miss or when Err is
	// non-nil.
	Item *Item[K, V]

	// Err holds the error returned by the ValueLoader. It is nil on
	// hits, misses, and requests served with a plain Loader.
	Err error
}

// Miss reports whether the key was not found and no value was loaded.
// A canceled request or a failed load is not a miss.
func (r GetResult[K, V]) Miss() bool {
	return r.Err == nil && r.Item == nil
}

// GetManyOptions holds optional parameters of the GetMany method.
type GetManyOptions[K comparable, V any] struct {
	// Options holds per-key options, indexed the same way as the keys
	// argument passed to GetMany. A nil entry or an entry missing for a
	// position means that the cache-wide defaults are used. When a key
	// appears multiple times, the options of its first occurrence are
	// used for the single lookup/load.
	Options [][]Option[K, V]

	// MaxConcurrency limits the number of loader executions that may be
	// started concurrently by this GetMany call. Joining an already
	// in-flight load (started by another caller, or for a duplicate key)
	// does not consume a slot. A value of 0 or lower means no limit.
	MaxConcurrency int
}

// missingLoad describes the single lookup/load performed for a unique
// key within a GetMany call.
type missingLoad[K comparable, V any] struct {
	getOpts  options[K, V]
	snapshot *list.Element // element present at lookup time, may be expired
	expired  bool          // snapshot pointed to an expired item
	item     *Item[K, V]   // resolved item
	err      error         // resolved error
	hit      bool
}

// GetMany retrieves items for the provided, ordered keys.
//
// Each unique key is looked up at most once and loaded at most once;
// the result is copied to every position that requested the key. The
// returned slice always has the same length and order as the input.
//
// Hit/miss determination, touches and hit/miss metrics are based on a
// snapshot taken when each unique key is first examined, in the order of
// first occurrence. Loaded values, load errors, evictions caused by the
// insertion, and insertion/update metrics are determined when the load
// completes. The relative order of observable effects for different
// keys is unspecified because loads run concurrently.
//
// When the context is canceled, no new loads are started and
// not-yet-resolved results report the context error. A load that is
// shared with other callers is never canceled because a single waiter
// gave up; it is canceled only when every waiter has abandoned it.
//
// A ValueLoader (see WithValueLoader) is required for the generation
// guard and for load errors to be surfaced. A plain Loader keeps the
// legacy semantics, including when the generation guard is enabled:
// the loader itself is responsible for inserting the value, so its
// result may be written back regardless of generations or concurrent
// sets. The single-key Get method is not affected by GetMany and keeps
// its existing behavior.
func (c *Cache[K, V]) GetMany(ctx context.Context, keys []K, opts *GetManyOptions[K, V]) []GetResult[K, V] {
	results := make([]GetResult[K, V], len(keys))
	if len(keys) == 0 {
		return results
	}

	var (
		perKeyOpts [][]Option[K, V]
		maxConc    int
	)
	if opts != nil {
		perKeyOpts = opts.Options
		maxConc = opts.MaxConcurrency
	}

	unique := make(map[K]*missingLoad[K, V])
	order := make([]K, 0, len(keys))

	for i, key := range keys {
		results[i].Key = key

		p, ok := unique[key]
		if ok {
			continue
		}

		getOpts := options[K, V]{
			loader:            c.options.loader,
			valueLoader:       c.options.valueLoader,
			disableTouchOnHit: c.options.disableTouchOnHit,
		}
		if i < len(perKeyOpts) {
			getOpts = applyOptions(getOpts, perKeyOpts[i]...)
		}

		p = &missingLoad[K, V]{getOpts: getOpts}

		// Phase 1: snapshot lookup. Locks are taken only around the map
		// operations, never around a loader execution.
		c.items.mu.Lock()
		elem := c.get(key, !getOpts.disableTouchOnHit, false)
		if elem != nil {
			p.item = elem.Value.(*Item[K, V])
			p.hit = true
		} else {
			p.snapshot = c.items.values[key]
			if p.snapshot != nil {
				p.expired = p.snapshot.Value.(*Item[K, V]).isExpiredUnsafe()
			}
		}
		c.items.mu.Unlock()

		c.metricsMu.Lock()
		if p.hit {
			c.metrics.Hits++
		} else {
			c.metrics.Misses++
		}
		c.metricsMu.Unlock()

		unique[key] = p
		order = append(order, key)
	}

	// Phase 2: load the missing unique keys, bounded by MaxConcurrency.
	var sem chan struct{}
	if maxConc > 0 {
		sem = make(chan struct{}, maxConc)
	}

	missing := make([]K, 0, len(order))
	for _, key := range order {
		if !unique[key].hit {
			missing = append(missing, key)
		}
	}

	// Workers acquire a concurrency slot before joining/starting a load,
	// so a request that is canceled while queued neither starts nor
	// registers a new in-flight load. Joining an existing load does not
	// hold a slot for its whole duration: the slot is released right
	// after the load has been registered and the worker waits as a
	// follower.
	workers := maxConc
	if workers <= 0 || workers > len(missing) {
		workers = len(missing)
	}

	work := make(chan K)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range work {
				p := unique[key]

				if sem != nil {
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						p.err = ctx.Err()
						continue
					}
				}

				c.loadMissing(ctx, key, p, sem)
			}
		}()
	}

	dispatched := make(map[K]bool, len(missing))
feed:
	for _, key := range missing {
		select {
		case work <- key:
			dispatched[key] = true
		case <-ctx.Done():
			// No further loads are started; resolve every key that was
			// not dispatched to a worker with the cancellation error.
			for _, k := range missing {
				if !dispatched[k] {
					unique[k].err = ctx.Err()
				}
			}
			break feed
		}
	}
	close(work)
	wg.Wait()

	// Copy the shared result back to every position.
	for i, key := range keys {
		p := unique[key]
		results[i].Item = p.item
		results[i].Err = p.err
	}

	return results
}

// loadMissing joins an in-flight load of the key or starts one. Only the
// leader executes the loader; followers wait for the shared result.
// The caller holds a concurrency slot (sem) which is released when the
// loader execution finishes, or immediately for followers.
func (c *Cache[K, V]) loadMissing(ctx context.Context, key K, p *missingLoad[K, V], sem chan struct{}) {
	call, loadCtx, leader := c.loads.begin(c, key)
	if !leader {
		// Joining a shared in-flight load does not hold a concurrency
		// slot for the duration of the wait.
		if sem != nil {
			<-sem
		}
		p.item, p.err = call.await(&c.loads, ctx)
		return
	}

	releaseSlot := func() {
		if sem != nil {
			<-sem
		}
	}

	// evaluateCancellation is called before the loader starts. When the
	// caller is done, the load is canceled unless another waiter keeps
	// it alive; either way the caller's own result is the context error.
	evaluateCancellation := func() error {
		if ctx.Err() == nil {
			return nil
		}

		c.loads.mu.Lock()
		call.leaderCanceled = true
		if call.waiters == 1 {
			call.cancel()
		}
		c.loads.mu.Unlock()

		return ctx.Err()
	}

	if err := evaluateCancellation(); err != nil {
		c.loads.finish(key, call, nil, err)
		p.err = err
		return
	}

	// While the loader executes, canceling the last waiter cancels the
	// load context. Loaders that ignore the context keep running but
	// their result is still handled below.
	stopWatch := context.AfterFunc(ctx, func() {
		c.loads.mu.Lock()
		call.leaderCanceled = true
		if call.waiters == 1 {
			call.cancel()
		}
		c.loads.mu.Unlock()
	})

	var (
		item *Item[K, V]
		err  error
	)

	item, err = c.runLoad(ctx, loadCtx, call, key, p)
	releaseSlot()
	stopWatch()

	c.loads.finish(key, call, item, err)

	// When the caller gave up while the loader was executing, its own
	// result reports the cancellation even if the load itself was
	// allowed to finish.
	if ctxErr := ctx.Err(); ctxErr != nil {
		p.err = ctxErr
		return
	}

	p.item = item
	p.err = err
}

// runLoad executes the configured loader for the key and, when possible,
// inserts the produced value into the cache.
func (c *Cache[K, V]) runLoad(ctx, loadCtx context.Context, call *loadCall[K, V], key K, p *missingLoad[K, V]) (*Item[K, V], error) {
	if vl := p.getOpts.valueLoader; vl != nil {
		value, ttl, err := vl.Load(loadCtx, c, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, nil
			}

			return nil, err
		}

		// Make the cancellation decision synchronously after the loader
		// returns: the watcher's cancel(loadCtx) may not have been
		// observed yet even when ctx is already done.
		if ctx.Err() != nil {
			c.loads.mu.Lock()
			call.leaderCanceled = true
			if call.waiters == 1 {
				call.cancel()
			}
			c.loads.mu.Unlock()
		}

		return c.insertLoaded(key, value, ttl, loadCtx, call, p.snapshot, p.expired), nil
	}

	// Legacy path: the Loader is fully responsible for inserting the
	// value into the cache, exactly as with the single-key Get. The
	// context and the generation guard cannot be applied here.
	if l := p.getOpts.loader; l != nil {
		return l.Load(c, key), nil
	}

	return nil, nil
}

// insertLoaded stores a value loaded by a ValueLoader unless the load
// has gone stale: the generation advanced, every waiter canceled, or the
// key was concurrently set/deleted. A stale value is returned as a
// detached item that is not part of the cache.
func (c *Cache[K, V]) insertLoaded(key K, value V, ttl time.Duration, loadCtx context.Context, call *loadCall[K, V], snapshot *list.Element, snapshotExpired bool) *Item[K, V] {
	detached := func() *Item[K, V] {
		itemTTL := ttl
		if itemTTL == DefaultTTL || itemTTL == PreviousOrDefaultTTL {
			itemTTL = c.options.ttl
		}

		return NewItemWithOpts(key, value, itemTTL, c.options.itemOpts...)
	}

	c.loads.mu.Lock()
	followers := call.waiters - 1
	c.loads.mu.Unlock()

	if loadCtx.Err() != nil && followers == 0 {
		return detached()
	}

	if c.options.generationGuard && c.gen.Load() != call.startGen {
		return detached()
	}

	c.items.mu.Lock()
	defer c.items.mu.Unlock()

	if c.options.generationGuard {
		current := c.items.values[key]
		if snapshotExpired {
			// The entry was already expired at lookup time. Updating it
			// in place mirrors what a single-key Get load does; a newer
			// Set would have replaced the element.
			if current != snapshot {
				return detached()
			}
		} else if current != nil {
			// The key was absent at lookup time; any concurrent value
			// wins over the loaded one.
			return detached()
		}
	}

	return c.set(key, value, ttl)
}

// loadCall represents a single in-flight load shared by one or more
// callers.
type loadCall[K comparable, V any] struct {
	done           chan struct{}
	cancel         context.CancelFunc
	item           *Item[K, V]
	err            error
	waiters        int  // leader plus followers
	leaderCanceled bool // leader's own context was canceled
	startGen       uint64
}

// await waits for the shared result. When the provided context is
// canceled first, the caller abandons the load; the load itself is
// canceled only when no other waiter remains.
func (call *loadCall[K, V]) await(g *loadGroup[K, V], ctx context.Context) (*Item[K, V], error) {
	abandon := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		call.waiters--
		if call.waiters == 1 && call.leaderCanceled {
			call.cancel()
		}
		g.mu.Unlock()
	})

	select {
	case <-call.done:
		// If our context fired concurrently, report the cancellation;
		// the AfterFunc unregisters this waiter either way.
		select {
		case <-ctx.Done():
			abandon()
			return nil, ctx.Err()
		default:
		}
		abandon()
		return call.item, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// loadGroup coordinates in-flight loads across GetMany calls. Its mutex
// is never held while a loader executes.
type loadGroup[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*loadCall[K, V]
}

// begin joins an existing load of the key or starts a new one. The
// returned context is valid only for the leader and is canceled when the
// load should be abandoned.
func (g *loadGroup[K, V]) begin(c *Cache[K, V], key K) (*loadCall[K, V], context.Context, bool) {
	loadCtx, cancel := context.WithCancel(context.Background())

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.m == nil {
		g.m = make(map[K]*loadCall[K, V])
	}

	if call, ok := g.m[key]; ok {
		call.waiters++
		cancel() // unused leader context
		return call, nil, false
	}

	call := &loadCall[K, V]{
		done:     make(chan struct{}),
		cancel:   cancel,
		waiters:  1,
		startGen: c.gen.Load(),
	}
	g.m[key] = call

	return call, loadCtx, true
}

// finish publishes the result, unblocks all waiters, and unregisters the
// load.
func (g *loadGroup[K, V]) finish(key K, call *loadCall[K, V], item *Item[K, V], err error) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	call.item = item
	call.err = err
	close(call.done)
}

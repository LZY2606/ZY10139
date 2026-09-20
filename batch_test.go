package ttlcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keyGate coordinates a single key's load: the load enters via enter()
// (which signals waiters) and blocks until release() is invoked.
type keyGate struct {
	mu       sync.Mutex
	cond     *sync.Cond
	started  bool
	released bool
}

func newKeyGate() *keyGate {
	g := &keyGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// enter marks the load as started and blocks until release.
func (g *keyGate) enter() {
	g.mu.Lock()
	g.started = true
	g.cond.Broadcast()
	for !g.released {
		g.cond.Wait()
	}
	g.mu.Unlock()
}

// release unblocks an in-flight load and marks the gate released so
// later loads do not block either.
func (g *keyGate) release() {
	g.mu.Lock()
	g.released = true
	g.cond.Broadcast()
	g.mu.Unlock()
}

// waitStarted blocks until the load starts.
func (g *keyGate) waitStarted() {
	g.mu.Lock()
	for !g.started {
		g.cond.Wait()
	}
	g.mu.Unlock()
}

// isStarted reports whether the load entered without blocking.
func (g *keyGate) isStarted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.started
}

// gateLoader is a context-aware loader whose per-key loads block on a
// keyGate. Tests coordinate the load lifecycle with explicit barriers
// instead of sleeps: waitStarted parks until a load enters, and
// releaseKey lets it finish.
type gateLoader struct {
	mu        sync.Mutex
	enterCond *sync.Cond
	calls     map[string]int
	gates     map[string]*keyGate
	values    map[string]string
	errors    map[string]error
	counter   atomic.Int64
}

func newGateLoader() *gateLoader {
	g := &gateLoader{
		calls:  make(map[string]int),
		gates:  make(map[string]*keyGate),
		values: make(map[string]string),
		errors: make(map[string]error),
	}
	g.enterCond = sync.NewCond(&g.mu)

	return g
}

func (g *gateLoader) getOrCreateGate(key string) *keyGate {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.getOrCreateGateLocked(key)
}

func (g *gateLoader) getOrCreateGateLocked(key string) *keyGate {
	gate := g.gates[key]
	if gate == nil {
		gate = newKeyGate()
		g.gates[key] = gate
	}

	return gate
}

func (g *gateLoader) releaseKey(key string) {
	g.getOrCreateGate(key).release()
}

func (g *gateLoader) waitStarted(key string) {
	g.getOrCreateGate(key).waitStarted()
}

func (g *gateLoader) assertNotStarted(t *testing.T, key string) {
	t.Helper()

	if g.getOrCreateGate(key).isStarted() {
		t.Fatalf("load of %s unexpectedly started", key)
	}
}

func (g *gateLoader) callCount(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.calls[key]
}

// startedKeys returns the keys whose load has entered.
func (g *gateLoader) startedKeys() []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	keys := make([]string, 0)
	for key := range g.gates {
		if g.calls[key] > 0 {
			keys = append(keys, key)
		}
	}

	return keys
}

// waitStartedCount blocks until at least n distinct loads entered.
func (g *gateLoader) waitStartedCount(n int) {
	g.mu.Lock()
	for len(g.calls) < n {
		g.enterCond.Wait()
	}
	g.mu.Unlock()
}

func (g *gateLoader) LoadContext(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
	g.counter.Add(1)

	g.mu.Lock()
	g.calls[key]++
	gate := g.getOrCreateGateLocked(key)
	value, hasValue := g.values[key]
	loadErr := g.errors[key]
	g.enterCond.Broadcast()
	g.mu.Unlock()

	if loadErr != nil {
		return "", DefaultTTL, loadErr
	}

	gate.enter()

	if !hasValue {
		value = "value-" + key
	}

	return value, DefaultTTL, nil
}

// Load makes gateLoader satisfy the Loader interface. Tests use the
// context-aware path, so it is never invoked.
func (g *gateLoader) Load(_ *Cache[string, string], _ string) *Item[string, string] {
	panic("gateLoader.Load should not be called")
}

func Test_GetMany_EmptyInput(t *testing.T) {
	cache := New[string, string]()

	res := cache.GetMany(context.Background(), nil, nil, 1)
	assert.Empty(t, res)
}

func Test_GetMany_OptionsLengthMismatchPanics(t *testing.T) {
	cache := New[string, string]()

	require.NotPanics(t, func() {
		cache.GetMany(context.Background(), []string{"a"}, nil, 0) // nil opts allowed
	})

	assert.Panics(t, func() {
		cache.GetMany(context.Background(), []string{"a", "b"}, [][]Option[string, string]{
			{WithDisableTouchOnHit[string, string]()},
		}, 0)
	})
}

func Test_GetMany_HitsAndMissesSnapshot(t *testing.T) {
	cache := New[string, string](WithTTL[string, string](time.Hour))
	cache.Set("a", "1", DefaultTTL)
	cache.Set("b", "2", DefaultTTL)

	results := cache.GetMany(context.Background(), []string{"a", "missing", "b"}, nil, 4)

	require.Len(t, results, 3)
	assert.True(t, results[0].Hit())
	assert.Equal(t, "1", results[0].Item.Value())
	assert.False(t, results[1].Hit())
	assert.Nil(t, results[1].Err)
	assert.True(t, results[2].Hit())
	assert.Equal(t, "2", results[2].Item.Value())

	assert.Equal(t, Metrics{Insertions: 2, Hits: 2, Misses: 1}, cache.Metrics())
}

func Test_GetMany_DuplicateKeyLookupOnce(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithTTL[string, string](time.Hour),
		WithLoader[string, string](loader),
	)
	cache.Set("hit", "cached", DefaultTTL)

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"hit", "miss", "hit", "miss"}, nil, 4)
	}()

	loader.waitStarted("miss")
	assert.Equal(t, 1, loader.callCount("miss"))
	loader.releaseKey("miss")

	results := <-done
	require.Len(t, results, 4)

	for i, key := range []string{"hit", "miss", "hit", "miss"} {
		assert.Equal(t, key, results[i].Key)
		require.NotNil(t, results[i].Item, "position %d", i)
	}
	assert.Equal(t, "cached", results[0].Item.Value())
	assert.Equal(t, "cached", results[2].Item.Value())
	assert.Same(t, results[0].Item, results[2].Item)
	assert.Equal(t, "value-miss", results[1].Item.Value())
	assert.Same(t, results[1].Item, results[3].Item)
	assert.Equal(t, 1, loader.callCount("miss"))

	// One hit and one miss for the unique keys.
	assert.Equal(t, Metrics{Hits: 1, Misses: 1, Insertions: 2}, cache.Metrics())
}

func Test_GetMany_DuplicateKeyFirstOptionsWin(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithTTL[string, string](time.Hour),
		WithLoader[string, string](loader),
	)
	cache.Set("k", "cached", time.Hour)

	oldExpiresAt := cache.storedItem("k").expiresAt

	opts := [][]Option[string, string]{
		{WithDisableTouchOnHit[string, string]()},
		{}, // repeated position requests touch; must be ignored
	}
	results := cache.GetMany(context.Background(), []string{"k", "k"}, opts, 1)
	assert.True(t, results[0].Hit())
	assert.Same(t, results[0].Item, results[1].Item)
	assert.Equal(t, oldExpiresAt, results[0].Item.expiresAt)
	assert.Equal(t, Metrics{Hits: 1, Insertions: 1}, cache.Metrics())
}

func Test_GetMany_TouchAndLRUOrder(t *testing.T) {
	cache := New[string, string](WithTTL[string, string](time.Hour))
	cache.Set("a", "1", DefaultTTL)
	cache.Set("b", "2", DefaultTTL)

	// Snapshot lookup order for distinct keys matches first occurrence,
	// and each touched item moves to the LRU front. The last touched
	// distinct key ("a") ends up at the front.
	cache.GetMany(context.Background(), []string{"b", "a", "b"}, nil, 1)

	assert.Equal(t, "a", cache.items.lru.Front().Value.(*Item[string, string]).key)
	assert.Equal(t, Metrics{Hits: 2, Insertions: 2}, cache.Metrics())
}

func Test_GetMany_RespectsMaxConcurrency(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithLoader[string, string](loader),
	)

	done := make(chan struct{})
	go func() {
		cache.GetMany(context.Background(), []string{"k1", "k2", "k3", "k4"}, nil, 2)
		close(done)
	}()

	// Wait until exactly two distinct loads are in-flight, without
	// assuming which keys the scheduler picked first.
	loader.waitStartedCount(2)
	assert.Len(t, loader.startedKeys(), 2)

	// Free one in-flight load; a queued load must take its slot,
	// keeping the number of started keys at three.
	first := loader.startedKeys()[0]
	loader.releaseKey(first)
	loader.waitStartedCount(3)
	assert.Len(t, loader.startedKeys(), 3)

	// Free the second original load and let the last queued load run.
	for _, key := range loader.startedKeys() {
		loader.releaseKey(key)
	}
	loader.waitStartedCount(4)
	for _, key := range loader.startedKeys() {
		loader.releaseKey(key)
	}

	<-done
	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		assert.Equal(t, 1, loader.callCount(key))
	}
}

func Test_GetMany_CancelStopsQueuedLoads(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithLoader[string, string](loader),
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(ctx, []string{"k1", "k2", "k3"}, nil, 1)
	}()

	loader.waitStartedCount(1)
	inFlight := loader.startedKeys()[0]

	// The other two keys are queued. Canceling must not start them.
	cancel()
	loader.releaseKey(inFlight)

	results := <-done
	require.Len(t, results, 3)
	var loaded, canceled int
	for i, r := range results {
		if r.Key == inFlight {
			require.True(t, r.Hit(), "in-flight key %s should complete", r.Key)
			assert.Equal(t, "value-"+r.Key, r.Item.Value())
			loaded++
		} else {
			assert.False(t, r.Hit(), "position %d (%s)", i, r.Key)
			assert.ErrorIs(t, r.Err, context.Canceled)
			canceled++
		}
	}
	assert.Equal(t, 1, loaded)
	assert.Equal(t, 2, canceled)

	assert.Equal(t, 1, loader.callCount(inFlight))

	// The started load committed normally even though the caller left,
	// while queued loads never ran.
	assert.Equal(t, "value-"+inFlight, cache.storedItem(inFlight).Value())
}

func Test_GetMany_CanceledBeforeAnyLoad(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithLoader[string, string](loader),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := cache.GetMany(ctx, []string{"k1", "k2"}, nil, 1)
	require.Len(t, results, 2)
	for _, r := range results {
		assert.False(t, r.Hit())
		assert.ErrorIs(t, r.Err, context.Canceled)
	}
	assert.Equal(t, 0, loader.callCount("k1"))
}

func Test_GetMany_SharedLoadSurvivesCanceledWaiter(t *testing.T) {
	// This property is verified with SuppressedLoader backed by the
	// real singleflight group in
	// Test_SuppressedLoader_GetManySharesSingleLoad, which uses the
	// counting group to deterministically park a follower. Here we
	// additionally verify a canceled waiting context does not abort
	// the shared load: the loader ctx is detached from callers.
	loader := newGateLoader()
	cache := New[string, string](
		WithLoader[string, string](loader),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(ctx, []string{"k"}, nil, 1)
	}()
	loader.waitStarted("k")

	cancel() // cancel while the load is in-flight
	loader.releaseKey("k")

	res := <-done
	require.True(t, res[0].Hit())
	assert.Equal(t, "value-k", res[0].Item.Value())
	assert.Equal(t, 1, loader.callCount("k"))
	assert.Equal(t, "value-k", cache.storedItem("k").Value())
}

func Test_GetMany_LoadError(t *testing.T) {
	loader := newGateLoader()
	loadErr := errors.New("boom")
	loader.errors["bad"] = loadErr
	cache := New[string, string](
		WithLoader[string, string](loader),
	)

	loader.releaseKey("good")
	results := cache.GetMany(context.Background(), []string{"bad", "bad", "good"}, nil, 2)
	require.Len(t, results, 3)
	assert.Same(t, loadErr, results[0].Err)
	assert.Same(t, loadErr, results[1].Err)
	assert.False(t, results[0].Hit())
	assert.True(t, results[2].Hit())
	assert.Nil(t, cache.storedItem("bad"))
	assert.Equal(t, "value-good", cache.storedItem("good").Value())
	assert.Equal(t, Metrics{Misses: 2, Insertions: 1}, cache.Metrics())
}

func Test_GetMany_LoaderReturnsZeroValueIsMiss(t *testing.T) {
	cache := New[string, string](
		WithLoader[string, string](ContextLoaderFunc[string, string](
			func(_ context.Context, _ *Cache[string, string], _ string) (string, time.Duration, error) {
				return "", DefaultTTL, nil
			},
		)),
	)

	results := cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	assert.False(t, results[0].Hit())
	assert.Nil(t, results[0].Err)
	assert.Nil(t, cache.storedItem("k"))
}

func Test_GetMany_DeleteAllDuringSlowLoadDiscardsResult(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithLoader[string, string](loader),
	)

	var (
		inserted []string
		mu       sync.Mutex
	)
	cache.OnInsertion(func(_ context.Context, item *Item[string, string]) {
		mu.Lock()
		inserted = append(inserted, item.Key())
		mu.Unlock()
	})

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"old"}, nil, 1)
	}()

	loader.waitStarted("old")
	require.Equal(t, uint64(0), cache.Generation())

	cache.DeleteAll()
	require.Equal(t, uint64(1), cache.Generation())

	loader.releaseKey("old")
	results := <-done

	// The original waiters still receive the value...
	require.True(t, results[0].Hit())
	assert.Equal(t, "value-old", results[0].Item.Value())

	// ...but it must never enter the new generation.
	assert.Nil(t, cache.storedItem("old"))
	assert.Equal(t, uint64(0), cache.Metrics().Insertions)

	// The cache accepts fresh content after the wipe.
	cache.Set("new", "fresh", DefaultTTL)
	assert.Equal(t, "fresh", cache.storedItem("new").Value())

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(inserted) == 1
	}, time.Second, time.Millisecond)

	mu.Lock()
	assert.Equal(t, []string{"new"}, inserted)
	mu.Unlock()
}

func Test_GetMany_SetPreemptsSameGenerationLoad(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithTTL[string, string](time.Hour),
		WithLoader[string, string](loader),
	)

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()

	loader.waitStarted("k")

	// A concurrent Set wins the key while the slow load is in-flight.
	cache.Set("k", "manual", DefaultTTL)

	loader.releaseKey("k")
	results := <-done

	require.True(t, results[0].Hit())
	assert.Equal(t, "value-k", results[0].Item.Value())

	// The committed item is the manual one; the stale load did not
	// overwrite it and produced no insertion/update event.
	stored := cache.storedItem("k")
	assert.Equal(t, "manual", stored.Value())
	assert.Equal(t, Metrics{Misses: 1, Insertions: 1}, cache.Metrics())
}

func Test_GetMany_ReinsertsAfterConcurrentSetAndDelete(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithLoader[string, string](loader),
	)

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()

	loader.waitStarted("k")

	// Another actor transiently populates and then removes the key.
	// The final state (absent) matches the captured baseline
	// (absent), so the finishing load is still allowed to commit.
	cache.Set("k", "interim", DefaultTTL)
	cache.Delete("k")

	loader.releaseKey("k")
	results := <-done
	assert.True(t, results[0].Hit())
	assert.Equal(t, "value-k", cache.storedItem("k").Value())
}

func Test_GetMany_ResetGeneration(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithLoader[string, string](loader),
	)
	cache.Set("keep", "me", DefaultTTL)

	// Explicit reset without deleting items.
	cache.ResetGeneration()
	assert.Equal(t, uint64(1), cache.Generation())
	assert.Equal(t, "me", cache.storedItem("keep").Value())

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()
	loader.waitStarted("k")
	cache.ResetGeneration()
	loader.releaseKey("k")

	results := <-done
	assert.True(t, results[0].Hit())
	assert.Nil(t, cache.storedItem("k"))
	assert.Equal(t, "me", cache.storedItem("keep").Value())
}

func Test_GetMany_CapacityEvictionInCompletionOrder(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithCapacity[string, string](2),
		WithLoader[string, string](loader),
	)

	evictedCh := make(chan string, 4)
	cache.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		if reason == EvictionReasonCapacityReached {
			evictedCh <- item.Key()
		}
	})

	done := make(chan struct{})
	go func() {
		cache.GetMany(context.Background(), []string{"a", "b", "c"}, nil, 3)
		close(done)
	}()

	// All three loads are in-flight concurrently.
	loader.waitStarted("a")
	loader.waitStarted("b")
	loader.waitStarted("c")

	// Finish them in a fixed order: a, b commit first (cache fills to
	// capacity), then c evicts the LRU back, which is "a".
	loader.releaseKey("a")
	assert.Eventually(t, func() bool { return cache.storedItem("a") != nil }, time.Second, time.Millisecond)
	loader.releaseKey("b")
	assert.Eventually(t, func() bool { return cache.storedItem("b") != nil }, time.Second, time.Millisecond)
	loader.releaseKey("c")
	<-done

	assert.Nil(t, cache.storedItem("a"))
	assert.Equal(t, "value-b", cache.storedItem("b").Value())
	assert.Equal(t, "value-c", cache.storedItem("c").Value())

	select {
	case key := <-evictedCh:
		assert.Equal(t, "a", key)
	case <-time.After(time.Second):
		t.Fatal("expected capacity eviction event")
	}
	select {
	case key := <-evictedCh:
		t.Fatalf("unexpected extra eviction of %s", key)
	default:
	}

	assert.Equal(t, Metrics{
		Misses:     3,
		Insertions: 3,
		Evictions:  1,
	}, cache.Metrics())
}

func Test_GetMany_StopRestartAdvancesGeneration(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithTTL[string, string](time.Hour),
		WithLoader[string, string](loader),
	)

	go cache.Start()
	assert.Eventually(t, cache.IsStarted, time.Second, time.Millisecond)
	assert.Equal(t, uint64(0), cache.Generation())

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()
	loader.waitStarted("k")

	cache.Stop()
	assert.False(t, cache.IsStarted())
	assert.Equal(t, uint64(0), cache.Generation(), "Stop alone must not advance generation")

	go cache.Start()
	assert.Eventually(t, cache.IsStarted, time.Second, time.Millisecond)
	assert.Equal(t, uint64(1), cache.Generation())
	defer cache.Stop()

	loader.releaseKey("k")
	results := <-done
	assert.True(t, results[0].Hit())
	assert.Nil(t, cache.storedItem("k"))
}

func Test_GetMany_GenerationGuardDisabledKeepsLegacySemantics(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithLoader[string, string](loader),
	)
	assert.Equal(t, uint64(0), cache.Generation())

	// DeleteAll during a slow load: legacy behavior re-inserts the
	// value and does not advance generations.
	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"old"}, nil, 1)
	}()
	loader.waitStarted("old")
	cache.DeleteAll()
	assert.Equal(t, uint64(0), cache.Generation())
	loader.releaseKey("old")
	<-done
	assert.Equal(t, "value-old", cache.storedItem("old").Value())

	// ResetGeneration is a no-op without the guard.
	cache.ResetGeneration()
	assert.Equal(t, uint64(0), cache.Generation())

	// A concurrent Set is overwritten by the finishing load, matching
	// the historical last-write-wins behavior of Loader.
	done2 := make(chan []GetResult[string, string], 1)
	go func() {
		done2 <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()
	loader.waitStarted("k")
	cache.Set("k", "manual", DefaultTTL)
	loader.releaseKey("k")
	<-done2
	assert.Equal(t, "value-k", cache.storedItem("k").Value())
}

func Test_Get_UsesContextLoaderAndGuard(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithLoader[string, string](loader),
	)

	done := make(chan *Item[string, string], 1)
	go func() {
		done <- cache.Get("k")
	}()

	loader.waitStarted("k")
	cache.DeleteAll() // generation advances while load is in-flight
	loader.releaseKey("k")

	item := <-done
	require.NotNil(t, item)
	assert.Equal(t, "value-k", item.Value())
	assert.Nil(t, cache.storedItem("k"))
	assert.Equal(t, Metrics{Misses: 1}, cache.Metrics())
}

func Test_Get_DefaultBehaviorUnchanged(t *testing.T) {
	// Plain legacy Loader self-inserts and still works through Get.
	cache := New[string, string](
		WithLoader[string, string](LoaderFunc[string, string](
			func(c *Cache[string, string], key string) *Item[string, string] {
				return c.Set(key, "legacy", DefaultTTL)
			},
		)),
	)

	item := cache.Get("k")
	require.NotNil(t, item)
	assert.Equal(t, "legacy", item.Value())
	assert.Same(t, item, cache.storedItem("k"))
	assert.Equal(t, Metrics{Misses: 1, Insertions: 1}, cache.Metrics())
}

// joinTrackerLoader wraps an inner ContextLoader and coalesces calls
// in-process, recording how many callers share a load. The leader
// blocks on a gate; followers block on the same result channel. It
// mirrors SuppressedLoader's singleflight behavior with deterministic
// registration signaling.
type joinTrackerLoader struct {
	inner ContextLoader[string, string]

	mu        sync.Mutex
	cond      *sync.Cond
	callers   int
	leaderOut chan *loadOutcome
}

type loadOutcome struct {
	value string
	ttl   time.Duration
	err   error
}

func newJoinTrackerLoader(inner ContextLoader[string, string]) *joinTrackerLoader {
	l := &joinTrackerLoader{
		inner:     inner,
		leaderOut: make(chan *loadOutcome, 1),
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *joinTrackerLoader) waitCallers(n int) {
	l.mu.Lock()
	for l.callers < n {
		l.cond.Wait()
	}
	l.mu.Unlock()
}

func (l *joinTrackerLoader) LoadContext(ctx context.Context, c *Cache[string, string], key string) (string, time.Duration, error) {
	l.mu.Lock()
	l.callers++
	l.cond.Broadcast()
	leader := l.callers == 1
	l.mu.Unlock()

	if !leader {
		outcome := <-l.leaderOut
		return outcome.value, outcome.ttl, outcome.err
	}

	value, ttl, err := l.inner.LoadContext(ctx, c, key)
	l.leaderOut <- &loadOutcome{value: value, ttl: ttl, err: err}
	return value, ttl, err
}

func (l *joinTrackerLoader) Load(c *Cache[string, string], key string) *Item[string, string] {
	value, ttl, err := l.LoadContext(context.Background(), c, key)
	if err != nil {
		return nil
	}

	return c.Set(key, value, ttl)
}

func Test_SuppressedLoader_GetManySharesSingleLoad(t *testing.T) {
	loader := newGateLoader()
	tracker := newJoinTrackerLoader(loader)
	cache := New[string, string](
		WithLoader[string, string](tracker),
	)

	done1 := make(chan []GetResult[string, string], 1)
	done2 := make(chan []GetResult[string, string], 1)
	go func() {
		done1 <- cache.GetMany(context.Background(), []string{"k", "k"}, nil, 1)
	}()
	go func() {
		done2 <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()

	// Wait until the second call is parked as a follower while the
	// leader load is still blocked in the gate loader.
	tracker.waitCallers(2)
	loader.releaseKey("k")

	res1 := <-done1
	res2 := <-done2

	assert.Equal(t, 1, loader.callCount("k"))
	require.True(t, res1[0].Hit())
	require.True(t, res1[1].Hit())
	require.True(t, res2[0].Hit())
	for _, r := range append(res1, res2...) {
		assert.Equal(t, "value-k", r.Item.Value())
	}
	assert.Equal(t, "value-k", cache.storedItem("k").Value())
}

func Test_GetMany_LoaderErrorDoesNotInsert(t *testing.T) {
	loadErr := errors.New("nope")
	cache := New[string, string](
		WithLoader[string, string](ContextLoaderFunc[string, string](
			func(_ context.Context, _ *Cache[string, string], _ string) (string, time.Duration, error) {
				return "", DefaultTTL, loadErr
			},
		)),
	)

	res := cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	assert.ErrorIs(t, res[0].Err, loadErr)
	assert.Nil(t, cache.storedItem("k"))
	assert.Equal(t, Metrics{Misses: 1}, cache.Metrics())
}

func Test_GetMany_LoadedItemUsesDefaultTTL(t *testing.T) {
	loader := newGateLoader()
	cache := New[string, string](
		WithTTL[string, string](time.Hour),
		WithLoader[string, string](loader),
	)

	loader.releaseKey("k")
	res := cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	require.True(t, res[0].Hit())
	assert.Equal(t, time.Hour, res[0].Item.ttl)
	assert.WithinDuration(t, time.Now().Add(time.Hour), res[0].Item.expiresAt, time.Second)
}

func Test_GetMany_PerEntryLoaderOption(t *testing.T) {
	cache := New[string, string]()

	custom := ContextLoaderFunc[string, string](
		func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
			return "custom-" + key, DefaultTTL, nil
		},
	)

	opts := [][]Option[string, string]{
		nil,
		{WithLoader[string, string](custom)},
	}
	res := cache.GetMany(context.Background(), []string{"none", "yes"}, opts, 2)
	assert.False(t, res[0].Hit())
	require.True(t, res[1].Hit())
	assert.Equal(t, "custom-yes", res[1].Item.Value())
}

func Test_GetMany_LoaderCanCallCacheWithoutDeadlock(t *testing.T) {
	// A nested loader triggers another GetMany while the outer load
	// is in-flight. Because loaders run outside the cache mutex, this
	// must not deadlock, and the nested call may share the same
	// suppressed group for different keys.
	group := new(singleflight.Group)
	base := LoaderFunc[string, string](func(c *Cache[string, string], key string) *Item[string, string] {
		if key == "outer" {
			inner := c.Get("inner")
			return c.Set("outer", "outer->"+inner.Value(), DefaultTTL)
		}

		return c.Set("inner", "base", DefaultTTL)
	})
	suppressed := NewSuppressedLoader[string, string](base, group)
	cache := New[string, string](
		WithLoader[string, string](suppressed),
	)

	res := cache.GetMany(context.Background(), []string{"outer", "inner"}, nil, 2)
	require.True(t, res[0].Hit())
	assert.Equal(t, "outer->base", res[0].Item.Value())
	assert.Equal(t, "base", res[1].Item.Value())
}

func Test_GetMany_UpdateEventOnExistingKey(t *testing.T) {
	cache := New[string, string](WithTTL[string, string](time.Hour))
	cache.Set("k", "first", DefaultTTL)

	updatedCh := make(chan *Item[string, string], 1)
	cache.OnUpdate(func(_ context.Context, item *Item[string, string]) {
		updatedCh <- item
	})

	cache.Set("k2", "first2", DefaultTTL)
	cache.commitLoaded("k2", "second2", DefaultTTL, cache.Generation(), cache.storedItem("k2"))

	select {
	case item := <-updatedCh:
		assert.Equal(t, "second2", item.Value())
	case <-time.After(time.Second):
		t.Fatal("expected update event")
	}
	assert.Equal(t, Metrics{Insertions: 2, Updates: 1}, cache.Metrics())
}

func Test_GetMany_StopDuringLoadWithoutRestartKeepsResult(t *testing.T) {
	// Stop alone does not advance the generation; a load that
	// completes after Stop still commits because no restart occurred.
	loader := newGateLoader()
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithTTL[string, string](time.Hour),
		WithLoader[string, string](loader),
	)
	go cache.Start()
	assert.Eventually(t, cache.IsStarted, time.Second, time.Millisecond)

	done := make(chan []GetResult[string, string], 1)
	go func() {
		done <- cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	}()
	loader.waitStarted("k")

	cache.Stop()
	assert.False(t, cache.IsStarted())
	assert.Equal(t, uint64(0), cache.Generation())

	loader.releaseKey("k")
	res := <-done
	assert.True(t, res[0].Hit())
	assert.Equal(t, "value-k", cache.storedItem("k").Value())
}

func Test_GetMany_LegacyLoaderUnchangedWithGuard(t *testing.T) {
	// A self-inserting plain Loader retains legacy semantics even
	// with the guard enabled: its value is written last-write-wins.
	cache := New[string, string](
		WithGenerationGuard[string, string](),
		WithLoader[string, string](LoaderFunc[string, string](
			func(c *Cache[string, string], key string) *Item[string, string] {
				return c.Set(key, "legacy", DefaultTTL)
			},
		)),
	)

	res := cache.GetMany(context.Background(), []string{"k"}, nil, 1)
	require.True(t, res[0].Hit())
	assert.Equal(t, "legacy", res[0].Item.Value())
	assert.Equal(t, "legacy", cache.storedItem("k").Value())
	assert.Equal(t, Metrics{Misses: 1, Insertions: 1}, cache.Metrics())
}

func Test_SuppressedLoader_LegacyFollowerReadsCommitted(t *testing.T) {
	// A ContextLoaderFunc also satisfies Loader. When SuppressedLoader
	// serves a plain Get, the legacy Load path self-inserts; metrics
	// reflect a single insertion regardless of how many callers wait.
	loader := ContextLoaderFunc[string, string](
		func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
			return "v-" + key, DefaultTTL, nil
		},
	)
	suppressed := NewSuppressedLoader[string, string](loader, nil)
	cache := New[string, string](WithLoader[string, string](suppressed))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var seen []string
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if item := cache.Get("k"); item != nil {
				mu.Lock()
				seen = append(seen, item.Value())
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Len(t, seen, 4)
	assert.Equal(t, "v-k", cache.storedItem("k").Value())
	assert.Equal(t, uint64(1), cache.Metrics().Insertions)
}

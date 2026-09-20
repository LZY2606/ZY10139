package ttlcache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestValueLoader builds a ValueLoader whose execution for each key
// is blocked until the per-key release channel is closed. started is
// closed when the loader begins for the key, and invocations are counted.
func newTestValueLoader(t *testing.T) (
	loader ValueLoaderFunc[string, string],
	mu *sync.Mutex,
	calls map[string]int,
	started map[string]chan struct{},
	release map[string]chan struct{},
) {
	mu = &sync.Mutex{}
	calls = make(map[string]int)
	started = make(map[string]chan struct{})
	release = make(map[string]chan struct{})
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		started[k] = make(chan struct{})
		release[k] = make(chan struct{})
	}

	loader = ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		close(started[key])
		<-release[key]
		return "v-" + key, DefaultTTL, nil
	})
	return
}

func waitClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func Test_GetMany_EmptyAndOrder(t *testing.T) {
	c := New[string, string]()

	require.Empty(t, c.GetMany(context.Background(), nil, nil))

	c.Set("a", "x", NoTTL)
	results := c.GetMany(context.Background(), []string{"missing", "a", "missing", "a"}, nil)
	require.Len(t, results, 4)
	assert.True(t, results[0].Miss())
	assert.Equal(t, "x", results[1].Item.Value())
	assert.True(t, results[2].Miss())
	assert.Equal(t, "x", results[3].Item.Value())
	assert.Same(t, results[1].Item, results[3].Item)

	m := c.Metrics()
	assert.EqualValues(t, 1, m.Hits) // "a" counted once despite two positions
	assert.EqualValues(t, 1, m.Misses)
}

func Test_GetMany_DuplicateKeyLoadsOnce(t *testing.T) {
	loader, mu, calls, started, release := newTestValueLoader(t)
	c := New[string, string](WithValueLoader[string, string](loader))

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(),
			[]string{"a", "a", "a"},
			&GetManyOptions[string, string]{MaxConcurrency: 4})
		close(done)
	}()

	waitClosed(t, started["a"], "load of a to start")
	assert.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)

	close(release["a"])
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 3)
	for _, r := range results {
		require.NoError(t, r.Err)
		require.NotNil(t, r.Item)
		assert.Equal(t, "v-a", r.Item.Value())
	}
	assert.Same(t, results[0].Item, results[2].Item)

	mu.Lock()
	assert.Equal(t, 1, calls["a"])
	mu.Unlock()

	require.Contains(t, c.Keys(), "a")
	assert.Equal(t, "v-a", c.Get("a").Value())
	assert.EqualValues(t, 1, c.Metrics().Misses)
	assert.EqualValues(t, 1, c.Metrics().Insertions)
}

func Test_GetMany_DifferentKeysConcurrently(t *testing.T) {
	loader, mu, calls, started, release := newTestValueLoader(t)
	c := New[string, string](WithValueLoader[string, string](loader))

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(),
			[]string{"a", "b", "c", "a"},
			&GetManyOptions[string, string]{MaxConcurrency: 4})
		close(done)
	}()

	waitClosed(t, started["a"], "a start")
	waitClosed(t, started["b"], "b start")
	waitClosed(t, started["c"], "c start")

	for _, k := range []string{"a", "b", "c"} {
		close(release[k])
	}
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 4)
	assert.Equal(t, "v-a", results[0].Item.Value())
	assert.Equal(t, "v-b", results[1].Item.Value())
	assert.Equal(t, "v-c", results[2].Item.Value())
	assert.Same(t, results[0].Item, results[3].Item)

	mu.Lock()
	assert.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1}, calls)
	mu.Unlock()
}

func Test_GetMany_MaxConcurrency(t *testing.T) {
	var (
		mu      sync.Mutex
		current int
		maxSeen int
	)
	barrier := make(chan struct{})

	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		mu.Lock()
		current++
		if current > maxSeen {
			maxSeen = current
		}
		mu.Unlock()

		<-barrier

		mu.Lock()
		current--
		mu.Unlock()
		return key, NoTTL, nil
	})
	c := New[string, string](WithValueLoader[string, string](loader))

	done := make(chan struct{})
	go func() {
		c.GetMany(context.Background(),
			[]string{"a", "b", "c", "d", "e"},
			&GetManyOptions[string, string]{MaxConcurrency: 2})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return current == 2
	}, 5*time.Second, time.Millisecond)
	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return current > 2
	}, 200*time.Millisecond, 10*time.Millisecond)

	close(barrier)
	waitClosed(t, done, "GetMany to finish")
	assert.Equal(t, 2, maxSeen)
}

func Test_GetMany_CancelDoesNotStartNewLoads(t *testing.T) {
	var (
		mu    sync.Mutex
		calls = map[string]int{}
	)
	aStarted := make(chan struct{})
	aRelease := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(ctx context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		if key == "a" {
			close(aStarted)
			<-aRelease
		}
		return "v-" + key, NoTTL, nil
	})
	c := New[string, string](WithValueLoader[string, string](loader))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(ctx,
			[]string{"a", "b", "c"},
			&GetManyOptions[string, string]{MaxConcurrency: 1})
		close(done)
	}()

	// "a" holds the only concurrency slot and is blocked.
	waitClosed(t, aStarted, "a start")

	// Cancel while "a" is still running. The caller has already been
	// registered on the in-flight load, so its result survives; "b"/"c"
	// must never start.
	cancel()

	close(aRelease)
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 3)
	for i := 0; i < 3; i++ {
		assert.Nil(t, results[i].Item)
		assert.ErrorIs(t, results[i].Err, context.Canceled)
	}

	mu.Lock()
	// "a" ran to completion even though the caller canceled during it;
	// "b"/"c" never started.
	assert.Equal(t, 1, calls["a"])
	assert.Equal(t, 0, calls["b"])
	assert.Equal(t, 0, calls["c"])
	mu.Unlock()

	// The completed-but-abandoned load is not written to the cache.
	assert.False(t, c.Has("a"))
}

func Test_GetMany_SharedLoadNotCanceledBySingleWaiter(t *testing.T) {
	loader, _, _, started, release := newTestValueLoader(t)
	c := New[string, string](WithValueLoader[string, string](loader))

	leaderDone := make(chan struct{})
	go func() {
		c.GetMany(context.Background(), []string{"a"},
			&GetManyOptions[string, string]{MaxConcurrency: 1})
		close(leaderDone)
	}()
	waitClosed(t, started["a"], "leader load start")

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan struct{})
	var waiterResults []GetResult[string, string]
	go func() {
		waiterResults = c.GetMany(ctx, []string{"a"}, nil)
		close(waiterDone)
	}()

	// Wait until the follower is registered on the shared load.
	assert.Eventually(t, func() bool {
		c.loads.mu.Lock()
		defer c.loads.mu.Unlock()
		call, ok := c.loads.m["a"]
		return ok && call.waiters == 2
	}, 5*time.Second, time.Millisecond)

	cancel()
	waitClosed(t, waiterDone, "waiter to give up")
	require.Len(t, waiterResults, 1)
	assert.ErrorIs(t, waiterResults[0].Err, context.Canceled)

	// The shared load must still be alive and complete for the leader.
	assert.Never(t, func() bool {
		select {
		case <-leaderDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)

	close(release["a"])
	waitClosed(t, leaderDone, "leader load to complete")
	assert.Equal(t, "v-a", c.Get("a").Value())
}

func Test_GetMany_LoadCanceledWhenAllWaitersAbandon(t *testing.T) {
	canceled := make(chan struct{})
	started := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(ctx context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return "", 0, ctx.Err()
	})
	c := New[string, string](WithValueLoader[string, string](loader))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.GetMany(ctx, []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, started, "load to start")
	cancel()
	waitClosed(t, done, "GetMany to return")
	waitClosed(t, canceled, "load context to be canceled")

	_, ok := c.Items()["a"]
	assert.False(t, ok)
}

func Test_GetMany_GenerationGuard_DeleteAll(t *testing.T) {
	loadEnter := make(chan struct{})
	var loadCalls int
	var mu sync.Mutex

	enterOnce := sync.Once{}
	releaseFirst := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, c *Cache[string, string], key string) (string, time.Duration, error) {
		mu.Lock()
		loadCalls++
		first := loadCalls == 1
		mu.Unlock()
		if first {
			enterOnce.Do(func() { close(loadEnter) })
			<-releaseFirst
		}
		return "loaded", NoTTL, nil
	})
	c := New[string, string](
		WithGenerationGuard[string, string](),
		WithValueLoader[string, string](loader),
	)
	c.Set("keep", "k", NoTTL)

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, loadEnter, "load start")
	c.DeleteAll()
	assert.False(t, c.Has("keep"))
	close(releaseFirst)

	waitClosed(t, done, "GetMany to finish")
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)
	require.NotNil(t, results[0].Item)
	assert.Equal(t, "loaded", results[0].Item.Value())

	// The stale result is not part of the new generation's cache.
	assert.False(t, c.Has("a"))
	assert.Nil(t, c.Get("a"))

	// A later load inserts into the current generation.
	results2 := c.GetMany(context.Background(), []string{"a"}, nil)
	require.NoError(t, results2[0].Err)
	assert.Equal(t, "loaded", results2[0].Item.Value())
	assert.True(t, c.Has("a"))

	mu.Lock()
	assert.Equal(t, 2, loadCalls)
	mu.Unlock()
}

func Test_GetMany_GenerationGuard_ResetGeneration(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	c := New[string, string](
		WithGenerationGuard[string, string](),
		WithValueLoader[string, string](loader),
	)

	gen := c.Generation()
	assert.EqualValues(t, 1, gen)

	done := make(chan struct{})
	go func() {
		c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, loadEnter, "load start")
	newGen := c.ResetGeneration()
	assert.Equal(t, gen+1, newGen)
	close(release)
	waitClosed(t, done, "GetMany to finish")

	assert.False(t, c.Has("a"))
}

func Test_GetMany_GenerationGuard_StopRestart(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	c := New[string, string](
		WithTTL[string, string](time.Hour),
		WithGenerationGuard[string, string](),
		WithValueLoader[string, string](loader),
	)

	go c.Start()
	require.Eventually(t, c.IsStarted, 5*time.Second, time.Millisecond)

	done := make(chan struct{})
	go func() {
		c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()
	waitClosed(t, loadEnter, "load start")

	c.Stop()
	assert.False(t, c.IsStarted())
	// Stop on its own does not invalidate in-flight loads; the restart
	// advances the generation.
	close(release)
	waitClosed(t, done, "GetMany to finish")
	assert.True(t, c.Has("a"))
	genAfterStop := c.Generation()

	go c.Start()
	require.Eventually(t, c.IsStarted, 5*time.Second, time.Millisecond)
	assert.Equal(t, genAfterStop+1, c.Generation())

	results := c.GetMany(context.Background(), []string{"a"}, nil)
	require.NoError(t, results[0].Err)
	assert.Equal(t, "loaded", results[0].Item.Value())
	assert.True(t, c.Has("a"))

	c.Stop()
}

func Test_GetMany_SetWinsDuringLoad(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	c := New[string, string](
		WithGenerationGuard[string, string](),
		WithValueLoader[string, string](loader),
	)

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, loadEnter, "load start")
	c.Set("a", "fresh", NoTTL)
	gen := c.ResetGeneration()
	assert.EqualValues(t, 2, gen)
	close(release)
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 1)
	assert.Equal(t, "loaded", results[0].Item.Value())

	// The cache keeps the value that was set during the load.
	assert.Equal(t, "fresh", c.Get("a").Value())
	assert.EqualValues(t, 1, c.Metrics().Insertions)
}

func Test_GetMany_CapacityEviction(t *testing.T) {
	var (
		mu          sync.Mutex
		insertedKey string
		evictedKey  string
	)
	bStarted := make(chan struct{})
	letBFinish := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		if key == "b" {
			close(bStarted)
			<-letBFinish
		}
		return key, NoTTL, nil
	})
	c := New[string, string](
		WithCapacity[string, string](1),
		WithValueLoader[string, string](loader),
	)
	off := c.OnInsertion(func(_ context.Context, item *Item[string, string]) {
		mu.Lock()
		insertedKey = item.Key()
		mu.Unlock()
	})
	defer off()
	offe := c.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		assert.Equal(t, EvictionReasonCapacityReached, reason)
		mu.Lock()
		evictedKey = item.Key()
		mu.Unlock()
	})
	defer offe()

	done := make(chan struct{})
	go func() {
		c.GetMany(context.Background(), []string{"a", "b"},
			&GetManyOptions[string, string]{MaxConcurrency: 2})
		close(done)
	}()

	// "a" inserts first; only then release "b" which must evict "a".
	assert.Eventually(t, func() bool { return c.Has("a") }, 5*time.Second, time.Millisecond)
	waitClosed(t, bStarted, "b load to start")
	close(letBFinish)
	waitClosed(t, done, "GetMany to finish")

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return insertedKey == "b" && evictedKey == "a"
	}, 5*time.Second, time.Millisecond)
	assert.Len(t, c.Keys(), 1)
	assert.False(t, c.Has("a"))
	assert.True(t, c.Has("b"))
	m := c.Metrics()
	assert.EqualValues(t, 2, m.Insertions)
	assert.EqualValues(t, 1, m.Evictions)
}

func Test_GetMany_LegacyLoaderKeepsSemanticsWithGuard(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := LoaderFunc[string, string](func(c *Cache[string, string], key string) *Item[string, string] {
		close(loadEnter)
		<-release
		// The legacy loader inserts the value itself.
		return c.Set(key, "legacy", NoTTL)
	})
	c := New[string, string](
		WithGenerationGuard[string, string](),
		WithLoader[string, string](loader),
	)

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, loadEnter, "load start")
	c.ResetGeneration()
	c.DeleteAll()
	close(release)
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 1)
	require.NotNil(t, results[0].Item)
	assert.Equal(t, "legacy", results[0].Item.Value())
	// Legacy behavior: the loader's own Set wins regardless of generation.
	assert.True(t, c.Has("a"))
}

func Test_GetMany_SuppressedLoaderSingleflight(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	aEnter := make(chan struct{}, 4)
	aRelease := make(chan struct{})

	inner := LoaderFunc[string, string](func(c *Cache[string, string], key string) *Item[string, string] {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		if key == "a" {
			aEnter <- struct{}{}
			<-aRelease
		}
		return c.Set(key, "v-"+key, NoTTL)
	})
	loader := NewSuppressedLoader[string, string](inner, nil)
	c := New[string, string](WithLoader[string, string](loader))

	const waiters = 4
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := c.GetMany(context.Background(), []string{"a"}, nil)
			require.Len(t, r, 1)
			if r[0].Item != nil {
				assert.Equal(t, "v-a", r[0].Item.Value())
			}
		}()
	}

	// Exactly one load of "a" starts and stays blocked while the other
	// callers join the shared in-flight load.
	waitClosed(t, aEnter, "first load of a to start")
	// Give the scheduler time to register the other callers on the same
	// key without releasing the load. No load can finish until aRelease.
	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls["a"] > 1
	}, 150*time.Millisecond, 5*time.Millisecond)

	// A single-key Get of a different key proceeds concurrently without
	// deadlocking against the shared "a" load.
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.Equal(t, "v-b", c.Get("b").Value())
	}()

	close(aRelease)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	waitClosed(t, done, "all callers to finish")

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls["b"] == 1
	}, 5*time.Second, time.Millisecond)

	mu.Lock()
	assert.Equal(t, map[string]int{"a": 1, "b": 1}, calls)
	mu.Unlock()
	assert.Equal(t, "v-a", c.Get("a").Value())
}

func Test_GetMany_LoadErrorAndNotFound(t *testing.T) {
	loadErr := errors.New("boom")
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		switch key {
		case "a":
			return "", 0, loadErr
		case "b":
			return "", 0, ErrNotFound
		default:
			return key, NoTTL, nil
		}
	})
	c := New[string, string](WithValueLoader[string, string](loader))

	results := c.GetMany(context.Background(), []string{"a", "b", "c"},
		&GetManyOptions[string, string]{MaxConcurrency: 3})
	require.Len(t, results, 3)
	assert.Nil(t, results[0].Item)
	assert.ErrorIs(t, results[0].Err, loadErr)
	assert.True(t, results[1].Miss())
	assert.Equal(t, "c", results[2].Item.Value())

	assert.False(t, c.Has("a"))
	assert.False(t, c.Has("b"))
	assert.True(t, c.Has("c"))
	assert.EqualValues(t, 1, c.Metrics().Insertions)
	assert.EqualValues(t, 3, c.Metrics().Misses)
}

func Test_GetMany_SetWinsSameGeneration(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	c := New[string, string](
		WithGenerationGuard[string, string](),
		WithValueLoader[string, string](loader),
	)

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, loadEnter, "load start")
	// No generation change: a plain concurrent Set must still win.
	c.Set("a", "fresh", NoTTL)
	close(release)
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 1)
	assert.Equal(t, "loaded", results[0].Item.Value())
	assert.NotSame(t, results[0].Item, c.Get("a"))
	assert.Equal(t, "fresh", c.Get("a").Value())
}

func Test_GetMany_DeleteDuringLoadSameGeneration(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	c := New[string, string](
		WithGenerationGuard[string, string](),
		WithValueLoader[string, string](loader),
	)
	// Expired entries remain in the map until cleanup runs: the phase-1
	// lookup records the expired element as its snapshot and still
	// counts as a miss. Delete it while the load is in-flight.
	c.Set("a", "old", time.Nanosecond)

	done := make(chan struct{})
	go func() {
		c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()
	waitClosed(t, loadEnter, "load start")

	c.Delete("a")
	close(release)
	waitClosed(t, done, "GetMany to finish")
	assert.False(t, c.Has("a"))
}

func Test_GetMany_GuardDisabledWritesBack(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	c := New[string, string](WithValueLoader[string, string](loader))

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()

	waitClosed(t, loadEnter, "load start")
	c.ResetGeneration()
	c.DeleteAll()
	close(release)
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 1)
	assert.Equal(t, "loaded", results[0].Item.Value())
	// Legacy-compatible semantics without the guard: last writer wins
	// even across generations.
	assert.True(t, c.Has("a"))
}

func Test_GetMany_TouchOnHit(t *testing.T) {
	ttl := 120 * time.Millisecond
	c := New[string, string](
		WithTTL[string, string](ttl),
		WithValueLoader[string, string](ValueLoaderFunc[string, string](
			func(context.Context, *Cache[string, string], string) (string, time.Duration, error) {
				return "reloaded", NoTTL, nil
			})),
	)
	c.Set("a", "v", DefaultTTL)

	// Wait until the original expiry would have elapsed, then keep the
	// item alive through batch touches.
	time.Sleep(70 * time.Millisecond)
	c.GetMany(context.Background(), []string{"a", "a"}, nil)
	time.Sleep(70 * time.Millisecond)
	assert.True(t, c.Has("a"), "touch on hit should extend the TTL")
	assert.Equal(t, "v", c.Get("a").Value())

	// With touch disabled, the item expires according to its original
	// touched deadline.
	c.Set("b", "v", DefaultTTL)
	time.Sleep(70 * time.Millisecond)
	c.GetMany(context.Background(), []string{"b"},
		&GetManyOptions[string, string]{
			Options: [][]Option[string, string]{{WithDisableTouchOnHit[string, string]()}},
		})
	time.Sleep(70 * time.Millisecond)
	assert.False(t, c.Has("b"), "disabled touch must not extend the TTL")
}

func Test_GetMany_LoaderTTL(t *testing.T) {
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		if key == "forever" {
			return key, NoTTL, nil
		}
		return key, time.Minute, nil
	})
	c := New[string, string](
		WithTTL[string, string](time.Hour),
		WithValueLoader[string, string](loader),
	)

	results := c.GetMany(context.Background(), []string{"timed", "forever"}, nil)
	require.Len(t, results, 2)
	assert.Equal(t, time.Minute, results[0].Item.TTL())
	assert.Equal(t, NoTTL, results[1].Item.TTL())
}

func Test_GetMany_PerKeyValueLoader(t *testing.T) {
	c := New[string, string]()
	results := c.GetMany(context.Background(), []string{"a"},
		&GetManyOptions[string, string]{
			Options: [][]Option[string, string]{{
				WithValueLoader[string, string](ValueLoaderFunc[string, string](
					func(context.Context, *Cache[string, string], string) (string, time.Duration, error) {
						return "ephemeral", NoTTL, nil
					})),
			}},
		})
	require.Len(t, results, 1)
	require.NotNil(t, results[0].Item)
	assert.Equal(t, "ephemeral", results[0].Item.Value())
	assert.True(t, c.Has("a"))
}

func Test_GetMany_CallbacksOrderAndCount(t *testing.T) {
	var mu sync.Mutex
	var insertions []string
	var updates []string
	var evictions []EvictionReason

	loader := ValueLoaderFunc[string, string](func(_ context.Context, c *Cache[string, string], key string) (string, time.Duration, error) {
		return key, NoTTL, nil
	})
	c := New[string, string](
		WithCapacity[string, string](10),
		WithValueLoader[string, string](loader),
	)
	oi := c.OnInsertion(func(_ context.Context, item *Item[string, string]) {
		mu.Lock()
		insertions = append(insertions, item.Key())
		mu.Unlock()
	})
	defer oi()
	ou := c.OnUpdate(func(_ context.Context, item *Item[string, string]) {
		mu.Lock()
		updates = append(updates, item.Key())
		mu.Unlock()
	})
	defer ou()
	oe := c.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		mu.Lock()
		evictions = append(evictions, reason)
		mu.Unlock()
	})
	defer oe()

	c.Set("a", "old", NoTTL)
	// "b"/"c" are inserted and the duplicate "b" loads once; the present
	// "a" is a hit, so it produces no callbacks.
	c.GetMany(context.Background(), []string{"a", "b", "b", "c"}, nil)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(insertions) == 3 // "a" from Set, "b"/"c" from GetMany
	}, 5*time.Second, time.Millisecond)

	mu.Lock()
	assert.ElementsMatch(t, []string{"a", "b", "c"}, insertions)
	assert.Empty(t, updates)
	assert.Empty(t, evictions)
	mu.Unlock()

	c.DeleteAll()
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(evictions) == 3 // "a", "b", "c"
	}, 5*time.Second, time.Millisecond)
	for _, r := range evictions {
		assert.Equal(t, EvictionReasonDeleted, r)
	}
}

func Test_GetMany_MaxCostEviction(t *testing.T) {
	var (
		mu      sync.Mutex
		reasons []EvictionReason
		evicted []string
	)
	costFunc := func(item CostItem[string, string]) uint64 {
		return uint64(len(item.Value))
	}
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		return key, NoTTL, nil
	})
	c := New[string, string](
		WithMaxCost[string, string](3, costFunc),
		WithValueLoader[string, string](loader),
	)
	off := c.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		mu.Lock()
		reasons = append(reasons, reason)
		evicted = append(evicted, item.Key())
		mu.Unlock()
	})
	defer off()

	results := c.GetMany(context.Background(), []string{"aa", "bb"},
		&GetManyOptions[string, string]{MaxConcurrency: 1})
	require.Len(t, results, 2)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(evicted) == 1
	}, 5*time.Second, time.Millisecond)
	mu.Lock()
	assert.Equal(t, []EvictionReason{EvictionReasonMaxCostExceeded}, reasons)
	mu.Unlock()

	assert.Equal(t, uint64(2), c.Cost())
	m := c.Metrics()
	assert.EqualValues(t, 2, m.Insertions)
	assert.EqualValues(t, 1, m.Evictions)
}

func Test_GetMany_GenerationGetter(t *testing.T) {
	c := New[string, string]()
	assert.EqualValues(t, 1, c.Generation())
	c.DeleteAll()
	assert.EqualValues(t, 2, c.Generation())
	assert.EqualValues(t, 3, c.ResetGeneration())
	assert.EqualValues(t, 3, c.Generation())
}

func Test_GetMany_GenerationGuardDisabledByDefault(t *testing.T) {
	loadEnter := make(chan struct{})
	release := make(chan struct{})
	loader := ValueLoaderFunc[string, string](func(_ context.Context, _ *Cache[string, string], key string) (string, time.Duration, error) {
		close(loadEnter)
		<-release
		return "loaded", NoTTL, nil
	})
	// No WithGenerationGuard: old, last-writer-wins semantics are kept.
	c := New[string, string](WithValueLoader[string, string](loader))

	done := make(chan struct{})
	var results []GetResult[string, string]
	go func() {
		results = c.GetMany(context.Background(), []string{"a"}, nil)
		close(done)
	}()
	waitClosed(t, loadEnter, "load start")
	c.DeleteAll()
	close(release)
	waitClosed(t, done, "GetMany to finish")

	require.Len(t, results, 1)
	assert.Equal(t, "loaded", results[0].Item.Value())
	assert.True(t, c.Has("a"), "legacy semantics: stale load is written back")
}

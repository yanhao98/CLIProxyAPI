package auth

import (
	"fmt"
	"sync"
	"testing"
)

func TestShardedMapConcurrency(t *testing.T) {
	var sm shardedMap[string]
	const numGoroutines = 100
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("account-%d", (gid*31+i)%200)
				val := fmt.Sprintf("val-%d-%d", gid, i)

				actual, loaded := sm.LoadOrStore(key, val)
				if !loaded && actual != val {
					t.Errorf("expected actual == val on store")
				}

				got, ok := sm.Load(key)
				if !ok || got == "" {
					t.Errorf("expected load ok for key %s", key)
				}

				if i%10 == 0 {
					sm.Store(key, val)
				}
			}
		}(g)
	}

	wg.Wait()
}

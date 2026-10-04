package task_queue

import (
	"fmt"
	"math/rand"
	"testing"
)

// Asserts the amount of tasks enqueued collapses to the total # of unique idem keys
func TestEnqueueDeduplication(t *testing.T) {
	cases := []struct {
		name       string   // Our test output name
		keys       []string // our test input
		wantUnique int      // Expected value (asserts)
	}{
		{
			"all distinct keys",
			[]string{"a", "b", "c"},
			3,
		},
		{
			"some repeats",
			[]string{"a", "a", "b", "c", "d", "d"},
			4,
		},
		{
			"all the same",
			[]string{"a", "a", "a", "a", "a", "a"},
			1,
		},
		{
			"empty",
			[]string{},
			0,
		},
	}

	for _, testCase := range cases {
		// t.Run makes each row it's own subtest
		t.Run(testCase.name, func(t *testing.T) {
			q, _ := newTestQueue(t)
			produce(t, q, testCase.keys)
			if got := len(q.index); got != testCase.wantUnique {
				t.Fatalf("queue tasks = %d, want %d", got, testCase.wantUnique)
			}
		})
	}

}

// Takes in a seed and generates X (total) number of randomized idemKeys and returns it as an array. The random keys range from [0 -> pool) in value
func makeKeys(seed int64, pool, total int) []string {
	r := rand.New(rand.NewSource(seed)) // seeded -> reproducible
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", r.Intn(pool)) // pick from pool of size `pool`
	}
	return keys
}

func countDistinctKeys(k []string) int {
	// Count distinct via set:
	mySet := make(map[string]struct{})
	for _, key := range k {
		mySet[key] = struct{}{}
	}
	return len(mySet)
}

func TestEnqueueDeduplicationWithRandomIdemKeys(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		// Running test for each key (turns into seed-3 etc)
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			q, _ := newTestQueue(t)
			keys := makeKeys(seed, 6, 100)
			want := countDistinctKeys(keys)
			produce(t, q, keys)
			if got := len(q.index); got != want {
				t.Fatalf("queue task count: %d, wanted %d", got, want)
			}
		})
	}
}

// All tests only work for happy path / process doesn't crash. We need to check for crashes now

func TestEnqueueDeduplicationDuringReplayWAL(t *testing.T) {
	//Build the queue
	q, path := newTestQueue(t)

	//Produce with duplicates
	keys := []string{"k1", "k2", "k3", "k1", "k4", "k2", "k2"}
	want := countDistinctKeys(keys)
	produce(t, q, keys)

	// Now queue crashes
	q2 := reopenQueue(t, path)

	if got := len(q2.index); got != want {
		t.Fatalf("queue on restart task count: %d, wanted %d", got, want)
	}

	// Enqueueing seen queue after replay
	existingId, err := q2.Enqueue([]byte("a"), "k2")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Same task count
	if got := len(q2.index); got != want {
		t.Fatalf("queue after replay & enqueuing tasks task count: %d, wanted %d", got, want)
	}

	// Same IDs too
	if existingId != *q.indexOfIdemKeys["k2"] {
		t.Fatalf("Difference in UUID for idemKey post replay. Original ID: %v, post-restart ID: %v", *q.indexOfIdemKeys["k2"], existingId)
	}

	q2.Enqueue([]byte("a"), "newKey")
	if len(q2.index) != want+1 {
		t.Fatalf("Enqueue new unseen idemkey after replay failed. Have: %v but want: %d", len(q2.index), want+1)
	}

}

// Figure out a way to inject a failure at the byte/record level
// since you cant just .kill the process at byte 9/10 ykwim
func TestAppendWriteReturnsErrorAndDoesntMakeCorruptedRecordDurable(t *testing.T) {

}

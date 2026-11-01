/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package bonsai

import (
	"testing"
	"time"
)

func TestCloseJoinsValueLogGC(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
	}{
		{"before-first-tick", time.Hour},
		{"frequent-ticks", time.Microsecond},
		{"disabled", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(Options{Dir: t.TempDir(), ValueLogGCInterval: tc.interval})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			done := db.gcDone
			if (done == nil) != (tc.interval < 0) {
				t.Fatal("GC lifecycle does not match requested interval")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if done != nil {
				select {
				case <-done:
				default:
					t.Fatal("Close returned before the GC goroutine stopped")
				}
			}
			if err := db.Close(); err != nil {
				t.Fatalf("repeated Close: %v", err)
			}
		})
	}
}

// Closing immediately after Open can signal shutdown before the worker gets
// scheduled. It must still observe the closed channel and acknowledge exit.
func TestValueLogGCObservesStopBeforeStart(t *testing.T) {
	db := &DB{gcStop: make(chan struct{}), gcDone: make(chan struct{})}
	close(db.gcStop)
	go db.valueLogGCLoop(time.Hour)
	select {
	case <-db.gcDone:
	case <-time.After(5 * time.Second):
		t.Fatal("GC failed to stop when shutdown preceded startup")
	}
}

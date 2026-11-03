package x_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const probeSrc = `package main

import (
	"fmt"
	"runtime"

	%s
)

func main() { fmt.Println("goroutines", runtime.NumGoroutine()) }
`

func buildProbe(t *testing.T, imports ...string) string {
	t.Helper()
	root, _ := filepath.Abs("..")
	dir, err := os.MkdirTemp(root, "initprobe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	var blank []string
	for _, imp := range imports {
		blank = append(blank, "_ \""+imp+"\"")
	}
	src := strings.Replace(probeSrc, "%s", strings.Join(blank, "\n\t"), 1)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "probe")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

// A bare import of bonsai/x must be cheap and must not start goroutines,
// because every bashy exec links it.
func TestImportXInitCostAndGoroutines(t *testing.T) {
	bin := buildProbe(t, "github.com/qiangli/bonsai/x")
	re := regexp.MustCompile(`init github.com/qiangli/bonsai/x @[\d.]+ ms, ([\d.]+) ms clock`)
	// glog and opencensus start their own goroutines at import; that is the floor.
	baseBin := buildProbe(t, "github.com/golang/glog", "go.opencensus.io/stats/view")
	baseOut, err := exec.Command(baseBin).CombinedOutput()
	if err != nil {
		t.Fatal(err, string(baseOut))
	}
	base := goroutines(t, string(baseOut))
	var best float64 = -1
	for i := 0; i < 5; i++ {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), "GODEBUG=inittrace=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatal(err, string(out))
		}
		m := re.FindStringSubmatch(string(out))
		if m == nil {
			t.Fatalf("no inittrace line for bonsai/x:\n%s", out)
		}
		v, _ := strconv.ParseFloat(m[1], 64)
		if best < 0 || v < best {
			best = v
		}
		if i == 0 {
			if got := goroutines(t, string(out)); got != base {
				t.Errorf("import alone started goroutines: got %d, third-party baseline %d", got, base)
			}
		}
	}
	t.Logf("bonsai/x init best of 5: %.3f ms", best)
	if best > 2 {
		t.Errorf("bonsai/x init = %.3f ms, want <= 2 ms", best)
	}
}

func goroutines(t *testing.T, out string) int {
	t.Helper()
	i := strings.LastIndex(out, "goroutines ")
	if i < 0 {
		t.Fatalf("no goroutine count in %q", out)
	}
	n, err := strconv.Atoi(strings.TrimSpace(out[i+len("goroutines "):]))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

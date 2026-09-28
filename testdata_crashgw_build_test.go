package lsg

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// buildCrashHelper compiles the testdata/crashgw subprocess into a scratch
// module and returns its binary path. testdata is not part of the main
// module's build graph, so a throwaway module with a replace directive is
// used.
func buildCrashHelper(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Dir(thisFile)

	work := t.TempDir()
	srcDir := filepath.Join(work, "crashgw")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Skipf("mkdir helper: %v", err)
	}
	mainSrc, err := os.ReadFile(filepath.Join(root, "testdata", "crashgw", "main.go"))
	if err != nil {
		t.Skipf("read helper source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "main.go"), mainSrc, 0o644); err != nil {
		t.Skip(err)
	}
	gomod := "module crashgw\n\ngo 1.25.0\n\n" +
		"require github.com/guoyyit/local-unix-socket-gateway-go v0.0.0\n\n" +
		"replace github.com/guoyyit/local-unix-socket-gateway-go => " + root + "\n"
	if err := os.WriteFile(filepath.Join(srcDir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Skip(err)
	}

	out := filepath.Join(work, "crashgw-bin")
	cmd := exec.Command("go", "build", "-o", out, ".")
	cmd.Dir = srcDir
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("could not build crash helper: %v\n%s", err, b)
	}
	return out
}

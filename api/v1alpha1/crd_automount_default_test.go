package v1alpha1

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCRDsDoNotDefaultAutomountServiceAccountToken(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	var issues []string
	err := filepath.WalkDir(crdDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for {
			var doc map[string]any
			if decodeErr := decoder.Decode(&doc); decodeErr != nil {
				if errors.Is(decodeErr, io.EOF) {
					break
				}
				return fmt.Errorf("failed to decode document in %s: %w", path, decodeErr)
			}
			scanAutomountDefaults(doc, []string{filepath.Base(path)}, &issues, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk CRD directory: %v", err)
	}
	if len(issues) > 0 {
		t.Fatalf("found server-side defaults for automountServiceAccountToken:\n%s", strings.Join(issues, "\n"))
	}
}

func scanAutomountDefaults(node any, ancestry []string, issues *[]string, file string) {
	switch value := node.(type) {
	case map[string]any:
		if field, ok := value["automountServiceAccountToken"]; ok {
			if fieldMap, ok := field.(map[string]any); ok {
				if _, hasDefault := fieldMap["default"]; hasDefault {
					*issues = append(*issues, fmt.Sprintf("%s @ %s", file, strings.Join(append(ancestry, "automountServiceAccountToken"), " > ")))
				}
			}
		}
		for key, child := range value {
			scanAutomountDefaults(child, append(ancestry, key), issues, file)
		}
	case []any:
		for idx, child := range value {
			scanAutomountDefaults(child, append(ancestry, fmt.Sprintf("[%d]", idx)), issues, file)
		}
	}
}

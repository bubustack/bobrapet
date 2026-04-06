package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestMustSetupControllersRegistersTransport(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	controllerItems := findControllerItems(t, file)
	assertControllerSetupUsesType(t, fset, controllerItems, "Transport", "controller.TransportReconciler")
}

func TestMustSetupControllersRegistersStoryTriggerAndEffectClaim(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	controllerItems := findControllerItems(t, file)
	assertControllerSetupUsesType(t, fset, controllerItems, "StoryTrigger", "runscontroller.StoryTriggerReconciler")
	assertControllerSetupUsesType(t, fset, controllerItems, "EffectClaim", "runscontroller.EffectClaimReconciler")
}

func TestRunDoesNotRegisterStoryTriggerOrEffectClaimOutsideMustSetupControllers(t *testing.T) {
	t.Parallel()

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	mainSource := string(src)

	if count := strings.Count(mainSource, "runscontroller.StoryTriggerReconciler"); count != 1 {
		t.Fatalf("expected StoryTrigger reconciler to be registered only through mustSetupControllers, found %d occurrences", count) //nolint:lll
	}
	if count := strings.Count(mainSource, "runscontroller.EffectClaimReconciler"); count != 1 {
		t.Fatalf("expected EffectClaim reconciler to be registered only through mustSetupControllers, found %d occurrences", count) //nolint:lll
	}
}

func TestMustInitOperatorServicesPropagatesConstructorError(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	var initFn *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || fn.Name.Name != "mustInitOperatorServices" || fn.Body == nil {
			continue
		}
		initFn = fn
		break
	}
	if initFn == nil {
		t.Fatal("could not find mustInitOperatorServices")
	}

	src := nodeString(t, fset, initFn.Body)
	if !strings.Contains(src, "operatorConfigManager, err := config.NewOperatorConfigManager(") {
		t.Fatalf("mustInitOperatorServices must capture constructor error, got: %s", src)
	}
	if !strings.Contains(src, "if err != nil {") {
		t.Fatalf("mustInitOperatorServices must branch on constructor error, got: %s", src)
	}
	if !strings.Contains(src, "return nil, nil, nil, nil, fmt.Errorf(\"create operator config manager: %w\", err)") {
		t.Fatalf("mustInitOperatorServices must return wrapped constructor error, got: %s", src)
	}
}

//nolint:gocyclo // complex by design
func findControllerItems(t *testing.T, file *ast.File) map[string]map[string]ast.Expr {
	t.Helper()

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || fn.Name.Name != "mustSetupControllers" || fn.Body == nil {
			continue
		}
		for _, stmt := range fn.Body.List {
			assign, ok := stmt.(*ast.AssignStmt)
			if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 || assign.Tok != token.DEFINE {
				continue
			}
			lhs, ok := assign.Lhs[0].(*ast.Ident)
			if !ok || lhs.Name != "controllers" {
				continue
			}
			lit, ok := assign.Rhs[0].(*ast.CompositeLit)
			if !ok {
				t.Fatalf("controllers assignment is not a composite literal")
			}
			items := make(map[string]map[string]ast.Expr, len(lit.Elts))
			for _, elt := range lit.Elts {
				itemLit, ok := elt.(*ast.CompositeLit)
				if !ok {
					continue
				}
				fields := map[string]ast.Expr{}
				var name string
				for _, itemField := range itemLit.Elts {
					kv, ok := itemField.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					fields[key.Name] = kv.Value
					if key.Name == "name" {
						if basic, ok := kv.Value.(*ast.BasicLit); ok {
							name = strings.Trim(basic.Value, "\"")
						}
					}
				}
				if name != "" {
					items[name] = fields
				}
			}
			return items
		}
	}
	t.Fatalf("could not find controllers declaration in mustSetupControllers")
	return nil
}

func assertControllerSetupUsesType(t *testing.T, fset *token.FileSet, controllerItems map[string]map[string]ast.Expr, name, expectedType string) { //nolint:lll
	t.Helper()

	item, ok := controllerItems[name]
	if !ok {
		t.Fatalf("mustSetupControllers controllers list is missing %s entry", name)
	}

	setupExpr, ok := item["setup"]
	if !ok {
		t.Fatalf("%s controller entry is missing setup function", name)
	}
	setupSource := nodeString(t, fset, setupExpr)
	if !strings.Contains(setupSource, expectedType) {
		t.Fatalf("%s setup does not use %s: %s", name, expectedType, setupSource)
	}
}

func nodeString(t *testing.T, fset *token.FileSet, n ast.Node) string {
	t.Helper()

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, n); err != nil {
		t.Fatalf("format node: %v", err)
	}
	return buf.String()
}

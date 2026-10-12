package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func parseMain(t *testing.T, src any) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func funcBody(f *ast.File, name string) *ast.BlockStmt {
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name {
			return fd.Body
		}
	}
	return nil
}

// callName returns "pkg.Fn" or "Fn" for a call expression.
func callName(c *ast.CallExpr) string {
	switch fn := c.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		if id, ok := fn.X.(*ast.Ident); ok {
			return id.Name + "." + fn.Sel.Name
		}
		return "." + fn.Sel.Name
	}
	return ""
}

// unconditionalCall reports whether stmt is itself a call to name, or an `if`
// whose init statement is that call and whose condition is not a literal
// (so `if false { ... }` and nesting inside a branch do not count).
func unconditionalCall(stmt ast.Stmt, name string) (token.Pos, bool) {
	switch s := stmt.(type) {
	case *ast.ExprStmt:
		if c, ok := s.X.(*ast.CallExpr); ok && callName(c) == name {
			return c.Pos(), true
		}
	case *ast.AssignStmt: // x := call(...)
		if len(s.Rhs) == 1 {
			if c, ok := s.Rhs[0].(*ast.CallExpr); ok && callName(c) == name {
				return c.Pos(), true
			}
		}
	case *ast.IfStmt:
		if as, ok := s.Init.(*ast.AssignStmt); ok && len(as.Rhs) == 1 {
			if c, ok := as.Rhs[0].(*ast.CallExpr); ok && callName(c) == name {
				if id, lit := s.Cond.(*ast.Ident); lit && (id.Name == "false" || id.Name == "true") {
					return 0, false
				}
				return c.Pos(), true
			}
		}
	}
	return 0, false
}

// firstTopLevel returns the position of the first top-level statement of body
// that unconditionally calls name.
func firstTopLevel(body *ast.BlockStmt, name string) token.Pos {
	for _, st := range body.List {
		if pos, ok := unconditionalCall(st, name); ok {
			return pos
		}
	}
	return 0
}

// checkWiring returns "" if main() unconditionally calls setupMemLimit before
// db.Open and ListenAndServe and setupMemLimit unconditionally calls
// memlimit.Apply; otherwise a description of what is wrong.
func checkWiring(f *ast.File) string {
	mainBody, setupBody := funcBody(f, "main"), funcBody(f, "setupMemLimit")
	if mainBody == nil || setupBody == nil {
		return "main or setupMemLimit missing"
	}
	if firstTopLevel(setupBody, "memlimit.Apply") == 0 {
		return "setupMemLimit does not unconditionally call memlimit.Apply"
	}
	mem := firstTopLevel(mainBody, "setupMemLimit")
	if mem == 0 {
		return "main() does not unconditionally call setupMemLimit"
	}
	var dbPos, srvPos token.Pos
	ast.Inspect(mainBody, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			name := callName(c)
			if name == "db.Open" && dbPos == 0 {
				dbPos = c.Pos()
			}
			if strings.HasSuffix(name, ".ListenAndServe") && srvPos == 0 {
				srvPos = c.Pos()
			}
		}
		return true
	})
	if dbPos == 0 || srvPos == 0 {
		return "anchor calls (db.Open / ListenAndServe) not found; update this test"
	}
	if mem > dbPos || mem > srvPos {
		return "setupMemLimit must run before db.Open and ListenAndServe"
	}
	return ""
}

func TestMemLimitWiredBeforeServers(t *testing.T) {
	if msg := checkWiring(parseMain(t, nil)); msg != "" {
		t.Fatal(msg)
	}
}

// Negative controls: the checker must reject each way the wiring can rot while
// still compiling.
func TestMemLimitWiringCheckerRejectsBrokenShapes(t *testing.T) {
	const tail = `
func setupMemLimit(ctx context.Context) *memlimit.PeakAnon { if _, err := memlimit.Apply(ctx, nil); err != nil {}; return nil }
`
	cases := map[string]string{
		"dead branch in main": `package main
func main() { if false { setupMemLimit(ctx) }; db.Open(); srv.ListenAndServe() }` + tail,
		"nested in a branch": `package main
func main() { if cond { setupMemLimit(ctx) }; db.Open(); srv.ListenAndServe() }` + tail,
		"after db.Open": `package main
func main() { db.Open(); setupMemLimit(ctx); srv.ListenAndServe() }` + tail,
		"after ListenAndServe": `package main
func main() { db.Open(); srv.ListenAndServe(); setupMemLimit(ctx) }` + tail,
		"never called": `package main
func main() { db.Open(); srv.ListenAndServe() }` + tail,
		"apply in dead branch": `package main
func main() { setupMemLimit(ctx); db.Open(); srv.ListenAndServe() }
func setupMemLimit(ctx context.Context) { if false { memlimit.Apply(ctx, nil) } }`,
		"apply removed": `package main
func main() { setupMemLimit(ctx); db.Open(); srv.ListenAndServe() }
func setupMemLimit(ctx context.Context) {}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if msg := checkWiring(parseMain(t, src)); msg == "" {
				t.Fatal("checker accepted broken wiring")
			}
		})
	}
	good := `package main
func main() { anon := setupMemLimit(ctx); db.Open(); srv.ListenAndServe() }` + tail
	if msg := checkWiring(parseMain(t, good)); msg != "" {
		t.Fatalf("checker rejected good wiring: %s", msg)
	}
}

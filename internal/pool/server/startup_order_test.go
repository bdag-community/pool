package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The full startup audits can take minutes on a preserved ledger. Keep the
// side-effecting node-health prime after every audit, before miner admission.
// This structural guard supplements, not substitutes for, live cold startup.
func TestStartupHealthPrimeFollowsFinalAccountingAudit(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "Run" {
			run = fn
		}
	}
	if run == nil {
		t.Fatal("Run missing")
	}
	var finalAudit, prime, listen token.Pos
	var audits int
	ast.Inspect(run.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok {
			if id.Name == "reconcileAcceptedAccountingAtStartup" {
				audits++
				finalAudit = call.Pos()
			}
			if id.Name == "primeRouterNodeHealth" {
				prime = call.Pos()
			}
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			if id, ok := selector.X.(*ast.Ident); ok && id.Name == "net" && selector.Sel.Name == "Listen" {
				listen = call.Pos()
			}
		}
		return true
	})
	if audits != 2 || prime == 0 || listen == 0 {
		t.Fatal("required startup audit/prime/listener changed")
	}
	if !(finalAudit < prime && prime < listen) {
		t.Fatalf("health prime at %s must follow final audit %s and precede listen %s", fset.Position(prime), fset.Position(finalAudit), fset.Position(listen))
	}
}

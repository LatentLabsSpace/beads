package dolt

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoDoltVersionCommitInsideSQLTransaction is the package-wide regression
// guard for the server-mode lost-update bug (LatentLabsSpace/NEXUS#92,
// holodeck hd-gws). Running CALL DOLT_ADD / CALL DOLT_COMMIT on a still-open
// *sql.Tx builds the Dolt commit from the session's BEGIN-time root before the
// commit-time merge, writing concurrently-committed rows back to their
// BEGIN-time values. Upstream migrated the main issue-mutation path
// (#5740/#6040); the LatentLabsSpace fork migrated this package's remaining sites onto
// postTxCommit/publishPostTx and deleted the in-tx helper.
//
// The ordering contract itself is pinned behaviorally by
// TestIssueOperationDoltCommitRunsAfterTxCommit (sequence-capturing driver).
// Driving every high-level write through that driver is impractical (each
// issues dozens of reads the driver cannot answer), so this test pins the
// structural half instead: no production file in this package may issue a
// Dolt version-control call through a transaction handle, and the deleted
// helper may not come back. A new write path must stage a postTxCommit inside
// its transaction body and call publishPostTx after the SQL commit.
//
// This is a tripwire, not a proof. It is line-based and only recognises
// transaction handles named tx, sqlTx or regularTx, so a call split across
// lines or issued through a differently-named handle escapes it. Its scope is
// this package only: the proxied unit-of-work path
// (internal/storage/uow/doltserver_tx.go) still issues an in-transaction
// DOLT_COMMIT('-Am') and is covered by upstream's own lost-update test there,
// not by this guard.
func TestNoDoltVersionCommitInsideSQLTransaction(t *testing.T) {
	// A Dolt add/commit issued through a transaction handle: either the drain
	// helper or a direct Exec/Query on a variable named like a transaction.
	inTx := regexp.MustCompile(
		`(DrainCall\(\s*\w+\s*,\s*(tx|sqlTx|regularTx)\s*,\s*"CALL DOLT_(ADD|COMMIT)` +
			`|\b(tx|sqlTx|regularTx)\.(Exec|Query)(Context)?\([^)]*"CALL DOLT_(ADD|COMMIT))`)
	helper := regexp.MustCompile(`func \([^)]*\) doltAddAndCommitInTx\(|\.doltAddAndCommitInTx\(`)

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for i, line := range strings.Split(string(src), "\n") {
			code := line
			if j := strings.Index(code, "//"); j >= 0 {
				code = code[:j] // comments may discuss the hazard freely
			}
			if inTx.MatchString(code) {
				t.Errorf("%s:%d: Dolt version-control call issued inside an open SQL transaction (NEXUS#92 lost-update ordering) — stage a postTxCommit and publishPostTx after the commit instead:\n\t%s",
					f, i+1, strings.TrimSpace(line))
			}
			if helper.MatchString(code) {
				t.Errorf("%s:%d: doltAddAndCommitInTx reintroduced — it was deleted because its in-tx ordering loses concurrent writers' rows (NEXUS#92):\n\t%s",
					f, i+1, strings.TrimSpace(line))
			}
		}
	}
	if checked == 0 {
		t.Fatal("guard scanned no production files — run from the package directory")
	}
}

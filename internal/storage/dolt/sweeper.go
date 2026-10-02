package dolt

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	storeops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

// Sweeper returns the guarded bulk-clearance surface for this store.
func (s *DoltStore) Sweeper() (issueops.Sweeper, error) {
	if s == nil {
		return nil, &storage.ErrUnsupported{Op: "Sweeper", Backend: "nil"}
	}
	return &sweeper{store: s}, nil
}

// sweeper clears closed rows from one tier inside ONE transaction.
//
// There is no shared constructor package for this role, for the reason
// cycleDetector gives: the work is a candidate query, a recheck, an optional
// reference scan and a delete that must all see one snapshot, and a transaction
// is not reachable through storage.DoltStorage. The sharing happens one level
// down — this body and the embedded store's are a few lines each around
// issueops.SweepInTx — and two wrappers over one body is still ONE vote.
type sweeper struct{ store *DoltStore }

var _ issueops.Sweeper = (*sweeper)(nil)

// Sweep clears the request's tier.
//
// VALIDATION HAPPENS BEFORE THE TRANSACTION, which makes issueops.Sweeper's "a
// refusal changes nothing" true of the connection as well as of the rows.
//
// A DRY RUN TAKES A READ TRANSACTION. It writes nothing by construction, so
// giving it a write transaction and an empty commit would make the preview
// look like a mutation to everything watching the store.
//
// THE VERSION-CONTROL ENTRY IS ONE PER SWEEP, published AFTER the SQL commit
// (LatentLabsSpace/NEXUS#92 ordering). It used to be minted INSIDE the write
// transaction to make the entry atomic with the sweep — but an in-tx
// DOLT_ADD stages whole tables from the BEGIN-time root, so a sweep racing
// any concurrent writer wrote that writer's committed rows back to their
// BEGIN-time values. A sweep's data correctness for OTHER writers outranks
// the atomicity of its own audit entry: if the trailing commit fails, the
// sweep is durable and rides the next Dolt commit (publishPostTx contract).
// An ephemeral sweep touches only the wisp tables, which this plane ignores,
// so the post-tx staged-set guard finds nothing to commit and records none.
func (s *sweeper) Sweep(ctx context.Context, req issueops.SweepRequest) (issueops.SweepResult, error) {
	if err := workapi.ValidateSweepRequest(req); err != nil {
		return issueops.SweepResult{}, err
	}

	var result issueops.SweepResult
	run := func(tx *sql.Tx) error {
		var err error
		result, err = storeops.SweepInTx(ctx, tx, req)
		return err
	}
	if req.DryRun {
		if err := s.store.withReadTx(ctx, run); err != nil {
			return issueops.SweepResult{}, err
		}
		return result, nil
	}

	var pc postTxCommit
	if err := s.store.withWriteTx(ctx, func(tx *sql.Tx) error {
		pc = postTxCommit{}
		if err := run(tx); err != nil {
			return err
		}
		if result.Swept == 0 {
			return nil
		}
		pc.stage(sweptTables, fmt.Sprintf("bd: sweep %d %s bead(s)", result.Swept, req.Tier))
		return nil
	}); err != nil {
		return issueops.SweepResult{}, err
	}
	s.store.publishPostTx(ctx, pc)
	return result, nil
}

// sweptTables are the versioned tables a sweep can touch, staged before the
// commit. It is the same list DeleteIssues stages, because a sweep IS a
// delete of a selected set.
var sweptTables = []string{
	"issues", "dependencies", "labels", "comments", "events", "provenance_events",
	"child_counters", "issue_snapshots", "compaction_snapshots",
}

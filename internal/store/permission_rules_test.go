package store

import (
	"context"
	"path/filepath"
	"testing"
)

func permissionOwnerFixture() PermissionOwner {
	return PermissionOwner{Platform: "telegram", ChannelID: "chat1", ThreadID: "", UserID: "user1"}
}

func TestPermissionRuleRoundTrip(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PermissionRuleRepo()
	owner := permissionOwnerFixture()

	id, err := repo.Add(ctx, owner, "bash", []string{"git push origin main"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if id == 0 {
		t.Fatal("Add returned id 0")
	}

	rules, err := repo.ListByOwner(ctx, owner)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(rules) != 1 || rules[0].ID != id || rules[0].Tool != "bash" || rules[0].Patterns != "git push origin main" {
		t.Fatalf("rules = %+v", rules)
	}

	matched, err := repo.Match(ctx, owner, "bash", []string{"git push origin main"})
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if matched == nil || matched.ID != id {
		t.Fatalf("Match result = %+v", matched)
	}

	if err := repo.DeleteByID(ctx, owner, id); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}
	rules, err = repo.ListByOwner(ctx, owner)
	if err != nil {
		t.Fatalf("ListByOwner after delete: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("rules after delete = %+v", rules)
	}
}

func TestPermissionRuleCanonicalization(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PermissionRuleRepo()
	owner := permissionOwnerFixture()

	if _, err := repo.Add(ctx, owner, "write", []string{"b", "a", "b"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	rules, err := repo.ListByOwner(ctx, owner)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("rules = %+v, want a single deduped row", rules)
	}
	if rules[0].Patterns != "a|b" {
		t.Fatalf("canonical patterns = %q, want %q", rules[0].Patterns, "a|b")
	}

	// Equivalent ask with a different order matches.
	matched, err := repo.Match(ctx, owner, "write", []string{"a", "b"})
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if matched == nil {
		t.Fatal("reordered equivalent patterns did not match")
	}

	// A genuinely different pattern must not match.
	matched, err = repo.Match(ctx, owner, "write", []string{"a", "c"})
	if err != nil {
		t.Fatalf("Match different: %v", err)
	}
	if matched != nil {
		t.Fatalf("different pattern set matched rule %+v", matched)
	}

	// A different tool must not match.
	matched, err = repo.Match(ctx, owner, "edit", []string{"a", "b"})
	if err != nil {
		t.Fatalf("Match different tool: %v", err)
	}
	if matched != nil {
		t.Fatalf("different tool matched rule %+v", matched)
	}
}

func TestPermissionRuleOwnerIsolation(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PermissionRuleRepo()

	ownerA := permissionOwnerFixture()
	ownerB := PermissionOwner{Platform: "telegram", ChannelID: "chat1", ThreadID: "thread-1", UserID: ""}

	idA, err := repo.Add(ctx, ownerA, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Add A: %v", err)
	}
	idB, err := repo.Add(ctx, ownerB, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Add B: %v", err)
	}

	rulesA, err := repo.ListByOwner(ctx, ownerA)
	if err != nil {
		t.Fatalf("ListByOwner A: %v", err)
	}
	if len(rulesA) != 1 || rulesA[0].ID != idA {
		t.Fatalf("rules A = %+v", rulesA)
	}

	matched, err := repo.Match(ctx, ownerA, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Match A: %v", err)
	}
	if matched == nil || matched.ID != idA {
		t.Fatalf("Match A = %+v", matched)
	}

	// Thread-scoped rule does not leak into the other conversation.
	rulesB, err := repo.ListByOwner(ctx, ownerA)
	if err != nil {
		t.Fatalf("ListByOwner A again: %v", err)
	}
	if len(rulesB) != 1 {
		t.Fatalf("owner A saw %d rules, want 1", len(rulesB))
	}

	// Deleting in one owner leaves the other untouched.
	if err := repo.DeleteByID(ctx, ownerA, idB); err != nil {
		t.Fatalf("cross-owner delete: %v", err)
	}
	matchedB, err := repo.Match(ctx, ownerB, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Match B: %v", err)
	}
	if matchedB == nil || matchedB.ID != idB {
		t.Fatalf("owner B rule was affected by owner A delete: %+v", matchedB)
	}

	// Clear is owner-scoped too.
	if err := repo.ClearByOwner(ctx, ownerA); err != nil {
		t.Fatalf("ClearByOwner A: %v", err)
	}
	matchedB, err = repo.Match(ctx, ownerB, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Match B after clear A: %v", err)
	}
	if matchedB == nil {
		t.Fatal("clear in owner A removed owner B's rule")
	}
}

func TestPermissionRuleAddIsIdempotent(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PermissionRuleRepo()
	owner := permissionOwnerFixture()

	first, err := repo.Add(ctx, owner, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Add first: %v", err)
	}
	second, err := repo.Add(ctx, owner, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Add second: %v", err)
	}
	if first != second {
		t.Fatalf("duplicate add returned a new id: %d vs %d", first, second)
	}
	rules, err := repo.ListByOwner(ctx, owner)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("rules = %+v, want 1 row", rules)
	}
}

func TestPermissionRuleMigrationFromV7PreservesRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate.db")
	ctx := context.Background()

	s1, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	owner := permissionOwnerFixture()
	if err := s1.SessionRepo().SetActive(ctx, owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID, "sess-1", 0); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := s1.db.Exec("DROP TABLE permission_rule"); err != nil {
		t.Fatalf("drop permission_rule: %v", err)
	}
	if _, err := s1.db.Exec("PRAGMA user_version=7"); err != nil {
		t.Fatalf("stamp user_version=7: %v", err)
	}
	_ = s1.Close()

	s2, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	var version int
	if err := s2.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", version, SchemaVersion)
	}

	sessionID, _, err := s2.SessionRepo().Active(ctx, owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID)
	if err != nil {
		t.Fatalf("Active after migration: %v", err)
	}
	if sessionID != "sess-1" {
		t.Fatalf("session after migration = %q, want sess-1", sessionID)
	}

	// The freshly-created table must not pre-match anything and must accept
	// new rules.
	matched, err := s2.PermissionRuleRepo().Match(ctx, owner, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Match after migration: %v", err)
	}
	if matched != nil {
		t.Fatalf("fresh permission_rule pre-matched %+v", matched)
	}
	if _, err := s2.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"git status"}); err != nil {
		t.Fatalf("Add after migration: %v", err)
	}
	matched, err = s2.PermissionRuleRepo().Match(ctx, owner, "bash", []string{"git status"})
	if err != nil {
		t.Fatalf("Match after Add: %v", err)
	}
	if matched == nil {
		t.Fatal("added rule after migration did not match")
	}
}

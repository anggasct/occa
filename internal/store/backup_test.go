package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func seedBackupDB(t *testing.T, path, mark string) *SQLiteStore {
	t.Helper()
	s, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "alice", "sess-"+mark, 100); err != nil {
		t.Fatalf("session: %v", err)
	}
	if err := s.ChannelRepo().UpsertModel(ctx, "discord", "chan1", "provider/model"); err != nil {
		t.Fatalf("channel: %v", err)
	}
	if err := s.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "alice", "admin"); err != nil {
		t.Fatalf("override: %v", err)
	}
	if _, err := s.ScheduleRepo().Create(ctx, &Schedule{Platform: "telegram", ChannelID: "chat1", CronExpression: "0 * * * *", HumanSchedule: "hourly", Prompt: "remind " + mark, Enabled: true}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if _, err := s.PermissionRuleRepo().Add(ctx, PermissionOwner{Platform: "telegram", ChannelID: "chat1"}, "bash", []string{"ls " + mark}); err != nil {
		t.Fatalf("permission: %v", err)
	}
	return s
}

func digestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func mustClose(t *testing.T, s *SQLiteStore) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func assertReportCounts(t *testing.T, counts []RowCount, want map[string]int64) {
	t.Helper()
	got := make(map[string]int64, len(counts))
	for _, rc := range counts {
		got[rc.Table] = rc.Rows
	}
	for table, expected := range want {
		if got[table] != expected {
			t.Errorf("%s rows = %d, want %d", table, got[table], expected)
		}
	}
}

func globLeftovers(t *testing.T, dir, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	return matches
}

func TestBackupWhileLiveIsConsistent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedBackupDB(t, dbPath, "live")

	out := filepath.Join(dir, "live-backup.db")
	report, err := BackupFile(dbPath, out, false)
	if err != nil {
		t.Fatalf("BackupFile while live: %v", err)
	}
	if report.SHA256 == "" {
		t.Error("report sha256 is empty")
	}
	if report.Bytes == 0 {
		t.Error("report bytes is zero")
	}
	assertReportCounts(t, report.RowCounts, map[string]int64{
		"session": 1, "channel": 1, "user_override": 1, "schedule": 1, "permission_rule": 1,
	})

	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("stat backup: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("backup perms = %o, want 600", info.Mode().Perm())
	}

	restored, err := OpenWithDefaultWorkdir(out, "")
	if err != nil {
		t.Fatalf("reopen backup: %v", err)
	}
	defer func() { _ = restored.Close() }()
	ctx := context.Background()
	id, _, err := restored.SessionRepo().Active(ctx, "telegram", "chat1", "", "alice")
	if err != nil {
		t.Fatalf("active: %v", err)
	}
	if id != "sess-live" {
		t.Fatalf("backup session = %q, want sess-live", id)
	}
	schedules, err := restored.ScheduleRepo().List(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("schedules: %v", err)
	}
	if len(schedules) != 1 || schedules[0].Prompt != "remind live" {
		t.Fatalf("backup schedules = %+v, want remind live", schedules)
	}
}

func TestBackupRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.db")
	seedBackupDB(t, dbPath, "a")
	out := filepath.Join(dir, "out.db")
	if err := os.WriteFile(out, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := BackupFile(dbPath, out, false); err == nil || !strings.Contains(err.Error(), "refuse to overwrite") {
		t.Fatalf("expected refuse-to-overwrite error, got %v", err)
	}
	report, err := BackupFile(dbPath, out, true)
	if err != nil {
		t.Fatalf("forced backup: %v", err)
	}
	if report.SHA256 == "" {
		t.Error("forced backup sha256 empty")
	}
}

func TestBackupRefusesDatabaseItself(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.db")
	seedBackupDB(t, dbPath, "a")
	if _, err := BackupFile(dbPath, dbPath, true); err == nil {
		t.Fatal("expected error backing up onto the database itself")
	}
}

func TestBackupRefusesMissingDatabase(t *testing.T) {
	dir := t.TempDir()
	if _, err := BackupFile(filepath.Join(dir, "nope.db"), filepath.Join(dir, "out.db"), false); err == nil {
		t.Fatal("expected error for missing database")
	}
}

func TestLockDBExcludesSecondInstanceAndCheckRunning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.db")

	lock, err := LockDB(dbPath)
	if err != nil {
		t.Fatalf("LockDB: %v", err)
	}
	if _, err := LockDB(dbPath); !errors.Is(err, ErrDBInUse) {
		t.Fatalf("second LockDB = %v, want ErrDBInUse", err)
	}
	if err := CheckRunning(dbPath); !errors.Is(err, ErrDBInUse) {
		t.Fatalf("CheckRunning while locked = %v, want ErrDBInUse", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := CheckRunning(dbPath); err != nil {
		t.Fatalf("CheckRunning after unlock = %v, want nil", err)
	}
	lock2, err := LockDB(dbPath)
	if err != nil {
		t.Fatalf("LockDB after unlock: %v", err)
	}
	_ = lock2.Unlock()
}

func TestCheckRunningMissingLockFile(t *testing.T) {
	if err := CheckRunning(filepath.Join(t.TempDir(), "db.db")); err != nil {
		t.Fatalf("CheckRunning without lock file = %v, want nil", err)
	}
}

func TestRestoreRefusesWhileRunning(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.db")
	seedBackupDB(t, dbPath, "target")
	before := digestFile(t, dbPath)

	backup := filepath.Join(dir, "backup.db")
	if _, err := BackupFile(dbPath, backup, false); err != nil {
		t.Fatalf("BackupFile: %v", err)
	}

	lock, err := LockDB(dbPath)
	if err != nil {
		t.Fatalf("LockDB: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	_, err = RestoreFile(dbPath, backup, false)
	if !errors.Is(err, ErrDBInUse) {
		t.Fatalf("RestoreFile while running = %v, want ErrDBInUse", err)
	}
	if got := digestFile(t, dbPath); got != before {
		t.Fatal("database changed by refused restore")
	}
	if leftovers := globLeftovers(t, dir, "*.pre-restore.*.db"); len(leftovers) != 0 {
		t.Fatalf("refused restore left pre-restore files: %v", leftovers)
	}
}

func TestRestoreRoundTripPreservesData(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	seedBackupDB(t, srcPath, "src")
	backup := filepath.Join(dir, "backup.db")
	backupReport, err := BackupFile(srcPath, backup, false)
	if err != nil {
		t.Fatalf("BackupFile: %v", err)
	}

	tgtPath := filepath.Join(dir, "tgt.db")
	s := seedBackupDB(t, tgtPath, "tgt")
	mustClose(t, s)

	report, err := RestoreFile(tgtPath, backup, false)
	if err != nil {
		t.Fatalf("RestoreFile: %v", err)
	}
	assertReportCounts(t, report.RowCounts, map[string]int64{
		"session": 1, "channel": 1, "user_override": 1, "schedule": 1, "permission_rule": 1,
	})
	if report.SHA256 != backupReport.SHA256 {
		t.Fatalf("restored sha256 = %s, want backup %s", report.SHA256, backupReport.SHA256)
	}
	if report.PreRestoreBackup == "" {
		t.Error("restore report missing pre-restore backup name")
	}

	restored, err := OpenWithDefaultWorkdir(tgtPath, "")
	if err != nil {
		t.Fatalf("reopen restored: %v", err)
	}
	defer func() { _ = restored.Close() }()
	ctx := context.Background()
	id, _, err := restored.SessionRepo().Active(ctx, "telegram", "chat1", "", "alice")
	if err != nil || id != "sess-src" {
		t.Fatalf("restored session = %q err %v, want sess-src", id, err)
	}
	schedules, err := restored.ScheduleRepo().List(ctx, "telegram", "chat1")
	if err != nil || len(schedules) != 1 || schedules[0].Prompt != "remind src" {
		t.Fatalf("restored schedules = %+v err %v, want remind src", schedules, err)
	}
	rules, err := restored.PermissionRuleRepo().ListByOwner(ctx, PermissionOwner{Platform: "telegram", ChannelID: "chat1"})
	if err != nil || len(rules) != 1 || rules[0].Tool != "bash" || !strings.Contains(rules[0].Patterns, "src") {
		t.Fatalf("restored permission rules = %+v err %v, want bash ls src", rules, err)
	}
	ch, err := restored.ChannelRepo().Get(ctx, "discord", "chan1")
	if err != nil || ch == nil || ch.Model != "provider/model" {
		t.Fatalf("restored channel = %+v err %v, want provider/model", ch, err)
	}
	override, err := restored.OverrideRepo().Get(ctx, "telegram", "chat1", "alice")
	if err != nil || override == nil || override.Role != "admin" {
		t.Fatalf("restored override = %+v err %v, want admin", override, err)
	}
}

func TestRestoreRejectsCorruptInputKeepsTarget(t *testing.T) {
	dir := t.TempDir()
	tgtPath := filepath.Join(dir, "tgt.db")
	s := seedBackupDB(t, tgtPath, "target")
	mustClose(t, s)
	before := digestFile(t, tgtPath)

	corrupt := filepath.Join(dir, "corrupt.db")
	if err := os.WriteFile(corrupt, []byte("this is not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := RestoreFile(tgtPath, corrupt, false); err == nil {
		t.Fatal("expected corrupt input to be rejected")
	}
	if got := digestFile(t, tgtPath); got != before {
		t.Fatal("target changed by failed restore")
	}
	if leftovers := globLeftovers(t, dir, "*.pre-restore.*.db"); len(leftovers) != 0 {
		t.Fatalf("failed restore left pre-restore files: %v", leftovers)
	}
	if leftovers := globLeftovers(t, dir, ".occa-restore-*"); len(leftovers) != 0 {
		t.Fatalf("failed restore left temp files: %v", leftovers)
	}
}

func TestRestoreRejectsNonOccaDatabase(t *testing.T) {
	dir := t.TempDir()
	tgtPath := filepath.Join(dir, "tgt.db")
	s := seedBackupDB(t, tgtPath, "target")
	mustClose(t, s)

	foreign := filepath.Join(dir, "foreign.db")
	fdb, err := sql.Open("sqlite", foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fdb.Exec(`CREATE TABLE unrelated (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	_ = fdb.Close()

	if _, err := RestoreFile(tgtPath, foreign, false); err == nil || !strings.Contains(err.Error(), "not an occa database") {
		t.Fatalf("expected not-an-occa-database error, got %v", err)
	}
}

func TestRestoreRejectsNewerSchema(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	seedBackupDB(t, srcPath, "src")
	backup := filepath.Join(dir, "future.db")
	if _, err := BackupFile(srcPath, backup, false); err != nil {
		t.Fatalf("BackupFile: %v", err)
	}
	bdb, err := sql.Open("sqlite", backup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bdb.Exec("PRAGMA user_version=999"); err != nil {
		t.Fatal(err)
	}
	_ = bdb.Close()

	tgtPath := filepath.Join(dir, "tgt.db")
	s := seedBackupDB(t, tgtPath, "target")
	mustClose(t, s)

	if _, err := RestoreFile(tgtPath, backup, true); err == nil || !strings.Contains(err.Error(), "newer than this binary supports") {
		t.Fatalf("expected newer-schema rejection, got %v", err)
	}
}

func TestRestoreRefusesOlderSchemaWithoutForce(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	seedBackupDB(t, srcPath, "src")
	backup := filepath.Join(dir, "old.db")
	if _, err := BackupFile(srcPath, backup, false); err != nil {
		t.Fatalf("BackupFile: %v", err)
	}
	bdb, err := sql.Open("sqlite", backup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bdb.Exec("PRAGMA user_version=5"); err != nil {
		t.Fatal(err)
	}
	_ = bdb.Close()

	tgtPath := filepath.Join(dir, "tgt.db")
	s := seedBackupDB(t, tgtPath, "target")
	mustClose(t, s)

	if _, err := RestoreFile(tgtPath, backup, false); err == nil || !strings.Contains(err.Error(), "older") {
		t.Fatalf("expected older-schema refusal without force, got %v", err)
	}
	if _, err := RestoreFile(tgtPath, backup, true); err != nil {
		t.Fatalf("forced older-schema restore: %v", err)
	}

	ro, err := sql.Open("sqlite", "file:"+filepath.ToSlash(tgtPath)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	var version int
	if err := ro.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 5 {
		t.Fatalf("restored user_version = %d, want 5", version)
	}
	var n int
	if err := ro.QueryRow("SELECT COUNT(*) FROM session").Scan(&n); err != nil || n != 1 {
		t.Fatalf("restored session count = %d err %v, want 1", n, err)
	}
}

func TestRestorePreRestoreBackupIsRollbackable(t *testing.T) {
	dir := t.TempDir()
	tgtPath := filepath.Join(dir, "tgt.db")
	s := seedBackupDB(t, tgtPath, "target")
	mustClose(t, s)

	srcPath := filepath.Join(dir, "src.db")
	seedBackupDB(t, srcPath, "src")
	backup := filepath.Join(dir, "backup.db")
	if _, err := BackupFile(srcPath, backup, false); err != nil {
		t.Fatalf("BackupFile: %v", err)
	}

	report, err := RestoreFile(tgtPath, backup, false)
	if err != nil {
		t.Fatalf("RestoreFile: %v", err)
	}
	preFiles := globLeftovers(t, dir, "*.pre-restore.*.db")
	if len(preFiles) != 1 {
		t.Fatalf("expected exactly one pre-restore backup, got %v", preFiles)
	}
	if filepath.Base(preFiles[0]) != report.PreRestoreBackup {
		t.Fatalf("report pre-restore backup %q != on-disk %q", report.PreRestoreBackup, filepath.Base(preFiles[0]))
	}
	info, err := os.Stat(preFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("pre-restore backup perms = %o, want 600", info.Mode().Perm())
	}

	rolledBack, err := OpenWithDefaultWorkdir(preFiles[0], "")
	if err != nil {
		t.Fatalf("open pre-restore backup: %v", err)
	}
	defer func() { _ = rolledBack.Close() }()
	ctx := context.Background()
	id, _, err := rolledBack.SessionRepo().Active(ctx, "telegram", "chat1", "", "alice")
	if err != nil || id != "sess-target" {
		t.Fatalf("pre-restore backup session = %q err %v, want sess-target", id, err)
	}
	schedules, err := rolledBack.ScheduleRepo().List(ctx, "telegram", "chat1")
	if err != nil || len(schedules) != 1 || schedules[0].Prompt != "remind target" {
		t.Fatalf("pre-restore backup schedules = %+v err %v, want remind target", schedules, err)
	}
}

func TestRestoreRefusesMissingTarget(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.db")
	seedBackupDB(t, input, "src")
	backup := filepath.Join(dir, "backup.db")
	if _, err := BackupFile(input, backup, false); err != nil {
		t.Fatalf("BackupFile: %v", err)
	}
	if _, err := RestoreFile(filepath.Join(dir, "missing.db"), backup, false); err == nil {
		t.Fatal("expected error restoring into a missing database")
	}
}

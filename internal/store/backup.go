package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// ErrDBInUse is returned when the operator database is already locked by a
// running occa process (or another restore/daemon attempt). Restore refuses
// while the service is running because replacing a live WAL database would
// corrupt the open connection.
var ErrDBInUse = errors.New("store: database is in use")

// RowCount is the number of rows in one occa table, used in verification
// output. Counts are safe to print; database cell contents are not.
type RowCount struct {
	Table string
	Rows  int64
}

// BackupReport describes a completed backup: checksum, size, schema version,
// and row counts. It never carries database contents.
type BackupReport struct {
	SHA256        string
	Bytes         int64
	SchemaVersion int
	RowCounts     []RowCount
}

// RestoreReport describes a completed restore, including the automatically
// created pre-restore backup (leaf name) used to roll back.
type RestoreReport struct {
	SHA256           string
	Bytes            int64
	SchemaVersion    int
	RowCounts        []RowCount
	PreRestoreBackup string
}

// DBLock is an exclusive advisory lock on the database (flock on a sidecar
// <db>.lock file). It is held for the lifetime of the occa process so restore
// can refuse to replace the database while the service is running, and so a
// second occa instance cannot start against the same database.
type DBLock struct {
	fd int
}

var backupTables = []string{
	"session",
	"channel",
	"user_override",
	"schedule",
	"progress_notice",
	"thread_config",
	"permission_rule",
}

var requiredTables = []string{"session", "channel", "user_override", "schedule"}

// LockDB acquires the exclusive database lock, creating the lock file if
// needed. It fails with ErrDBInUse when another occa process holds the lock.
func LockDB(dbPath string) (*DBLock, error) {
	fd, err := syscall.Open(dbPath+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open database lock: %w", err)
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = syscall.Close(fd)
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w (another occa process is running)", ErrDBInUse)
		}
		return nil, fmt.Errorf("store: lock database: %w", err)
	}
	return &DBLock{fd: fd}, nil
}

// Unlock releases the database lock.
func (l *DBLock) Unlock() error {
	if l == nil || l.fd < 0 {
		return nil
	}
	_ = syscall.Flock(l.fd, syscall.LOCK_UN)
	err := syscall.Close(l.fd)
	l.fd = -1
	if err != nil {
		return fmt.Errorf("store: close database lock: %w", err)
	}
	return nil
}

// CheckRunning reports ErrDBInUse when an occa process currently holds the
// database lock. A missing or stale lock file means the service is not
// running.
func CheckRunning(dbPath string) error {
	fd, err := syscall.Open(dbPath+".lock", syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return fmt.Errorf("store: check database lock: %w", err)
	}
	defer func() { _ = syscall.Close(fd) }()
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrDBInUse
		}
		return fmt.Errorf("store: check database lock: %w", err)
	}
	return nil
}

// BackupFile writes a SQLite-consistent backup of the database at dbPath to
// outputPath using VACUUM INTO, which snapshots the database even while a
// live occa process is writing to it in WAL mode. It refuses to overwrite an
// existing output unless force is set.
func BackupFile(dbPath, outputPath string, force bool) (*BackupReport, error) {
	srcAbs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("store: resolve database path: %w", err)
	}
	outAbs, err := filepath.Abs(outputPath)
	if err != nil {
		return nil, fmt.Errorf("store: resolve output path: %w", err)
	}
	if filepath.Clean(srcAbs) == filepath.Clean(outAbs) {
		return nil, errors.New("store: backup output is the database file itself")
	}
	if _, err := os.Stat(srcAbs); err != nil {
		return nil, fmt.Errorf("store: database not found: %w", err)
	}
	if _, err := validateDatabase(srcAbs); err != nil {
		return nil, err
	}
	if _, err := os.Stat(outAbs); err == nil {
		if !force {
			return nil, errors.New("store: refuse to overwrite existing backup (use --force)")
		}
	}

	tmp, err := os.CreateTemp(filepath.Dir(outAbs), ".occa-backup-*")
	if err != nil {
		return nil, fmt.Errorf("store: create backup temp file: %w", err)
	}
	tmpName := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, fmt.Errorf("store: close backup temp file: %w", err)
	}
	if err := os.Remove(tmpName); err != nil {
		return nil, fmt.Errorf("store: prepare backup temp file: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	db, err := sql.Open("sqlite", srcAbs)
	if err != nil {
		return nil, fmt.Errorf("store: open database for backup: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return nil, fmt.Errorf("store: backup busy timeout: %w", err)
	}
	target := strings.ReplaceAll(tmpName, "'", "''")
	if _, err := db.Exec("VACUUM INTO '" + target + "'"); err != nil {
		return nil, fmt.Errorf("store: backup: %w", err)
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return nil, fmt.Errorf("store: backup permissions: %w", err)
	}

	version, err := validateDatabase(tmpName)
	if err != nil {
		return nil, err
	}
	sum, size, counts, err := inspectDatabase(tmpName)
	if err != nil {
		return nil, err
	}
	if err := os.Rename(tmpName, outAbs); err != nil {
		return nil, fmt.Errorf("store: publish backup: %w", err)
	}
	committed = true
	if err := syncDir(filepath.Dir(outAbs)); err != nil {
		slog.Warn("store: sync backup directory", "error", err)
	}
	return &BackupReport{SHA256: sum, Bytes: size, SchemaVersion: version, RowCounts: counts}, nil
}

// RestoreFile replaces the database at dbPath with an exact copy of inputPath
// after validating the input, creating a timestamped pre-restore backup, and
// atomically swapping the file. It refuses while occa is running, refuses
// corrupt/non-occa/newer-schema inputs, and requires --force to replace a
// newer database with an older-schema backup.
func RestoreFile(dbPath, inputPath string, force bool) (*RestoreReport, error) {
	dbAbs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("store: resolve database path: %w", err)
	}
	inAbs, err := filepath.Abs(inputPath)
	if err != nil {
		return nil, fmt.Errorf("store: resolve input path: %w", err)
	}
	if filepath.Clean(dbAbs) == filepath.Clean(inAbs) {
		return nil, errors.New("store: restore input is the database file itself")
	}
	if _, err := os.Stat(dbAbs); err != nil {
		return nil, fmt.Errorf("store: database not found: %w", err)
	}
	if _, err := os.Stat(inAbs); err != nil {
		return nil, fmt.Errorf("store: restore input not found: %w", err)
	}
	lock, err := LockDB(dbAbs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Unlock() }()

	inVersion, err := validateDatabase(inAbs)
	if err != nil {
		return nil, err
	}
	curVersion, err := currentVersion(dbAbs)
	if err != nil {
		return nil, err
	}
	if inVersion < curVersion && !force {
		return nil, fmt.Errorf("store: refusing to replace schema version %d with older %d (use --force)", curVersion, inVersion)
	}

	prePath := fmt.Sprintf("%s.pre-restore.%d.db", dbAbs, time.Now().UnixNano())
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(prePath)
		}
	}()
	if _, err := BackupFile(dbAbs, prePath, false); err != nil {
		return nil, fmt.Errorf("store: pre-restore backup: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dbAbs), ".occa-restore-*")
	if err != nil {
		return nil, fmt.Errorf("store: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	in, err := os.Open(inAbs)
	if err != nil {
		return nil, fmt.Errorf("store: open restore input: %w", err)
	}
	_, err = io.Copy(tmp, in)
	closeErr := in.Close()
	if err != nil {
		return nil, fmt.Errorf("store: copy restore input: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("store: close restore input: %w", closeErr)
	}
	if err := tmp.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("store: temp file permissions: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return nil, fmt.Errorf("store: sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("store: close temp file: %w", err)
	}

	version, err := validateDatabase(tmpName)
	if err != nil {
		return nil, fmt.Errorf("store: validate restore input: %w", err)
	}
	sum, size, counts, err := inspectDatabase(tmpName)
	if err != nil {
		return nil, err
	}
	if err := checkpointDatabase(dbAbs); err != nil {
		return nil, err
	}
	if err := removeJournalFiles(dbAbs); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpName, dbAbs); err != nil {
		return nil, fmt.Errorf("store: replace database: %w", err)
	}
	committed = true
	if err := syncDir(filepath.Dir(dbAbs)); err != nil {
		slog.Warn("store: sync database directory", "error", err)
	}

	return &RestoreReport{
		SHA256:           sum,
		Bytes:            size,
		SchemaVersion:    version,
		RowCounts:        counts,
		PreRestoreBackup: filepath.Base(prePath),
	}, nil
}

func checkpointDatabase(path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("store: open database for checkpoint: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return fmt.Errorf("store: checkpoint busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("store: checkpoint database: %w", err)
	}
	return nil
}

func removeJournalFiles(path string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("store: remove database journal: %w", err)
		}
	}
	return nil
}

// inspectDatabase computes the checksum, size, and per-table row counts of a
// validated database file.
func inspectDatabase(path string) (string, int64, []RowCount, error) {
	sum, size, err := fileDigest(path)
	if err != nil {
		return "", 0, nil, err
	}
	ro, err := sql.Open("sqlite", readOnlyDSN(path))
	if err != nil {
		return "", 0, nil, fmt.Errorf("store: open database for inspection: %w", err)
	}
	defer func() { _ = ro.Close() }()
	counts := make([]RowCount, 0, len(backupTables))
	for _, table := range backupTables {
		var exists int
		if err := ro.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&exists); err != nil {
			return "", 0, nil, fmt.Errorf("store: inspect schema: %w", err)
		}
		if exists == 0 {
			continue
		}
		var n int64
		if err := ro.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
			return "", 0, nil, fmt.Errorf("store: count %s: %w", table, err)
		}
		counts = append(counts, RowCount{Table: table, Rows: n})
	}
	return sum, size, counts, nil
}

// validateDatabase checks that path is a readable, integrity-clean occa
// database with a schema this binary can migrate, and returns its schema
// version (0 means a legacy database the migrations will adopt).
func validateDatabase(path string) (int, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0, fmt.Errorf("store: resolve database path: %w", err)
	}
	if _, err := os.Stat(abs); err != nil {
		return 0, fmt.Errorf("store: database not found: %w", err)
	}
	db, err := sql.Open("sqlite", readOnlyDSN(abs))
	if err != nil {
		return 0, fmt.Errorf("store: open database for validation: %w", err)
	}
	defer func() { _ = db.Close() }()

	var ok string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&ok); err != nil {
		return 0, fmt.Errorf("store: database integrity check failed: %w", err)
	}
	if ok != "ok" {
		return 0, fmt.Errorf("store: database integrity check failed: %s", firstLine(ok))
	}

	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("store: read schema version: %w", err)
	}
	if version > schemaVersion {
		return 0, fmt.Errorf("store: schema version %d is newer than this binary supports (%d)", version, schemaVersion)
	}
	for _, table := range requiredTablesForVersion(version) {
		if !hasTable(db, table) {
			return 0, fmt.Errorf("store: not an occa database (missing table %s)", table)
		}
	}
	for table, columns := range requiredColumnsForVersion(version) {
		if err := requireColumns(db, table, columns); err != nil {
			return 0, err
		}
	}
	return version, nil
}

func requiredTablesForVersion(version int) []string {
	tables := append([]string(nil), requiredTables...)
	if version >= 5 {
		tables = append(tables, "progress_notice")
	}
	if version >= 7 {
		tables = append(tables, "thread_config")
	}
	if version >= 8 {
		tables = append(tables, "permission_rule")
	}
	return tables
}

func requiredColumnsForVersion(version int) map[string][]string {
	columns := map[string][]string{
		"session":       {"id", "channel_id", "platform", "agent_session_id", "active", "created_at", "updated_at"},
		"channel":       {"channel_id", "platform", "model", "listen_mode", "workdir", "created_at", "updated_at"},
		"user_override": {"id", "channel_id", "platform", "user_id", "role", "model", "created_at", "updated_at"},
		"schedule":      {"id", "channel_id", "platform", "cron_expression", "human_schedule", "prompt", "enabled", "created_at", "updated_at"},
	}
	if version >= 2 {
		columns["session"] = append(columns["session"], "thread_id", "user_id")
		columns["channel"] = append(columns["channel"], "auto_thread")
	}
	if version >= 3 {
		columns["session"] = append(columns["session"], "agent_pid")
	}
	if version >= 4 {
		columns["session"] = append(columns["session"], "title")
	}
	if version >= 5 {
		columns["progress_notice"] = []string{"id", "platform", "channel_id", "thread_id", "message_id", "created_at"}
	}
	if version >= 6 {
		columns["session"] = append(columns["session"], "model")
	}
	if version >= 7 {
		columns["thread_config"] = []string{"id", "platform", "channel_id", "thread_id", "workdir", "model", "listen_mode", "created_at", "updated_at"}
	}
	if version >= 8 {
		columns["permission_rule"] = []string{"id", "platform", "channel_id", "thread_id", "user_id", "tool", "patterns", "created_at"}
	}
	return columns
}

func hasTable(db *sql.DB, table string) bool {
	var exists int
	return db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&exists) == nil && exists == 1
}

func requireColumns(db *sql.DB, table string, columns []string) error {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return fmt.Errorf("store: inspect table %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	found := make(map[string]struct{}, len(columns))
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("store: inspect table %s: %w", table, err)
		}
		found[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: inspect table %s: %w", table, err)
	}
	for _, column := range columns {
		if _, ok := found[column]; !ok {
			return fmt.Errorf("store: incompatible schema (table %s missing column %s)", table, column)
		}
	}
	return nil
}

func currentVersion(dbPath string) (int, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return 0, fmt.Errorf("store: resolve database path: %w", err)
	}
	db, err := sql.Open("sqlite", readOnlyDSN(abs))
	if err != nil {
		return 0, fmt.Errorf("store: open current database: %w", err)
	}
	defer func() { _ = db.Close() }()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("store: read current database version: %w", err)
	}
	return version, nil
}

func readOnlyDSN(absPath string) string {
	return "file:" + filepath.ToSlash(absPath) + "?mode=ro&_pragma=busy_timeout(5000)"
}

func fileDigest(path string) (sum string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("store: open for checksum: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("store: checksum: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

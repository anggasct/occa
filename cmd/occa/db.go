package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func runDBCommand(args []string) int {
	if len(args) == 0 {
		dbUsage()
		return 0
	}
	switch args[0] {
	case "backup":
		return runDBBackup(args[1:])
	case "restore":
		return runDBRestore(args[1:])
	case "help", "-h", "--help":
		dbUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "occa db: unknown command %q\n\n", args[0])
		dbUsage()
		return 2
	}
}

func runDBBackup(args []string) int {
	fs := flag.NewFlagSet("occa db backup", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var configPath, output string
	var force bool
	fs.StringVar(&configPath, "config", "", "path to occa config file")
	fs.StringVar(&configPath, "c", "", "path to occa config file (shorthand)")
	fs.StringVar(&output, "output", "", "backup output path")
	fs.StringVar(&output, "o", "", "backup output path (shorthand)")
	fs.BoolVar(&force, "force", false, "overwrite an existing backup file")
	fs.BoolVar(&force, "f", false, "overwrite an existing backup file (shorthand)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if output == "" {
		fmt.Fprintln(os.Stderr, "occa db backup: --output is required")
		fs.Usage()
		return 2
	}
	dbPath, err := config.DBPath(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa db backup: %s\n", safeDBError(err))
		return 1
	}
	report, err := store.BackupFile(dbPath, output, force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa db backup: %s\n", safeDBError(err))
		return 1
	}
	printBackupReport(report)
	return 0
}

func runDBRestore(args []string) int {
	fs := flag.NewFlagSet("occa db restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var configPath, input string
	var force bool
	fs.StringVar(&configPath, "config", "", "path to occa config file")
	fs.StringVar(&configPath, "c", "", "path to occa config file (shorthand)")
	fs.StringVar(&input, "input", "", "backup file to restore")
	fs.StringVar(&input, "i", "", "backup file to restore (shorthand)")
	fs.BoolVar(&force, "force", false, "restore over a newer schema database (creates a pre-restore backup either way)")
	fs.BoolVar(&force, "f", false, "restore over a newer schema database (shorthand)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if input == "" {
		fmt.Fprintln(os.Stderr, "occa db restore: --input is required")
		fs.Usage()
		return 2
	}
	dbPath, err := config.DBPath(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa db restore: %s\n", safeDBError(err))
		return 1
	}
	report, err := store.RestoreFile(dbPath, input, force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa db restore: %s\n", safeDBError(err))
		return 1
	}
	fmt.Println("restore complete")
	fmt.Printf("  sha256: %s\n", report.SHA256)
	fmt.Printf("  rows: %s\n", formatRowCounts(report.RowCounts))
	return 0
}

func safeDBError(err error) string {
	switch {
	case errors.Is(err, store.ErrDBInUse):
		return "database is in use; stop occa before restoring"
	case strings.Contains(err.Error(), "refuse to overwrite"):
		return "refuse to overwrite an existing backup; use --force"
	case strings.Contains(err.Error(), "older"):
		return "restore input uses an older schema; use --force if intentional"
	case strings.Contains(err.Error(), "newer than this binary supports"):
		return "restore input schema is newer than this binary supports"
	case strings.Contains(err.Error(), "not an occa database"), strings.Contains(err.Error(), "integrity check"):
		return "restore input is invalid or incompatible"
	default:
		return "operation failed; verify the database, input, and permissions"
	}
}

func printBackupReport(report *store.BackupReport) {
	fmt.Println("backup complete")
	fmt.Printf("  sha256: %s\n", report.SHA256)
	fmt.Printf("  rows: %s\n", formatRowCounts(report.RowCounts))
}

func formatRowCounts(counts []store.RowCount) string {
	parts := make([]string, 0, len(counts))
	for _, rc := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", rc.Table, rc.Rows))
	}
	return strings.Join(parts, " ")
}

func dbUsage() {
	fmt.Fprintln(os.Stderr, `usage: occa db <command> [flags]

Operator database backup and restore. The database path is resolved from the
occa config file (--config, default ~/.occa/config.yaml) or OCCA_DB_PATH.

commands:
  backup   write a SQLite-consistent backup of the live database
  restore  validate a backup and atomically replace the stopped database

flags:
  -c, --config <path>   path to occa config file
  -f, --force           overwrite an existing backup / allow older-schema restore
  -o, --output <path>   backup output path (backup)
  -i, --input <path>    backup file to restore (restore)
  -h, --help            show this help`)
}

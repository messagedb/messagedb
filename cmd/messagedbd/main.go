package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/messagedb/messagedb/cmd/messagedbd/backup"
	"github.com/messagedb/messagedb/cmd/messagedbd/help"
	"github.com/messagedb/messagedb/cmd/messagedbd/restore"
	"github.com/messagedb/messagedb/cmd/messagedbd/run"
)

// These variables are populated via the Go linker.
var (
	version string = "0.1"
	commit  string
)

func init() {
	// If commit not set, make that clear.
	if commit == "" {
		commit = "unknown"
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the program execution.
type Main struct {
	Logger *log.Logger

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain return a new instance of Main.
func NewMain() *Main {
	return &Main{
		Logger: log.New(os.Stderr, "[run] ", log.LstdFlags),
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "run":
		cmd := run.NewCommand()

		// Tell the server the build details.
		cmd.Version = version
		cmd.Commit = commit

		if err := cmd.Run(args...); err != nil {
			return fmt.Errorf("run: %s", err)
		}

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
		m.Logger.Println("listening for signals")

		// Block until one of the signals above is received
		select {
		case <-signalCh:
			m.Logger.Println("signal received, initializing clean shutdown...")
			go func() {
				cmd.Close()
			}()
		}

		// Block again until another signal is received, a shutdown timeout elapses,
		// or the Command is gracefully closed
		m.Logger.Println("waiting for clean shutdown...")
		select {
		case <-signalCh:
			m.Logger.Println("second signal received, initializing hard shutdown")
		case <-time.After(time.Second * 30):
			m.Logger.Println("time limit reached, initializing hard shutdown")
		case <-cmd.Closed:
			m.Logger.Println("server shutdown completed")
		}

		// goodbye

	case "backup":
		name := backup.NewCommand()
		if err := name.Run(args...); err != nil {
			return fmt.Errorf("backup: %s", err)
		}
	case "restore":
		name := restore.NewCommand()
		if err := name.Run(args...); err != nil {
			return fmt.Errorf("restore: %s", err)
		}
	case "config":
		if err := run.NewPrintConfigCommand().Run(args...); err != nil {
			return fmt.Errorf("config: %s", err)
		}
	case "version":
		if err := NewVersionCommand().Run(args...); err != nil {
			return fmt.Errorf("version: %s", err)
		}
	case "help":
		if err := help.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("help: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run 'messaged help' for usage`+"\n\n", name)
	}

	return nil
}

// ParseCommandName extracts the command name and args from the args list.
func ParseCommandName(args []string) (string, []string) {
	// Retrieve command name as first argument.
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
	}

	// Special case -h immediately following binary name
	if len(args) > 0 && args[0] == "-h" {
		name = "help"
	}

	// If command is "help" and has an argument then rewrite args to use "-h".
	if name == "help" && len(args) > 1 {
		args[0], args[1] = args[1], "-h"
		name = args[0]
	}

	// If a named command is specified then return it with its arguments.
	if name != "" {
		return name, args[1:]
	}
	return "", args
}

// Command represents the command executed by "messaged version".
type VersionCommand struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewVersionCommand return a new instance of VersionCommand.
func NewVersionCommand() *VersionCommand {
	return &VersionCommand{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run prints the current version and commit info.
func (cmd *VersionCommand) Run(args ...string) error {
	// Parse flags in case -h is specified.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(versionUsage)) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Print version info.
	fmt.Fprintf(cmd.Stdout, "MessageDB v%s (git: %s)\n", version, commit)

	return nil
}

var versionUsage = `
usage: version

	version displays the MessageDB version and build git commit hash
`

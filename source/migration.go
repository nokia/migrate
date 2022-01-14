package source

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"text/tabwriter"
)

// Direction is either up or down.
type Direction string

const (
	Down Direction = "down"
	Up   Direction = "up"
)

type Status string

const (
	Skipped Status = "skipped"
	Pending Status = "pending"
	Done    Status = "done"
	Failed  Status = "failed"
)

type MigrationFunc func(ctx context.Context, db interface{}) error

var MgrFunctions = make(map[string]MigrationFunc) // map of filename and functions                           // current release

// Migration is a helper struct for source drivers that need to
// build the full directory tree in memory.
// Migration is fully independent from migrate.Migration.
type Migration struct {
	// Version is the version of this migration.
	Version uint

	// Identifier can be any string that helps identifying
	// this migration in the source.
	Identifier string

	// Direction is either Up or Down.
	Direction Direction

	// Raw holds the raw location path to this migration in source.
	// ReadUp and ReadDown will use this.
	Raw string

	// status of the migration
	Status Status

	Error string
}

// Migrations wraps Migration and has an internal index
// to keep track of Migration order.
type Migrations struct {
	index      uintSlice
	migrations map[uint]map[Direction]*Migration
}

func NewMigrations() *Migrations {
	return &Migrations{
		index:      make(uintSlice, 0),
		migrations: make(map[uint]map[Direction]*Migration),
	}
}

func (i *Migrations) Append(m *Migration) (ok bool) {
	if m == nil {
		return false
	}

	if i.migrations[m.Version] == nil {
		i.migrations[m.Version] = make(map[Direction]*Migration)
	}

	// reject duplicate versions
	if _, dup := i.migrations[m.Version][m.Direction]; dup {
		return false
	}

	i.migrations[m.Version][m.Direction] = m
	i.buildIndex()

	return true
}

func (i *Migrations) buildIndex() {
	i.index = make(uintSlice, 0, len(i.migrations))
	for version := range i.migrations {
		i.index = append(i.index, version)
	}
	sort.Slice(i.index, func(x, y int) bool {
		return i.index[x] < i.index[y]
	})
}

func (i *Migrations) First() (version uint, ok bool) {
	if len(i.index) == 0 {
		return 0, false
	}
	return i.index[0], true
}

func (i *Migrations) Prev(version uint) (prevVersion uint, ok bool) {
	pos := i.findPos(version)
	if pos >= 1 && len(i.index) > pos-1 {
		return i.index[pos-1], true
	}
	return 0, false
}

func (i *Migrations) Next(version uint) (nextVersion uint, ok bool) {
	pos := i.findPos(version)
	if pos >= 0 && len(i.index) > pos+1 {
		return i.index[pos+1], true
	}
	return 0, false
}

func (i *Migrations) Up(version uint) (m *Migration, ok bool) {
	if _, ok := i.migrations[version]; ok {
		if mx, ok := i.migrations[version][Up]; ok {
			return mx, true
		}
	}
	return nil, false
}

func (i *Migrations) Down(version uint) (m *Migration, ok bool) {
	if _, ok := i.migrations[version]; ok {
		if mx, ok := i.migrations[version][Down]; ok {
			return mx, true
		}
	}
	return nil, false
}

func (i *Migrations) findPos(version uint) int {
	if len(i.index) > 0 {
		ix := i.index.Search(version)
		if ix < len(i.index) && i.index[ix] == version {
			return ix
		}
	}
	return -1
}

func (i *Migrations) MarkSkipMigrations(version uint, dir Direction) {
	for idx := range i.index {
		if dir == Up && i.index[idx] <= version {
			// mark all older version as skipped.
			i.migrations[i.index[idx]][dir].Status = Skipped
		} else if dir == Down && i.index[idx] >= version {
			// mark all newer version as skipped.
			i.migrations[i.index[idx]][dir].Status = Skipped
		}
	}
}

func (i *Migrations) UpdateStatus(version uint, status Status, errstr string) {
	if _, ok := i.migrations[version]; ok {
		if mx, ok := i.migrations[version][Up]; ok {
			mx.Status = status
			mx.Error = errstr
		}
		if mx, ok := i.migrations[version][Down]; ok {
			mx.Status = status
			mx.Error = errstr
		}
	}
}

func (i *Migrations) PrintSummary(dir Direction) {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "\n\t\t%s\n\n", "+++++ Migration Summary +++++")
	fmt.Fprintf(w, "\t%s\t%s\t%s\t\n", "Migration Source", "Status", "Error")
	fmt.Fprintf(w, "\t%s\t%s\t%s\t\n", "----------------", "------", "-----")
	for idx := range i.index {
		fmt.Fprintf(w, "\t%s\t%s\t%s\t\n",
			i.migrations[i.index[idx]][dir].Raw,
			i.migrations[i.index[idx]][dir].Status,
			i.migrations[i.index[idx]][dir].Error)
	}

	fmt.Fprintf(w, "\t%s\t%s\t%s\t\n", "----------------", "------", "-----")
	if err := w.Flush(); err != nil {
		fmt.Printf("error in closing formatter: %v\n", err)
	}
}

type uintSlice []uint

func (s uintSlice) Search(x uint) int {
	return sort.Search(len(s), func(i int) bool { return s[i] >= x })
}

// register go migration function
func RegisterFuncMigration(fn MigrationFunc) {
	_, file, _, _ := runtime.Caller(1)
	name := filepath.Base(file)
	MgrFunctions[name] = fn
}

package googlecloudstorage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/golang-migrate/migrate/v4/source"
	"google.golang.org/api/iterator"
)

func init() {
	source.Register("gcs", &gcs{})
}

type gcs struct {
	bucket     *storage.BucketHandle
	prefix     string
	migrations *source.Migrations
}

func (g *gcs) Open(folder string) (source.Driver, error) {
	u, err := url.Parse(folder)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	driver := gcs{
		bucket:     client.Bucket(u.Host),
		prefix:     strings.Trim(u.Path, "/") + "/",
		migrations: source.NewMigrations(),
	}
	err = driver.loadMigrations()
	if err != nil {
		return nil, err
	}
	return &driver, nil
}

func (g *gcs) loadMigrations() error {
	iter := g.bucket.Objects(context.Background(), &storage.Query{
		Prefix:    g.prefix,
		Delimiter: "/",
	})
	object, err := iter.Next()
	for ; err == nil; object, err = iter.Next() {
		_, fileName := path.Split(object.Name)
		m, parseErr := source.DefaultParse(fileName)
		if parseErr != nil {
			continue
		}
		if !g.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", object.Name)
		}
	}
	if err != iterator.Done {
		return err
	}
	return nil
}

func (g *gcs) Close() error {
	return nil
}

func (g *gcs) First() (uint, error) {
	v, ok := g.migrations.First()
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (g *gcs) Prev(version uint) (uint, error) {
	v, ok := g.migrations.Prev(version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (g *gcs) Next(version uint) (uint, error) {
	v, ok := g.migrations.Next(version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (g *gcs) ReadUp(version uint) (io.ReadCloser, string, string, source.MigrationFunc, error) {
	if m, ok := g.migrations.Up(version); ok {
		return g.open(m)
	}
	return nil, "", "", nil, os.ErrNotExist
}

func (g *gcs) ReadDown(version uint) (io.ReadCloser, string, string, source.MigrationFunc, error) {
	if m, ok := g.migrations.Down(version); ok {
		return g.open(m)
	}
	return nil, "", "", nil, os.ErrNotExist
}

func (g *gcs) open(m *source.Migration) (io.ReadCloser, string, string, source.MigrationFunc, error) {
	objectPath := path.Join(g.prefix, m.Raw)
	reader, err := g.bucket.Object(objectPath).NewReader(context.Background())
	if err != nil {
		return nil, "", "", nil, err
	}
	return reader, m.Identifier, m.Raw, nil, nil
}

func (g *gcs) MarkSkipMigrations(version uint, dir source.Direction) {
	g.migrations.MarkSkipMigrations(version, dir)
}

func (g *gcs) UpdateStatus(version uint, status source.Status, errstr string) {
	g.migrations.UpdateStatus(version, status, errstr)
}

func (g *gcs) PrintSummary(dir source.Direction) {
	g.migrations.PrintSummary(dir)
}

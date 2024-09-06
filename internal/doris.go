package internal

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

type DorisStorage struct {
	logger *zap.Logger

	db               *sql.DB
	cfg              *Config
	reader           spanstore.Reader
	writer           spanstore.Writer
	dependencyReader dependencystore.Reader
}

func NewDorisStorage(ctx context.Context, cfg *Config) (*DorisStorage, error) {
	logger := LoggerFromContext(ctx)

	dsn := cfg.Doris.DSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	reader := &dorisReader{
		logger: logger.With(zap.String("doris", "reader")),
		db:     db,
		cfg:    cfg,
	}

	writer := &dorisWriterNoop{
		logger: logger.With(zap.String("doris", "writer")),
	}

	dependencyReader := &dorisDependencyReader{
		logger: logger.With(zap.String("doris", "dependency-reader")),
		dr:     reader,
	}

	return &DorisStorage{
		logger:           logger,
		db:               db,
		cfg:              cfg,
		reader:           reader,
		writer:           writer,
		dependencyReader: dependencyReader,
	}, nil
}

var (
	_ shared.StoragePlugin = (*DorisStorage)(nil)
)

func (ds *DorisStorage) SpanReader() spanstore.Reader {
	return ds.reader
}

func (ds *DorisStorage) SpanWriter() spanstore.Writer {
	return ds.writer
}

func (ds *DorisStorage) DependencyReader() dependencystore.Reader {
	return ds.dependencyReader
}

func (ds *DorisStorage) Close() error {
	return nil
}

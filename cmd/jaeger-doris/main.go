package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/joker-star-l/jaeger-doris/internal"
	"github.com/mattn/go-isatty"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx := context.Background()
	ctx = contextWithStandardSignals(ctx)

	cfg := &internal.Config{}

	logger, err := initLogger(cfg)
	if err != nil {
		panic(err)
	}
	ctx = internal.LoggerWithContext(ctx, logger)

	err = run(ctx, cfg)
	if err != nil {
		panic(err)
	}
}

func initLogger(_ *internal.Config) (*zap.Logger, error) {
	var loggerConfig zap.Config
	if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		loggerConfig = zap.NewDevelopmentConfig()
	} else {
		loggerConfig = zap.NewProductionConfig()
	}
	var err error
	loggerConfig.Level, err = zap.ParseAtomicLevel("debug") // TODO: configurable
	if err != nil {
		return nil, err
	}
	return loggerConfig.Build(zap.AddStacktrace(zap.ErrorLevel))
}

func contextWithStandardSignals(ctx context.Context) context.Context {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			return
		}
	}()
	return ctx
}

func run(ctx context.Context, cfg *internal.Config) error {
	backend, err := internal.NewDorisStorage(ctx, cfg)
	if err != nil {
		return err
	}
	defer backend.Close()

	grpcHandler := shared.NewGRPCHandlerWithPlugins(backend, nil, nil)
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	healthServer := health.NewServer()
	err = grpcHandler.Register(grpcServer, healthServer)
	if err != nil {
		return err
	}

	grpcListener, err := net.Listen("tcp", "localhost:5000") // TODO: configurable
	if err != nil {
		return err
	}
	defer grpcListener.Close()

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		errCh <- grpcServer.Serve(grpcListener)
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()

	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
		select {
		case err = <-errCh:
		case <-time.After(3 * time.Second):
			err = errors.New("the gRPC server never stopped")
		}
	}

	return err
}

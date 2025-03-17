package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"

	"github.com/simonasgal/jaeger-doris/internal"
	"github.com/simonasgal/jaeger-doris/thrid_party/jaeger/plugin/storage/grpc/shared"
)

var configPath string

const serviceName = "jaeger-doris"

func main() {
	cfg := &internal.Config{}
	command := &cobra.Command{
		Use:   serviceName,
		Args:  cobra.NoArgs,
		Short: serviceName + " is the Jaeger-doris gRPC remote storage service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := cfg.Init(configPath)
			if err != nil {
				return fmt.Errorf("failed to get config: %w", err)
			}

			err = cfg.Validate()
			if err != nil {
				return fmt.Errorf("failed to validate config: %w", err)
			}

			logger, err := initLogger(cfg)
			if err != nil {
				return fmt.Errorf("failed to start logger: %w", err)
			}

			ctx := internal.LoggerWithContext(cmd.Context(), logger)
			return run(ctx, cfg)
		},
	}
	command.Flags().StringVarP(&configPath, "config", "c", "", "configuration file")

	ctx := contextWithStandardSignals(context.Background())
	if err := command.ExecuteContext(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			os.Exit(1)
		}
	}
}

func initLogger(cfg *internal.Config) (*zap.Logger, error) {
	var loggerConfig zap.Config
	if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		loggerConfig = zap.NewDevelopmentConfig()
	} else {
		loggerConfig = zap.NewProductionConfig()
	}
	var err error
	loggerConfig.Level, err = zap.ParseAtomicLevel(cfg.Service.LogLevel)
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

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (ss *contextServerStream) Context() context.Context {
	return ss.ctx
}

func run(ctx context.Context, cfg *internal.Config) error {
	backend, err := internal.NewDorisStorage(ctx, cfg)
	if err != nil {
		return err
	}
	defer backend.Close()

	logger := internal.LoggerFromContext(ctx)
	grpcHandlerOpts := &shared.GRPCHandlerOptions{SpanBatchSize: int(cfg.Service.GRPCSpanBatchSize)}

	grpcHandler := shared.NewGRPCHandlerWithPlugins(backend, nil, nil, grpcHandlerOpts)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			ctx = internal.LoggerWithContext(ctx, logger)
			res, err := handler(ctx, req)
			if err != nil && err != context.Canceled {
				logger.Error("gRPC interceptor", zap.Error(err))
			}
			return res, err
		}),
		grpc.StreamInterceptor(func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := internal.LoggerWithContext(stream.Context(), logger)
			stream = &contextServerStream{
				ServerStream: stream,
				ctx:          ctx,
			}
			err := handler(srv, stream)
			if err != nil && err != context.Canceled {
				logger.Error("gRPC interceptor", zap.Error(err))
			}
			return err
		}),
	)

	reflection.Register(grpcServer)
	healthServer := health.NewServer()
	err = grpcHandler.Register(grpcServer, healthServer)
	if err != nil {
		return err
	}

	grpcListener, err := net.Listen("tcp", cfg.Service.Address())
	if err != nil {
		return err
	}
	defer func() { _ = grpcListener.Close() }()

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		errCh <- grpcServer.Serve(grpcListener)
	}()

	logger.Info("start")
	<-ctx.Done()
	logger.Info("exiting")

	grpcServer.GracefulStop()

	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		logger.Warn("the gRPC server is being stubborn, so forcing it to stop")
		grpcServer.Stop()
		select {
		case err = <-errCh:
		case <-time.After(3 * time.Second):
			err = errors.New("the gRPC server never stopped")
		}
	}

	err = multierr.Combine(err, backend.Close())
	return err
}

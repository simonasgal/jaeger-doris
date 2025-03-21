// Copyright (c) 2019 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"io"

	"github.com/jaegertracing/jaeger/model"
	_ "github.com/jaegertracing/jaeger/pkg/gogocodec" // force gogo codec registration
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// changed: default span batch size, may be overridden  by configuration
const defaultSpanBatchSize = 1000

// GRPCHandler implements all methods of Remote Storage gRPC API.
type GRPCHandler struct {
	// impl        StoragePlugin
	// ArchiveImpl ArchiveStoragePlugin
	// StreamImpl  StreamingSpanWriterPlugin
	impl *GRPCHandlerStorageImpl
	opts *GRPCHandlerOptions
}

// GRPCHandlerOptions contains grpc handler options
type GRPCHandlerOptions struct {
	SpanBatchSize int
}

// GRPCHandlerStorageImpl contains accessors for various storage implementations needed by the handler.
type GRPCHandlerStorageImpl struct {
	SpanReader       func() spanstore.Reader
	SpanWriter       func() spanstore.Writer
	DependencyReader func() dependencystore.Reader

	ArchiveSpanReader func() spanstore.Reader
	ArchiveSpanWriter func() spanstore.Writer

	StreamingSpanWriter func() spanstore.Writer
}

// NewGRPCHandler creates a handler given individual storage implementations.
func NewGRPCHandler(impl *GRPCHandlerStorageImpl, opts *GRPCHandlerOptions) *GRPCHandler {
	return &GRPCHandler{impl: impl, opts: opts}
}

// NewGRPCHandler creates a handler given implementations grouped by plugin services.
func NewGRPCHandlerWithPlugins(mainImpl StoragePlugin, archiveImpl ArchiveStoragePlugin, streamImpl StreamingSpanWriterPlugin, opts *GRPCHandlerOptions) *GRPCHandler {
	impl := &GRPCHandlerStorageImpl{
		SpanReader:       mainImpl.SpanReader,
		SpanWriter:       mainImpl.SpanWriter,
		DependencyReader: mainImpl.DependencyReader,

		ArchiveSpanReader:   func() spanstore.Reader { return nil },
		ArchiveSpanWriter:   func() spanstore.Writer { return nil },
		StreamingSpanWriter: func() spanstore.Writer { return nil },
	}
	if archiveImpl != nil {
		impl.ArchiveSpanReader = archiveImpl.ArchiveSpanReader
		impl.ArchiveSpanWriter = archiveImpl.ArchiveSpanWriter
	}
	if streamImpl != nil {
		impl.StreamingSpanWriter = streamImpl.StreamingSpanWriter
	}
	return NewGRPCHandler(impl, opts)
}

// Register registers the server as gRPC methods handler.
func (s *GRPCHandler) Register(ss *grpc.Server, hs *health.Server) error {
	storage_v1.RegisterSpanReaderPluginServer(ss, s)
	storage_v1.RegisterSpanWriterPluginServer(ss, s)
	storage_v1.RegisterArchiveSpanReaderPluginServer(ss, s)
	storage_v1.RegisterArchiveSpanWriterPluginServer(ss, s)
	storage_v1.RegisterPluginCapabilitiesServer(ss, s)
	storage_v1.RegisterDependenciesReaderPluginServer(ss, s)
	storage_v1.RegisterStreamingSpanWriterPluginServer(ss, s)

	hs.SetServingStatus("jaeger.storage.v1.SpanReaderPlugin", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v1.SpanWriterPlugin", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v1.ArchiveSpanReaderPlugin", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v1.ArchiveSpanWriterPlugin", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v1.PluginCapabilities", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v1.DependenciesReaderPlugin", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v1.StreamingSpanWriterPlugin", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(ss, hs)

	return nil
}

// GetDependencies returns all interservice dependencies
func (s *GRPCHandler) GetDependencies(ctx context.Context, r *storage_v1.GetDependenciesRequest) (*storage_v1.GetDependenciesResponse, error) {
	deps, err := s.impl.DependencyReader().GetDependencies(ctx, r.EndTime, r.EndTime.Sub(r.StartTime))
	if err != nil {
		return nil, err
	}
	return &storage_v1.GetDependenciesResponse{
		Dependencies: deps,
	}, nil
}

// WriteSpanStream receive the span from stream and save it
func (s *GRPCHandler) WriteSpanStream(stream storage_v1.StreamingSpanWriterPlugin_WriteSpanStreamServer) error {
	writer := s.impl.StreamingSpanWriter()
	if writer == nil {
		return status.Error(codes.Unimplemented, "not implemented")
	}
	for {
		in, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		err = writer.WriteSpan(stream.Context(), in.Span)
		if err != nil {
			return err
		}
	}
	return stream.SendAndClose(&storage_v1.WriteSpanResponse{})
}

// WriteSpan saves the span
func (s *GRPCHandler) WriteSpan(ctx context.Context, r *storage_v1.WriteSpanRequest) (*storage_v1.WriteSpanResponse, error) {
	err := s.impl.SpanWriter().WriteSpan(ctx, r.Span)
	if err != nil {
		return nil, err
	}
	return &storage_v1.WriteSpanResponse{}, nil
}

func (s *GRPCHandler) Close(context.Context, *storage_v1.CloseWriterRequest) (*storage_v1.CloseWriterResponse, error) {
	if closer, ok := s.impl.SpanWriter().(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return nil, err
		}

		return &storage_v1.CloseWriterResponse{}, nil
	}
	return nil, status.Error(codes.Unimplemented, "span writer does not support graceful shutdown")
}

// GetTrace takes a traceID and streams a Trace associated with that traceID
func (s *GRPCHandler) GetTrace(r *storage_v1.GetTraceRequest, stream storage_v1.SpanReaderPlugin_GetTraceServer) error {
	trace, err := s.impl.SpanReader().GetTrace(stream.Context(), r.TraceID)
	if errors.Is(err, spanstore.ErrTraceNotFound) {
		return status.Error(codes.NotFound, spanstore.ErrTraceNotFound.Error())
	}
	if err != nil {
		return err
	}

	err = s.sendSpans(trace.Spans, stream.Send)
	if err != nil {
		return err
	}

	return nil
}

// GetServices returns a list of all known services
func (s *GRPCHandler) GetServices(ctx context.Context, _ *storage_v1.GetServicesRequest) (*storage_v1.GetServicesResponse, error) {
	services, err := s.impl.SpanReader().GetServices(ctx)
	if err != nil {
		return nil, err
	}
	return &storage_v1.GetServicesResponse{
		Services: services,
	}, nil
}

// GetOperations returns the operations of a given service
func (s *GRPCHandler) GetOperations(
	ctx context.Context,
	r *storage_v1.GetOperationsRequest,
) (*storage_v1.GetOperationsResponse, error) {
	operations, err := s.impl.SpanReader().GetOperations(ctx, spanstore.OperationQueryParameters{
		ServiceName: r.Service,
		SpanKind:    r.SpanKind,
	})
	if err != nil {
		return nil, err
	}
	grpcOperation := make([]*storage_v1.Operation, len(operations))
	for i, operation := range operations {
		grpcOperation[i] = &storage_v1.Operation{
			Name:     operation.Name,
			SpanKind: operation.SpanKind,
		}
	}
	return &storage_v1.GetOperationsResponse{
		Operations: grpcOperation,
	}, nil
}

// FindTraces streams traces that match the traceQuery
func (s *GRPCHandler) FindTraces(r *storage_v1.FindTracesRequest, stream storage_v1.SpanReaderPlugin_FindTracesServer) error {
	traces, err := s.impl.SpanReader().FindTraces(stream.Context(), &spanstore.TraceQueryParameters{
		ServiceName:   r.Query.ServiceName,
		OperationName: r.Query.OperationName,
		Tags:          r.Query.Tags,
		StartTimeMin:  r.Query.StartTimeMin,
		StartTimeMax:  r.Query.StartTimeMax,
		DurationMin:   r.Query.DurationMin,
		DurationMax:   r.Query.DurationMax,
		NumTraces:     int(r.Query.NumTraces),
	})
	if err != nil {
		return err
	}

	for _, trace := range traces {
		err = s.sendSpans(trace.Spans, stream.Send)
		if err != nil {
			return err
		}
	}

	return nil
}

// FindTraceIDs retrieves traceIDs that match the traceQuery
func (s *GRPCHandler) FindTraceIDs(ctx context.Context, r *storage_v1.FindTraceIDsRequest) (*storage_v1.FindTraceIDsResponse, error) {
	traceIDs, err := s.impl.SpanReader().FindTraceIDs(ctx, &spanstore.TraceQueryParameters{
		ServiceName:   r.Query.ServiceName,
		OperationName: r.Query.OperationName,
		Tags:          r.Query.Tags,
		StartTimeMin:  r.Query.StartTimeMin,
		StartTimeMax:  r.Query.StartTimeMax,
		DurationMin:   r.Query.DurationMin,
		DurationMax:   r.Query.DurationMax,
		NumTraces:     int(r.Query.NumTraces),
	})
	if err != nil {
		return nil, err
	}
	return &storage_v1.FindTraceIDsResponse{
		TraceIDs: traceIDs,
	}, nil
}

func (s *GRPCHandler) sendSpans(spans []*model.Span, sendFn func(*storage_v1.SpansResponseChunk) error) error {
	chunk := make([]model.Span, 0, len(spans))

	// Changed: use batch size from the configuration, if available
	spanBatchSize := defaultSpanBatchSize
	if s.opts != nil {
		spanBatchSize = s.opts.SpanBatchSize
	}
	for i := 0; i < len(spans); i += spanBatchSize {
		chunk = chunk[:0]
		for j := i; j < len(spans) && j < i+spanBatchSize; j++ {
			chunk = append(chunk, *spans[j])
		}
		pld := storage_v1.SpansResponseChunk{Spans: chunk}
		if err := sendFn(&pld); err != nil {

			fmt.Printf("===== chunl len: %v\n", len(chunk))
			b, _ := json.Marshal(pld)
			fmt.Printf("===== len pld: %v\n", len(b))
			//for _, item := range chunk {
			// fmt.Printf("===== item: %+v\n", item)
			//b, _ := json.Marshal(item)
			//fmt.Printf("===== chunk len: %v\n", len(b))
			//}
			// fmt.Printf("==== chunk: %#v\n", chunk)

			return err
			return fmt.Errorf("grpc plugin failed to send response: %w", err)
		}
	}

	return nil
}

func (s *GRPCHandler) Capabilities(context.Context, *storage_v1.CapabilitiesRequest) (*storage_v1.CapabilitiesResponse, error) {
	return &storage_v1.CapabilitiesResponse{
		ArchiveSpanReader:   s.impl.ArchiveSpanReader() != nil,
		ArchiveSpanWriter:   s.impl.ArchiveSpanWriter() != nil,
		StreamingSpanWriter: s.impl.StreamingSpanWriter() != nil,
	}, nil
}

func (s *GRPCHandler) GetArchiveTrace(r *storage_v1.GetTraceRequest, stream storage_v1.ArchiveSpanReaderPlugin_GetArchiveTraceServer) error {
	reader := s.impl.ArchiveSpanReader()
	if reader == nil {
		return status.Error(codes.Unimplemented, "not implemented")
	}
	trace, err := reader.GetTrace(stream.Context(), r.TraceID)
	if errors.Is(err, spanstore.ErrTraceNotFound) {
		return status.Error(codes.NotFound, spanstore.ErrTraceNotFound.Error())
	}
	if err != nil {
		return err
	}

	err = s.sendSpans(trace.Spans, stream.Send)
	if err != nil {
		return err
	}

	return nil
}

func (s *GRPCHandler) WriteArchiveSpan(ctx context.Context, r *storage_v1.WriteSpanRequest) (*storage_v1.WriteSpanResponse, error) {
	writer := s.impl.ArchiveSpanWriter()
	if writer == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	err := writer.WriteSpan(ctx, r.Span)
	if err != nil {
		return nil, err
	}
	return &storage_v1.WriteSpanResponse{}, nil
}

/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * OpenTelemetry tracer setup. Three exporters are supported, any combination
 * may be enabled via flags:
 *
 *   --trace-stdout       pretty-printed stdout exporter (development)
 *   --trace-otlp-http    OTLP/HTTP to --trace-endpoint (default localhost:4318)
 *   --trace-otlp-grpc    OTLP/gRPC to --trace-endpoint (default localhost:4317)
 *
 * The standard OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS / _INSECURE env vars are
 * honoured by the OTLP exporters when --trace-endpoint is left blank.
 */

package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type tracingOpts struct {
	stdout      bool
	otlpHTTP    bool
	otlpGRPC    bool
	endpoint    string // host:port; empty → exporter default + OTEL_* env honoured
	insecure    bool   // applies to OTLP exporters
	serviceName string
	version     string
}

// setupTracing installs an OpenTelemetry tracer provider with one or more
// span exporters. Returns a shutdown function that flushes all batchers.
// If no exporter is enabled, returns a no-op shutdown.
func setupTracing(ctx context.Context, opts tracingOpts) (func(context.Context) error, error) {
	if !opts.stdout && !opts.otlpHTTP && !opts.otlpGRPC {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(opts.serviceName),
			semconv.ServiceVersion(opts.version),
		),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}

	if opts.stdout {
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdout exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}

	if opts.otlpHTTP {
		httpOpts := []otlptracehttp.Option{}
		if opts.endpoint != "" {
			httpOpts = append(httpOpts, otlptracehttp.WithEndpoint(opts.endpoint))
		}
		if opts.insecure {
			httpOpts = append(httpOpts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptrace.New(ctx, otlptracehttp.NewClient(httpOpts...))
		if err != nil {
			return nil, fmt.Errorf("otlp/http exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}

	if opts.otlpGRPC {
		grpcOpts := []otlptracegrpc.Option{}
		if opts.endpoint != "" {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithEndpoint(opts.endpoint))
		}
		if opts.insecure {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(grpcOpts...))
		if err != nil {
			return nil, fmt.Errorf("otlp/grpc exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

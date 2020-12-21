package utils

import (
	"context"
	"os"
	"strings"

	"github.com/go-courier/metax"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/codes"
	exportertrace "go.opentelemetry.io/otel/sdk/export/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type OutputType string

var (
	OutputAlways    OutputType = "Always"
	OutputOnFailure OutputType = "OnFailure"
	OutputNever     OutputType = "Never"
)

type FormatType string

var (
	FormatTEXT FormatType = "text"
	FormatJSON FormatType = "json"
)

func NewTracer(outputType OutputType, formatter logrus.Formatter) trace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithConfig(sdktrace.Config{
			DefaultSampler: sdktrace.AlwaysSample(),
		}),
		sdktrace.WithSyncer(&StdoutExporter{outputType, formatter}),
	)
}

type StdoutExporter struct {
	outputType OutputType
	formatter  logrus.Formatter
}

func (e *StdoutExporter) Shutdown(ctx context.Context) error {
	return nil
}

// ExportSpan writes a SpanData in json format to stdout.
func (e *StdoutExporter) ExportSpans(ctx context.Context, spanData []*exportertrace.SpanData) error {
	if e.outputType == OutputNever {
		return nil
	}

	for i := range spanData {
		data := spanData[i]

		if e.outputType == OutputOnFailure {
			if data.StatusCode == codes.Ok {
				continue
			}
		}

		for _, event := range data.MessageEvents {
			if event.Name[0] != '@' {
				continue
			}

			lv, err := logrus.ParseLevel(event.Name[1:])
			if err != nil {
				continue
			}

			entry := logrus.NewEntry(logrus.StandardLogger())
			entry.Level = lv
			entry.Time = event.Time
			entry.Data = logrus.Fields{}

			for _, kv := range event.Attributes {
				k := string(kv.Key)
				if k == "msg" {
					entry.Message = kv.Value.AsString()
					continue
				}
				entry.Data[k] = kv.Value.AsInterface()
			}

			for _, kv := range data.Attributes {
				k := string(kv.Key)
				if k == "meta" {
					assignMeta(entry, metax.ParseMeta(kv.Value.AsString()))
					continue
				}
				entry.Data[k] = kv.Value.AsInterface()
			}

			entry.Data["span"] = data.Name
			entry.Data["traceID"] = data.SpanContext.TraceID

			if data.SpanContext.HasSpanID() {
				entry.Data["spanID"] = data.SpanContext.SpanID
			}

			if data.ParentSpanID.IsValid() {
				entry.Data["parentSpanID"] = data.ParentSpanID
			}

			log(e.formatter, entry)
		}
	}

	return nil
}

func log(formatter logrus.Formatter, e *logrus.Entry) {
	data, err := formatter.Format(e)
	if err == nil {
		_, _ = os.Stdout.Write(data)
	}
}

func resolveFormatter(formatType FormatType) logrus.Formatter {
	if strings.ToLower(string(formatType)) == string(FormatJSON) {
		return &logrus.JSONFormatter{CallerPrettyfier: callerPrettyfier}
	}
	return &logrus.TextFormatter{ForceColors: true, CallerPrettyfier: callerPrettyfier}
}

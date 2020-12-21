package utils

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/go-courier/metax"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/label"
)

type Log struct {
	ReportCaller string
	Level        string     `env:""`
	Output       OutputType `env:""`
	Format       FormatType
	init         bool
}

func (log *Log) SetDefaults() {
	if log.ReportCaller == "" {
		log.ReportCaller = "enabled"
	}

	if log.Level == "" {
		log.Level = "DEBUG"
	}

	if log.Output == "" {
		log.Output = OutputAlways
	}

	if log.Format == "" {
		log.Format = FormatJSON
	}
}

func (log *Log) Init() {
	if !log.init {
		formatter := resolveFormatter(log.Format)

		global.SetTracerProvider(NewTracer(log.Output, formatter))

		logrus.SetLevel(getLogLevel(log.Level))

		if log.ReportCaller == "enabled" {
			logrus.SetReportCaller(true)
		}

		t := NewSpanSamplerForLogger(resolveProjectName(), formatter)

		logrus.SetFormatter(t)
		logrus.SetOutput(os.Stdout)

		log.init = true
	}
}

func getLogLevel(l string) logrus.Level {
	level, err := logrus.ParseLevel(strings.ToLower(l))
	if err == nil {
		return level
	}
	return logrus.InfoLevel
}

func NewSpanSamplerForLogger(projectName string, formatter logrus.Formatter) *SpanSamplerForLogger {
	return &SpanSamplerForLogger{
		projectName: projectName,
		formatter:   formatter,
	}
}

type SpanSamplerForLogger struct {
	projectName string
	formatter   logrus.Formatter
}

func (t *SpanSamplerForLogger) Format(entry *logrus.Entry) ([]byte, error) {
	ctx := entry.Context
	if ctx == nil {
		ctx = context.Background()
	}

	meta := metax.MetaFromContext(ctx)
	s := trace.SpanFromContext(ctx)
	remoteSpanContext := trace.RemoteSpanContextFromContext(ctx)
	if remoteSpanContext.IsValid() {
		entry.Data["traceID"] = remoteSpanContext.TraceID
		entry.Data["remoteParentSpanID"] = remoteSpanContext.SpanID
	}

	if s.SpanContext().IsValid() {
		s.SetAttributes(
			label.String("meta", meta.String()),
			label.String("project", t.projectName),
		)

		data := make([]label.KeyValue, 0)

		for k := range entry.Data {
			data = append(data, label.Any(k, entry.Data[k]))
		}

		if entry.Caller != nil {
			fn, _ := callerPrettyfier(entry.Caller)
			data = append(data, label.String("func", fn))
		}

		data = append(data, label.String("msg", entry.Message))

		s.AddEventWithTimestamp(ctx, entry.Time, "@"+entry.Level.String(), data...)

		if entry.Level <= logrus.ErrorLevel {
			s.SetStatus(codes.Error, entry.Message)
		}

		return nil, nil
	}

	assignMeta(entry, meta)

	return t.formatter.Format(entry)
}

func callerPrettyfier(f *runtime.Frame) (function string, file string) {
	return f.Function + " line:" + strconv.FormatInt(int64(f.Line), 10), ""
}

func assignMeta(entry *logrus.Entry, meta metax.Meta) {
	for k := range meta {
		if k == "_id" {
			continue
		}
		entry.Data[k] = meta[k]
	}
}

func resolveProjectName() string {
	projectName := os.Getenv("PROJECT_NAME")

	version := os.Getenv("PROJECT_VERSION")
	if version != "" {
		projectName += "@" + version
	}

	return projectName
}

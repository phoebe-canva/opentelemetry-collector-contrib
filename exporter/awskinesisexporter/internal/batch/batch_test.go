// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batch_test

import (
	"testing"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awskinesisexporter/internal/batch"
)

func TestBatchingMessages(t *testing.T) {
	t.Parallel()

	b := batch.New()
	for i := 0; i < 948; i++ {
		assert.NoError(t, b.AddRecord([]byte("foobar"), "fixed-string"), "Must not error when adding elements into the batch")
	}

	chunk := b.Chunk()
	for _, records := range chunk {
		for _, record := range records {
			assert.Equal(t, []byte("foobar"), record.Data, "Must have the expected record value")
			assert.Equal(t, "fixed-string", *record.PartitionKey, "Must have the expected partition key")
		}
	}

	assert.Len(t, chunk, 2, "Must have split the batch into two chunks")
	assert.Len(t, chunk, 2, "Must not modify the stored data within the batch")

	assert.Error(t, b.AddRecord(nil, "fixed-string"), "Must error when invalid record provided")
	assert.Error(t, b.AddRecord([]byte("some data that is very important"), ""), "Must error when invalid partition key provided")
}

func TestCustomBatchSizeConstraints(t *testing.T) {
	t.Parallel()

	b := batch.New(
		batch.WithMaxRecordsPerBatch(1),
	)
	const records = 203
	for i := 0; i < records; i++ {
		assert.NoError(t, b.AddRecord([]byte("foobar"), "fixed-string"), "Must not error when adding elements into the batch")
	}
	assert.Len(t, b.Chunk(), records, "Must have one batch per record added")
}

func BenchmarkChunkingRecords(b *testing.B) {
	bt := batch.New()
	for i := 0; i < 948; i++ {
		assert.NoError(b, bt.AddRecord([]byte("foobar"), "fixed-string"), "Must not error when adding elements into the batch")
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		assert.Len(b, bt.Chunk(), 2, "Must have exactly two chunks")
	}
}

func generateOtlpLog() plog.Logs {
	ln := plog.NewLogs()
	rl := ln.ResourceLogs()
	for i := 0; i < 100; i++ {
		rl.AppendEmpty()
		newResourceLog := rl.At(i)
		newResourceLog.SetSchemaUrl("test")
		logRecord := newResourceLog.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		logRecord.Attributes().PutStr("cloud_account_id", faker.FirstName())
		logRecord.Attributes().PutStr("service_name", faker.FirstName())
		logRecord.Body().SetStr(faker.Sentence())
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}
	return ln
}

func BenchmarkEncodingOtlpJson(b *testing.B) {
	bm, err := batch.NewEncoder("otlp_json")
	if err != nil {
		b.Errorf("Errored out %v", err)
	}
	//body := []byte(`[{"resourceLogs":[{"resource":{"attributes":[{"key":"resource-attr","value":{"stringValue":"resource-attr-val-1"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"1581452773000000789","severityNumber":9,"severityText":"Info","name":"logA","body":{"stringValue":"This is a log message"},"attributes":[{"key":"app","value":{"stringValue":"server"}},{"key":"instance_num","value":{"intValue":"1"}}],"droppedAttributesCount":1,"traceId":"08040201000000000000000000000000","spanId":"0102040800000000"},{"timeUnixNano":"1581452773000000789","severityNumber":9,"severityText":"Info","name":"logB","body":{"stringValue":"something happened"},"attributes":[{"key":"customer","value":{"stringValue":"acme"}},{"key":"env","value":{"stringValue":"dev"}}],"droppedAttributesCount":1,"traceId":"","spanId":""}]}]}]}]`)
	ln := generateOtlpLog()

	b.Run("test json encoding", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			bm.Logs(ln)
		}
	})

}

func BenchmarkEncodingOtlp(b *testing.B) {

	bm, err := batch.NewEncoder("otlp")
	if err != nil {
		b.Errorf("Errored out %v", err)
	}
	//body := []byte(`[{"resourceLogs":[{"resource":{"attributes":[{"key":"resource-attr","value":{"stringValue":"resource-attr-val-1"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"1581452773000000789","severityNumber":9,"severityText":"Info","name":"logA","body":{"stringValue":"This is a log message"},"attributes":[{"key":"app","value":{"stringValue":"server"}},{"key":"instance_num","value":{"intValue":"1"}}],"droppedAttributesCount":1,"traceId":"08040201000000000000000000000000","spanId":"0102040800000000"},{"timeUnixNano":"1581452773000000789","severityNumber":9,"severityText":"Info","name":"logB","body":{"stringValue":"something happened"},"attributes":[{"key":"customer","value":{"stringValue":"acme"}},{"key":"env","value":{"stringValue":"dev"}}],"droppedAttributesCount":1,"traceId":"","spanId":""}]}]}]}]`)
	ln := generateOtlpLog()

	b.Run("test otlp encoding", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			bm.Logs(ln)
		}
	})

}

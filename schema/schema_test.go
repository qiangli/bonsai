/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/qiangli/bonsai/protos/pb"
)

func TestStateGetOwnsSchema(t *testing.T) {
	s := &state{}
	s.init()
	defer s.elog.Finish()
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 7)
	stored := &pb.SchemaUpdate{
		Predicate: "name", ValueType: pb.Posting_STRING, Count: true,
		Tokenizer:  []string{"exact"},
		IndexSpecs: []*pb.VectorIndexSpec{{Name: "hnsw"}},
	}
	stored.ProtoReflect().SetUnknown(unknown)
	s.Set("name", stored)

	got, ok := s.Get(context.Background(), "name")
	require.True(t, ok)
	require.True(t, proto.Equal(stored, got))
	got.Count = false
	got.Tokenizer[0] = "term"
	got.IndexSpecs[0].Name = "changed"
	got.ProtoReflect().SetUnknown(nil)

	again, ok := s.Get(context.Background(), "name")
	require.True(t, ok)
	require.True(t, proto.Equal(stored, again))
	require.Equal(t, unknown, []byte(again.ProtoReflect().GetUnknown()))
	require.True(t, s.predicate["name"].Count)
	require.Equal(t, "exact", s.predicate["name"].Tokenizer[0])
	require.Equal(t, "hnsw", s.predicate["name"].IndexSpecs[0].Name)
	missing, ok := s.Get(context.Background(), "missing")
	require.False(t, ok)
	require.Nil(t, missing)
}

func TestStateGetWriteContextOwnsSchema(t *testing.T) {
	s := &state{}
	s.init()
	defer s.elog.Finish()
	s.Set("name", &pb.SchemaUpdate{Predicate: "name", Tokenizer: []string{"exact"}})
	updated := &pb.SchemaUpdate{Predicate: "name", Count: true, Tokenizer: []string{"term"},
		IndexSpecs: []*pb.VectorIndexSpec{{Name: "hnsw"}}}
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 9)
	updated.ProtoReflect().SetUnknown(unknown)
	s.SetMutSchema("name", updated)
	ctx := GetWriteContext(context.Background())
	got, ok := s.Get(ctx, "name")
	require.True(t, ok)
	require.True(t, proto.Equal(updated, got))
	got.Count = false
	got.Tokenizer[0] = "exact"
	got.IndexSpecs[0].Name = "changed"
	got.ProtoReflect().SetUnknown(nil)

	again, ok := s.Get(ctx, "name")
	require.True(t, ok)
	require.True(t, proto.Equal(updated, again))
	require.Equal(t, unknown, []byte(again.ProtoReflect().GetUnknown()))
	require.Equal(t, "exact", s.predicate["name"].Tokenizer[0])
	require.True(t, s.mutSchema["name"].Count)
	require.Equal(t, "term", s.mutSchema["name"].Tokenizer[0])
	require.Equal(t, "hnsw", s.mutSchema["name"].IndexSpecs[0].Name)
	missing, ok := s.Get(ctx, "missing")
	require.False(t, ok)
	require.Nil(t, missing)
}

func TestStateGetTypeOwnsType(t *testing.T) {
	s := &state{}
	s.init()
	defer s.elog.Finish()
	stored := &pb.TypeUpdate{TypeName: "Person", Fields: []*pb.SchemaUpdate{{
		Predicate: "name", Tokenizer: []string{"exact"},
		IndexSpecs: []*pb.VectorIndexSpec{{Name: "hnsw"}},
	}}}
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 11)
	stored.ProtoReflect().SetUnknown(unknown)
	s.SetType("Person", stored)
	got, ok := s.GetType("Person")
	require.True(t, ok)
	require.True(t, proto.Equal(stored, got))
	got.TypeName = "Other"
	got.Fields[0].Predicate = "age"
	got.Fields[0].Tokenizer[0] = "term"
	got.Fields[0].IndexSpecs[0].Name = "changed"
	got.ProtoReflect().SetUnknown(nil)

	again, ok := s.GetType("Person")
	require.True(t, ok)
	require.True(t, proto.Equal(stored, again))
	require.Equal(t, unknown, []byte(again.ProtoReflect().GetUnknown()))
	require.Equal(t, "Person", s.types["Person"].TypeName)
	require.Equal(t, "name", s.types["Person"].Fields[0].Predicate)
	require.Equal(t, "exact", s.types["Person"].Fields[0].Tokenizer[0])
	require.Equal(t, "hnsw", s.types["Person"].Fields[0].IndexSpecs[0].Name)
	missing, ok := s.GetType("missing")
	require.False(t, ok)
	require.Nil(t, missing)
}

func TestCompareSchemaUpdates(t *testing.T) {
	tests := []struct {
		name     string
		original *pb.SchemaUpdate
		update   *pb.SchemaUpdate
		expected []string
	}{
		{
			name: "No changes",
			original: &pb.SchemaUpdate{
				Predicate: "name",
				ValueType: pb.Posting_STRING,
				Count:     true,
			},
			update: &pb.SchemaUpdate{
				Predicate: "name",
				ValueType: pb.Posting_STRING,
				Count:     true,
			},
			expected: []string(nil),
		},
		{
			name: "Predicate changed",
			original: &pb.SchemaUpdate{
				Predicate: "name",
				ValueType: pb.Posting_STRING,
				Count:     true,
			},
			update: &pb.SchemaUpdate{
				Predicate: "age",
				ValueType: pb.Posting_STRING,
				Count:     true,
			},
			expected: []string{"Predicate"},
		},
		{
			name: "Multiple fields changed",
			original: &pb.SchemaUpdate{
				Predicate: "name",
				ValueType: pb.Posting_STRING,
				Count:     true,
			},
			update: &pb.SchemaUpdate{
				Predicate: "age",
				ValueType: pb.Posting_STRING,
				Count:     false,
			},
			expected: []string{"Predicate", "Count"},
		},
		{
			name: "Unchanged and changed fields",
			original: &pb.SchemaUpdate{
				Predicate: "name",
				Count:     true,
				Upsert:    false,
			},
			update: &pb.SchemaUpdate{
				Predicate: "name",
				Count:     true,
				Upsert:    true,
			},
			expected: []string{"Upsert"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, compareSchemaUpdates(tt.original, tt.update))
		})
	}
}

// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestRunStoreParallelTasksLimitedBoundsConcurrency(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	tasks := make([]storeParallelTask, 4)
	for i := range tasks {
		tasks[i] = storeParallelTask{name: "bounded", run: func(context.Context) error {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return nil
		}}
	}
	if err := runStoreParallelTasksLimited(context.Background(), 2, tasks...); err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrency = %d, want 2", got)
	}
}

func TestNullableUUID_Zero(t *testing.T) {
	if nullableUUID(uuid.UUID{}) != nil {
		t.Error("expected nil for zero UUID")
	}
}

func TestNullableUUID_NonZero(t *testing.T) {
	id := uuid.New()
	result := nullableUUID(id)
	if result == nil {
		t.Fatal("expected non-nil for non-zero UUID")
	}
	if *result != id {
		t.Errorf("expected %s, got %s", id, *result)
	}
}

func TestTristate_Nil(t *testing.T) {
	if tristate(nil) != "any" {
		t.Error("expected \"any\" for nil")
	}
}

func TestTristate_True(t *testing.T) {
	b := true
	if tristate(&b) != "true" {
		t.Error("expected \"true\" for true")
	}
}

func TestTristate_False(t *testing.T) {
	b := false
	if tristate(&b) != "false" {
		t.Error("expected \"false\" for false")
	}
}

func TestToChannelMessage(t *testing.T) {
	senderName := "Alice"
	content := "hello"
	channelHash := []byte{0xab}
	sentAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	msg := toChannelMessage(42, "deadbeef", channelHash, &senderName, &content, sentAt, 7)

	if msg.ID != 42 {
		t.Errorf("expected ID 42, got %d", msg.ID)
	}
	if msg.PacketHash != "deadbeef" {
		t.Errorf("expected PacketHash deadbeef, got %s", msg.PacketHash)
	}
	if msg.ChannelHash != "ab" {
		t.Errorf("expected ChannelHash ab, got %s", msg.ChannelHash)
	}
	if msg.SenderName != "Alice" {
		t.Errorf("expected SenderName Alice, got %s", msg.SenderName)
	}
	if msg.Content != "hello" {
		t.Errorf("expected Content hello, got %s", msg.Content)
	}
	if msg.SentAt != 1700000000000 {
		t.Errorf("expected SentAt 1700000000000, got %d", msg.SentAt)
	}
	if msg.ObservationCount != 7 {
		t.Errorf("expected ObservationCount 7, got %d", msg.ObservationCount)
	}
}

func TestToChannelMessage_NilFields(t *testing.T) {
	sentAt := pgtype.Timestamptz{Time: time.UnixMilli(0), Valid: true}
	msg := toChannelMessage(1, "abc", []byte{0x01}, nil, nil, sentAt, 0)
	if msg.SenderName != "" {
		t.Errorf("expected empty SenderName, got %s", msg.SenderName)
	}
	if msg.Content != "" {
		t.Errorf("expected empty Content, got %s", msg.Content)
	}
}

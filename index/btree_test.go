package index

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mck753/kvx/data"
)

func TestBTree_Put(t *testing.T) {
	type args struct {
		key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil test",
			args: args{
				key: nil,
				pos: nil,
			},
			want: true,
		},
		{
			name: "normal test",
			args: args{
				key: []byte("1"),
				pos: &data.LogRecordPos{FID: 1, Offset: 1},
			},
			want: true,
		},
	}

	bt := NewBTree()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bt.Put(tt.args.key, tt.args.pos)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBTree_Get(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		args args
		want *data.LogRecordPos
	}{
		{
			name: "nil test",
			args: args{
				key: nil,
			},
			want: nil,
		},
		{
			name: "normal test",
			args: args{
				key: []byte("1"),
			},
			want: &data.LogRecordPos{FID: 1, Offset: 1},
		},
	}

	bt := NewBTree()
	bt.Put([]byte("1"), &data.LogRecordPos{FID: 1, Offset: 1})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, bt.Get(tt.args.key), "Get(%v)", tt.args.key)
		})
	}
}

func TestBTree_Delete(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil test",
			args: args{
				key: nil,
			},
			want: false,
		},
		{
			name: "normal test",
			args: args{
				key: []byte("1"),
			},
			want: true,
		},
	}

	bt := NewBTree()
	bt.Put([]byte("1"), &data.LogRecordPos{FID: 1, Offset: 1})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, bt.Delete(tt.args.key), "Delete(%v)", tt.args.key)
		})
	}
}

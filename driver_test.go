package bingo

import (
	"testing"
)

func TestIsErrDocumentExists(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"TestIsErrDocumentExists", args{ErrDocumentExists}, true},
		{"TestIsErrDocumentExists", args{ErrDocumentNotFound}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsErrDocumentExists(tt.args.err); got != tt.want {
				t.Errorf("IsErrDocumentExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

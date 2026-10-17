package correlation

import (
	"context"
	"testing"
)

func TestWithContextRoundTrip(t *testing.T) {
	t.Parallel()

	want := Context{
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		CommandID:     "cmd-1",
		UserID:        "user-1",
	}
	ctx := WithContext(context.Background(), want)

	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext: ok=false, want true")
	}
	if got != want {
		t.Fatalf("FromContext = %+v, want %+v", got, want)
	}
}

func TestFromContextAbsentReturnsFalse(t *testing.T) {
	t.Parallel()

	_, ok := FromContext(context.Background())
	if ok {
		t.Fatal("FromContext(background): ok=true, want false")
	}
}

func TestCommandIDFromContext(t *testing.T) {
	t.Parallel()

	t.Run("present", func(t *testing.T) {
		t.Parallel()
		ctx := WithContext(context.Background(), Context{CommandID: "cmd-42"})
		if got := CommandIDFromContext(ctx); got != "cmd-42" {
			t.Fatalf("CommandIDFromContext = %q, want %q", got, "cmd-42")
		}
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		if got := CommandIDFromContext(context.Background()); got != "" {
			t.Fatalf("CommandIDFromContext(background) = %q, want %q", got, "")
		}
	})
}

package commands

import (
	"context"
	"errors"
	"testing"
)

func TestHandlerFunc(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("handler failed")
	called := false
	hf := HandlerFunc[int, string](func(ctx context.Context, c int) (string, error) {
		called = true
		if c == 42 {
			return "answer", nil
		}
		return "", expectedErr
	})

	got, err := hf.Handle(context.Background(), 42)
	if !called {
		t.Fatal("HandlerFunc was not called")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "answer" {
		t.Fatalf("result = %q, want %q", got, "answer")
	}

	_, err = hf.Handle(context.Background(), 0)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v, want %v", err, expectedErr)
	}
}

func TestChain(t *testing.T) {
	t.Parallel()

	base := HandlerFunc[string, int](func(_ context.Context, _ string) (int, error) {
		return 0, nil
	})

	var order []string
	mw1 := Middleware[string, int](func(next Handler[string, int]) Handler[string, int] {
		return HandlerFunc[string, int](func(ctx context.Context, c string) (int, error) {
			order = append(order, "mw1-before")
			r, err := next.Handle(ctx, c)
			order = append(order, "mw1-after")
			return r, err
		})
	})
	mw2 := Middleware[string, int](func(next Handler[string, int]) Handler[string, int] {
		return HandlerFunc[string, int](func(ctx context.Context, c string) (int, error) {
			order = append(order, "mw2-before")
			r, err := next.Handle(ctx, c)
			order = append(order, "mw2-after")
			return r, err
		})
	})

	chained := Chain(base, mw1, mw2)
	if _, err := chained.Handle(context.Background(), "test"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Middlewares wrap right-to-left, so mw1 is outermost.
	want := []string{"mw1-before", "mw2-before", "mw2-after", "mw1-after"}
	if len(order) != len(want) {
		t.Fatalf("order len = %d, want %d: %v", len(order), len(want), order)
	}
	for i, w := range want {
		if order[i] != w {
			t.Fatalf("order[%d] = %q, want %q (full: %v)", i, order[i], w, order)
		}
	}
}

func TestChainNoMiddlewares(t *testing.T) {
	t.Parallel()
	base := HandlerFunc[int, int](func(_ context.Context, c int) (int, error) {
		return c * 2, nil
	})
	chained := Chain(base)
	got, err := chained.Handle(context.Background(), 5)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if got != 10 {
		t.Fatalf("result = %d, want 10", got)
	}
}

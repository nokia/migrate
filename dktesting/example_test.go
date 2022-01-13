package dktesting_test

import (
	"context"
	"testing"

	"github.com/dhui/dktest"
	"github.com/nokia/migrate/v4/dktesting"
)

func ExampleParallelTest() {
	t := &testing.T{} // Should actually be used in a Test

	isReady := func(ctx context.Context, c dktest.ContainerInfo) bool {
		// Return true if the container is ready to run tests.
		// Don't block here though. Use the Context to timeout container ready checks.
		return true
	}

	dktesting.ParallelTest(t, []dktesting.ContainerSpec{{
		ImageName: "docker_image:9.6",
		Options:   dktest.Options{ReadyFunc: isReady},
	}}, func(t *testing.T, c dktest.ContainerInfo) {
		// Run your test/s ...
		t.Fatal("...")
	})
}

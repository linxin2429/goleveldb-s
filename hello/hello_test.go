package hello

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHello(t *testing.T) {
	result := Greet()
	assert.Equal(t, "hello", result)
}

package memstore

import (
	"testing"

	"github.com/bapp-open/django-celery-beat-go/internal/leakcheck"
)

func TestMain(m *testing.M) { leakcheck.Main(m) }

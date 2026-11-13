package leakcheck

import "testing"

func TestMain(m *testing.M) { Main(m) }

func TestNoLeakReportedForCleanBinary(t *testing.T) {
	done := make(chan struct{})
	go func() { close(done) }()
	<-done
}

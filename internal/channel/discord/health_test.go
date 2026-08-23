package discord

import "testing"

func TestAdapterConnectedInitiallyFalse(t *testing.T) {
	a := New("token", nil)
	if ok, detail := a.Connected(); ok {
		t.Fatalf("Connected() = true,false before Start (detail %q)", detail)
	}
}

func TestAdapterConnectedAfterGatewayOpen(t *testing.T) {
	a := New("token", nil)
	a.connected.Store(true)
	if ok, _ := a.Connected(); !ok {
		t.Fatal("Connected() = false after gateway opened")
	}
}

func TestAdapterStopMarksDisconnected(t *testing.T) {
	a := New("token", nil)
	a.connected.Store(true)
	if err := a.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if ok, _ := a.Connected(); ok {
		t.Fatal("Connected() = true after Stop")
	}
}

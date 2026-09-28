package lsg

import "testing"

// Config (intent) and Stats (runtime) must stay distinct: mutating a
// Config snapshot returned to the caller must not change the gateway, and
// runtime counters must not leak back into Config.
func TestConfigStatsSeparation(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) { _ = c.Send(p) })

	snap := g.Config()
	snap.MaxPayload = 12345
	snap.IdleLease = 1
	if g.Config().MaxPayload == 12345 || g.Config().IdleLease == 1 {
		t.Fatal("mutating a returned Config snapshot affected live config")
	}

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	waitConnCount(g, 1)
	if err := cl.Send([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(); err != nil {
		t.Fatal(err)
	}

	st := g.Stats()
	if st.ActiveConns != 1 {
		t.Fatalf("active=%d want 1", st.ActiveConns)
	}
	if st.FramesIn != 1 || st.FramesOut != 1 {
		t.Fatalf("frames in=%d out=%d want 1/1", st.FramesIn, st.FramesOut)
	}
	if st.CurrentConfig.MaxPayload == 12345 {
		t.Fatal("runtime Stats snapshot picked up a local mutation")
	}
	if st.StartedAt.IsZero() || st.State != "running" {
		t.Fatalf("bad stats state: %+v", st)
	}
}

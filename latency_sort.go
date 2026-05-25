package latency_sort

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"
	"sync"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

var clog = log.NewWithPlugin("latency_sort")

type LatencySort struct {
	Next plugin.Handler
}

func (ls *LatencySort) Name() string { return "latency_sort" }

func (ls *LatencySort) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	if !isLocalhost(w.RemoteAddr()) {
		return ls.Next.ServeDNS(ctx, w, r)
	}

	clog.Debugf("local request for %s", r.Question[0].Name)
	rw := &responseWriter{ResponseWriter: w}
	return ls.Next.ServeDNS(ctx, rw, r)
}

type responseWriter struct {
	dns.ResponseWriter
}

func (rw *responseWriter) WriteMsg(res *dns.Msg) error {
	if res != nil && res.Rcode == dns.RcodeSuccess {
		sortAnswers(res)
	}
	return rw.ResponseWriter.WriteMsg(res)
}

func isLocalhost(addr net.Addr) bool {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

type addrRecord struct {
	addr net.IP
	rr   dns.RR
}

func sortAnswers(m *dns.Msg) {
	var addrRRs []addrRecord
	var others []dns.RR

	for _, rr := range m.Answer {
		switch h := rr.Header().Rrtype; h {
		case dns.TypeA:
			addrRRs = append(addrRRs, addrRecord{rr.(*dns.A).A, rr})
		case dns.TypeAAAA:
			addrRRs = append(addrRRs, addrRecord{rr.(*dns.AAAA).AAAA, rr})
		default:
			others = append(others, rr)
		}
	}

	if len(addrRRs) < 2 {
		return
	}

	addrs := make([]string, len(addrRRs))
	for i, ar := range addrRRs {
		addrs[i] = ar.addr.String()
	}
	clog.Debugf("pinging addresses: %v", addrs)

	fastest := findFastest(addrRRs)
	if fastest == -1 {
		clog.Debugf("no ping responses received within timeout, keeping original order")
		return
	}

	clog.Debugf("fastest address: %s (index %d)", addrRRs[fastest].addr, fastest)
	
	m.Answer = append(others, addrRRs[fastest].rr)
}

const (
	pingTimeout  = 300 * time.Millisecond
	pingData     = "latency-sort"
	icmpv4Proto  = 1
	icmpv6Proto  = 58
)

type pingResult struct {
	idx int
}

func findFastest(records []addrRecord) int {
	ch := make(chan pingResult, len(records)+1)
	var wg sync.WaitGroup

	for i, ar := range records {
		wg.Add(1)
		go func(idx int, ip net.IP) {
			defer wg.Done()
			_, err := ping(ip)
			if err == nil {
				ch <- pingResult{idx}
			}
		}(i, ar.addr)
	}

	go func() {
		wg.Wait()
		ch <- pingResult{-1} // all pings failed
	}()

	select {
	case res := <-ch:
		return res.idx
	case <-time.After(pingTimeout):
		return -1
	}
}

func ping(ip net.IP) (time.Duration, error) {
	var network string
	var proto int
	var typ icmp.Type
	var raddr net.Addr

	if ip.To4() != nil {
		network = "udp4"
		proto = icmpv4Proto
		typ = ipv4.ICMPTypeEcho
		raddr = &net.UDPAddr{IP: ip}
	} else {
		network = "udp6"
		proto = icmpv6Proto
		typ = ipv6.ICMPTypeEchoRequest
		raddr = &net.UDPAddr{IP: ip}
	}

	conn, err := icmp.ListenPacket(network, "")
	if err != nil {
		clog.Debugf("ping %s: listen failed: %v", ip, err)
		return 0, fmt.Errorf("listen: %w", err)
	}
	defer conn.Close()

	msg := icmp.Message{
		Type: typ,
		Code: 0,
		Body: &icmp.Echo{
			ID:   os.Getpid() & 0xffff,
			Seq:  1,
			Data: []byte(pingData),
		},
	}

	msgBytes, err := msg.Marshal(nil)
	if err != nil {
		return 0, fmt.Errorf("marshal: %w", err)
	}

	start := time.Now()
	if _, err := conn.WriteTo(msgBytes, raddr); err != nil {
		clog.Debugf("ping %s: write failed: %v", ip, err)
		return 0, fmt.Errorf("write: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(pingTimeout)); err != nil {
		return 0, fmt.Errorf("set deadline: %w", err)
	}

	reply := make([]byte, 1500)
	n, _, err := conn.ReadFrom(reply)
	if err != nil {
		clog.Debugf("ping %s: read failed: %v", ip, err)
		return 0, fmt.Errorf("read: %w", err)
	}

	parsed, err := icmp.ParseMessage(proto, reply[:n])
	if err != nil {
		return 0, fmt.Errorf("parse: %w", err)
	}

	switch parsed.Type {
	case ipv4.ICMPTypeEchoReply, ipv6.ICMPTypeEchoReply:
		rtt := time.Since(start)
		clog.Debugf("ping %s: reply received in %v", ip, rtt)
		return rtt, nil
	default:
		clog.Debugf("ping %s: unexpected ICMP type %v", ip, parsed.Type)
		return 0, fmt.Errorf("unexpected ICMP type: %v", parsed.Type)
	}
}

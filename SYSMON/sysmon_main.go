package main

// ─────────────────────────────────────────────────────────────────────────────
//  sysmon — background system monitor with a browser UI
//
//  Run:   go run main.go
//  Then open: http://localhost:7070
//
//  Architecture:
//    1. collector goroutine  — reads /proc every 2s, pushes to a channel
//    2. broadcaster goroutine — fans the latest stats out to every WebSocket client
//    3. HTTP server goroutine — serves the HTML page + WebSocket upgrades
//    4. per-client goroutine  — one per browser tab, reads from a personal channel
// ─────────────────────────────────────────────────────────────────────────────

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ─── Data types ──────────────────────────────────────────────────────────────

// Stats is the snapshot we send to the browser every tick.
type Stats struct {
	Timestamp string         `json:"timestamp"`
	CPU       float64        `json:"cpu"` // percent 0-100
	CPUCores  int            `json:"cpuCores"`
	CPUModel  string         `json:"cpuModel"`
	RAM       MemInfo        `json:"ram"`
	Swap      MemInfo        `json:"swap"`
	VRAM      VRAMInfo       `json:"vram"`
	Uptime    string         `json:"uptime"`
	LoadAvg   string         `json:"loadAvg"`
	Procs     []ProcessGroup `json:"procs"` // top groups by memory
	ProcCount int            `json:"procCount"`
}

type MemInfo struct {
	UsedGB  float64 `json:"usedGB"`
	TotalGB float64 `json:"totalGB"`
	Pct     float64 `json:"pct"`
}

type VRAMInfo struct {
	Name    string  `json:"name"`
	UsedMB  int64   `json:"usedMB"`
	TotalMB int64   `json:"totalMB"`
	Pct     float64 `json:"pct"`
	GpuPct  float64 `json:"gpuPct"`
	TempC   string  `json:"tempC"`
}

// Process is a single raw process entry (used internally).
type Process struct {
	PID  int
	Name string
	CPU  float64
	MemP float64
	MB   int64
	User string
}

// ProcessGroup is what we send to the browser — all PIDs with the same
// base name are merged into one row, with individual PIDs stored for expand.
type ProcessGroup struct {
	Name     string   `json:"name"`
	PIDs     []int    `json:"pids"`     // all PIDs in this group
	CPU      float64  `json:"cpu"`      // sum across all instances
	MemP     float64  `json:"memP"`     // sum across all instances
	MB       int64    `json:"mb"`       // sum across all instances
	Count    int      `json:"count"`    // number of instances
	Children []PIDRow `json:"children"` // per-PID detail for the expand panel
}

// PIDRow holds the detail for a single PID inside a group.
type PIDRow struct {
	PID  int     `json:"pid"`
	CPU  float64 `json:"cpu"`
	MemP float64 `json:"memP"`
	MB   int64   `json:"mb"`
}

// ─── Collector ───────────────────────────────────────────────────────────────
// Runs in its own goroutine. Reads /proc files, builds a Stats struct,
// and sends it to the out channel. Uses two CPU samples separated by 500ms
// to compute a real delta-based CPU percentage.

func collector(out chan<- Stats) {
	// We need two consecutive CPU readings to compute usage
	var prevIdle, prevTotal uint64

	for {
		// — CPU — read /proc/stat twice with a gap
		idle1, total1 := readCPUStat()
		if prevTotal == 0 {
			// First boot: seed and wait
			prevIdle, prevTotal = idle1, total1
			time.Sleep(500 * time.Millisecond)
			idle1, total1 = readCPUStat()
		}

		deltaIdle := idle1 - prevIdle
		deltaTotal := total1 - prevTotal
		cpuPct := 0.0
		if deltaTotal > 0 {
			cpuPct = (1.0 - float64(deltaIdle)/float64(deltaTotal)) * 100.0
			cpuPct = math.Round(cpuPct*10) / 10
		}
		prevIdle, prevTotal = idle1, total1

		// — Memory — read /proc/meminfo
		ram, swap := readMemInfo()

		// — VRAM — try nvidia-smi, fallback to "N/A"
		vram := readVRAM()

		// — Uptime + Load Average —
		uptime := readUptime()
		loadAvg := readLoadAvg()

		// — Process list — read /proc/[pid]/stat + status
		procs, procCount := readProcesses()

		s := Stats{
			Timestamp: time.Now().Format("15:04:05"),
			CPU:       cpuPct,
			CPUCores:  runtime.NumCPU(),
			CPUModel:  readCPUModel(),
			RAM:       ram,
			Swap:      swap,
			VRAM:      vram,
			Uptime:    uptime,
			LoadAvg:   loadAvg,
			Procs:     procs,
			ProcCount: procCount,
		}

		// Non-blocking send: drop if broadcaster is busy (never block the collector)
		select {
		case out <- s:
		default:
		}

		time.Sleep(2 * time.Second)
	}
}

// readCPUStat reads the first 'cpu' line from /proc/stat.
// Returns idle jiffies and total jiffies.
func readCPUStat() (idle, total uint64) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		// fields: cpu user nice system idle iowait irq softirq steal ...
		var vals []uint64
		for _, f := range fields[1:] {
			v, _ := strconv.ParseUint(f, 10, 64)
			vals = append(vals, v)
			total += v
		}
		if len(vals) >= 4 {
			idle = vals[3] // 4th column = idle
		}
		return
	}
	return
}

// readMemInfo parses /proc/meminfo for RAM and Swap.
func readMemInfo() (ram, swap MemInfo) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return
	}
	defer f.Close()

	vals := map[string]uint64{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) >= 2 {
			key := strings.TrimSuffix(parts[0], ":")
			v, _ := strconv.ParseUint(parts[1], 10, 64)
			vals[key] = v
		}
	}

	toGB := func(kb uint64) float64 {
		return math.Round(float64(kb)/1048576.0*10) / 10
	}
	pct := func(used, total uint64) float64 {
		if total == 0 {
			return 0
		}
		return math.Round(float64(used)/float64(total)*1000) / 10
	}

	ramUsed := vals["MemTotal"] - vals["MemAvailable"]
	ram = MemInfo{
		UsedGB:  toGB(ramUsed),
		TotalGB: toGB(vals["MemTotal"]),
		Pct:     pct(ramUsed, vals["MemTotal"]),
	}

	swapUsed := vals["SwapTotal"] - vals["SwapFree"]
	swap = MemInfo{
		UsedGB:  toGB(swapUsed),
		TotalGB: toGB(vals["SwapTotal"]),
		Pct:     pct(swapUsed, vals["SwapTotal"]),
	}
	return
}

// readVRAM tries nvidia-smi. Falls back gracefully.
func readVRAM() VRAMInfo {
	out, err := exec.Command("nvidia-smi",
		"--query-gpu=name,memory.used,memory.total,utilization.gpu,temperature.gpu",
		"--format=csv,noheader,nounits").Output()
	if err != nil {
		return VRAMInfo{Name: "No discrete GPU", TotalMB: 0}
	}

	parts := strings.SplitN(strings.TrimSpace(string(out)), ",", 5)
	if len(parts) < 5 {
		return VRAMInfo{Name: "GPU data unavailable"}
	}

	parse := func(s string) int64 {
		v, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		return v
	}
	parseF := func(s string) float64 {
		v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
		return v
	}

	used := parse(parts[1])
	total := parse(parts[2])
	gpuPct := parseF(parts[3])

	vramPct := 0.0
	if total > 0 {
		vramPct = math.Round(float64(used)/float64(total)*1000) / 10
	}

	return VRAMInfo{
		Name:    strings.TrimSpace(parts[0]),
		UsedMB:  used,
		TotalMB: total,
		Pct:     vramPct,
		GpuPct:  gpuPct,
		TempC:   strings.TrimSpace(parts[4]) + "°C",
	}
}

// readUptime reads /proc/uptime and formats it nicely.
func readUptime() string {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return "unknown"
	}
	secs, _ := strconv.ParseFloat(strings.Fields(string(data))[0], 64)
	d := int(secs) / 86400
	h := (int(secs) % 86400) / 3600
	m := (int(secs) % 3600) / 60
	if d > 0 {
		return fmt.Sprintf("%dd %dh %dm", d, h, m)
	}
	return fmt.Sprintf("%dh %dm", h, m)
}

// readLoadAvg reads /proc/loadavg.
func readLoadAvg() string {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "—"
	}
	parts := strings.Fields(string(data))
	if len(parts) >= 3 {
		return parts[0] + "  " + parts[1] + "  " + parts[2]
	}
	return string(data)
}

// readCPUModel reads the CPU model name from /proc/cpuinfo.
func readCPUModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "model name") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return "unknown"
}

// readProcesses lists all PIDs in /proc, reads each one's stat+status file,
// and returns the top 15 by memory usage.
//
// This is the most interesting goroutine-friendly part: in production you'd
// spawn a worker pool here. For simplicity we read sequentially — it's fast
// enough since /proc is a virtual FS in RAM.
func readProcesses() ([]ProcessGroup, int) {
	entries, err := filepath.Glob("/proc/[0-9]*/status")
	if err != nil {
		return nil, 0
	}

	// Read total RAM for % calculation
	ramData, _ := os.ReadFile("/proc/meminfo")
	var totalRAMkB uint64
	for _, line := range strings.Split(string(ramData), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fmt.Sscanf(strings.TrimPrefix(line, "MemTotal:"), "%d", &totalRAMkB)
			break
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	rawProcs := make([]Process, 0, len(entries))

	// Worker goroutine pool — 16 concurrent /proc readers
	sem := make(chan struct{}, 16)

	for _, path := range entries {
		wg.Add(1)
		sem <- struct{}{}

		go func(statusPath string) {
			defer wg.Done()
			defer func() { <-sem }()

			p := parseProcessStatus(statusPath, totalRAMkB)
			if p == nil {
				return
			}
			mu.Lock()
			rawProcs = append(rawProcs, *p)
			mu.Unlock()
		}(path)
	}

	wg.Wait()

	totalProcs := len(rawProcs)

	// ── Group by name ─────────────────────────────────────────────────────
	// Map: name → accumulated group data
	type accum struct {
		cpu  float64
		memP float64
		mb   int64
		rows []PIDRow
		pids []int
	}
	groups := map[string]*accum{}

	for _, p := range rawProcs {
		g, ok := groups[p.Name]
		if !ok {
			g = &accum{}
			groups[p.Name] = g
		}
		g.cpu += p.CPU
		g.memP += p.MemP
		g.mb += p.MB
		g.pids = append(g.pids, p.PID)
		g.rows = append(g.rows, PIDRow{
			PID:  p.PID,
			CPU:  p.CPU,
			MemP: p.MemP,
			MB:   p.MB,
		})
	}

	// Convert map → slice and round aggregated values
	result := make([]ProcessGroup, 0, len(groups))
	for name, g := range groups {
		// Sort children by MB descending so heaviest PID is first when expanded
		sort.Slice(g.rows, func(i, j int) bool { return g.rows[i].MB > g.rows[j].MB })
		sort.Ints(g.pids)

		result = append(result, ProcessGroup{
			Name:     name,
			PIDs:     g.pids,
			CPU:      math.Round(g.cpu*10) / 10,
			MemP:     math.Round(g.memP*10) / 10,
			MB:       g.mb,
			Count:    len(g.pids),
			Children: g.rows,
		})
	}

	// Sort groups by MB descending
	sort.Slice(result, func(i, j int) bool {
		return result[i].MB > result[j].MB
	})

	// Keep top 20 groups
	if len(result) > 20 {
		result = result[:20]
	}
	return result, totalProcs
}

// parseProcessStatus reads a single /proc/PID/status file.
func parseProcessStatus(statusPath string, totalRAMkB uint64) *Process {
	data, err := os.ReadFile(statusPath)
	if err != nil {
		return nil
	}

	fields := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			fields[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	pidStr := fields["Pid"]
	pid, _ := strconv.Atoi(pidStr)
	name := fields["Name"]
	user := fields["Uid"]

	vmRSSStr := strings.Fields(fields["VmRSS"])
	if len(vmRSSStr) == 0 {
		return nil
	}
	rssKB, _ := strconv.ParseInt(vmRSSStr[0], 10, 64)
	rssMB := rssKB / 1024

	memPct := 0.0
	if totalRAMkB > 0 {
		memPct = math.Round(float64(rssKB)/float64(totalRAMkB)*1000) / 10
	}

	// Read CPU from /proc/PID/stat (field 14+15 = utime+stime)
	// For simplicity we show the cumulative stat field normalised to seconds
	cpu := readProcCPU(pidStr)

	return &Process{
		PID:  pid,
		Name: name,
		CPU:  cpu,
		MemP: memPct,
		MB:   rssMB,
		User: user,
	}
}

// readProcCPU reads utime+stime from /proc/PID/stat and returns a rough
// CPU percentage based on total process lifetime.
func readProcCPU(pid string) float64 {
	data, err := os.ReadFile("/proc/" + pid + "/stat")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 17 {
		return 0
	}
	utime, _ := strconv.ParseFloat(fields[13], 64)
	stime, _ := strconv.ParseFloat(fields[14], 64)
	starttime, _ := strconv.ParseFloat(fields[21], 64)

	uptimeData, _ := os.ReadFile("/proc/uptime")
	uptimeSec, _ := strconv.ParseFloat(strings.Fields(string(uptimeData))[0], 64)

	clkTck := 100.0 // Hz — standard Linux
	elapsed := uptimeSec - (starttime / clkTck)
	if elapsed <= 0 {
		return 0
	}
	totalCPU := (utime + stime) / clkTck
	pct := (totalCPU / elapsed) * 100.0
	return math.Round(pct*10) / 10
}

// ─── Broadcaster ─────────────────────────────────────────────────────────────
// One goroutine receives stats from the collector.
// It keeps a registry of connected WebSocket clients (each has its own channel).
// When new stats arrive, it fans them out to every client concurrently.

type client struct {
	ch chan Stats
}

type broadcaster struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{clients: make(map[*client]struct{})}
}

func (b *broadcaster) add(c *client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[c] = struct{}{}
}

func (b *broadcaster) remove(c *client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.clients, c)
	close(c.ch)
}

// run reads from the collector channel and broadcasts to all connected clients.
func (b *broadcaster) run(in <-chan Stats) {
	for stats := range in {
		b.mu.RLock()
		// Fan-out: send to every client in parallel using goroutines
		var wg sync.WaitGroup
		for c := range b.clients {
			wg.Add(1)
			go func(cl *client) {
				defer wg.Done()
				select {
				case cl.ch <- stats: // non-blocking: skip slow clients
				default:
				}
			}(c)
		}
		b.mu.RUnlock()
		wg.Wait()
	}
}

// ─── WebSocket (stdlib only) ──────────────────────────────────────────────────
// Go's net/http doesn't ship a WebSocket implementation, but we can do the
// HTTP → WebSocket upgrade manually using net.Conn hijacking + RFC 6455 framing.
// This is educational to see how WebSocket actually works under the hood!

// wsUpgrade performs the HTTP → WebSocket handshake.
func wsUpgrade(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if r.Header.Get("Upgrade") != "websocket" {
		return nil, fmt.Errorf("not a websocket request")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	// WebSocket accept key = base64(sha1(key + magic))
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	io.WriteString(h, key+magic)
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Hijack the raw TCP connection so we can speak WebSocket frames
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("server doesn't support hijacking")
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	// Send the 101 Switching Protocols response
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	buf.WriteString(response)
	buf.Flush()

	return conn, nil
}

// wsSendText writes a WebSocket text frame (opcode 0x01) to the connection.
// WebSocket frames have a specific binary format defined in RFC 6455.
func wsSendText(conn net.Conn, msg []byte) error {
	length := len(msg)
	var header []byte

	// Byte 0: FIN bit (0x80) | opcode text (0x01)
	header = append(header, 0x81)

	// Byte 1+: payload length encoding
	switch {
	case length <= 125:
		header = append(header, byte(length))
	case length <= 65535:
		header = append(header, 126)
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(length))
		header = append(header, b...)
	default:
		header = append(header, 127)
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(length))
		header = append(header, b...)
	}

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := conn.Write(append(header, msg...))
	return err
}

// wsReadFrame reads one WebSocket frame — we only need this to detect
// client disconnect (CLOSE frame) or ping/pong.
func wsReadFrame(conn net.Conn) ([]byte, byte, error) {
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, 0, err
	}

	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	payloadLen := int(header[1] & 0x7F)

	if payloadLen == 126 {
		b := make([]byte, 2)
		io.ReadFull(conn, b)
		payloadLen = int(binary.BigEndian.Uint16(b))
	} else if payloadLen == 127 {
		b := make([]byte, 8)
		io.ReadFull(conn, b)
		payloadLen = int(binary.BigEndian.Uint64(b))
	}

	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		io.ReadFull(conn, maskKey)
	}

	payload := make([]byte, payloadLen)
	io.ReadFull(conn, payload)

	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}

	return payload, opcode, nil
}

// ─── HTTP Handlers ────────────────────────────────────────────────────────────

func wsHandler(b *broadcaster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrade(w, r)
		if err != nil {
			http.Error(w, "WebSocket upgrade failed", 400)
			return
		}

		// Each connected browser tab gets its own buffered channel
		c := &client{ch: make(chan Stats, 2)}
		b.add(c)
		defer b.remove(c)

		log.Printf("client connected from %s", r.RemoteAddr)

		// Goroutine 1: read from channel → write to WebSocket
		// Goroutine 2: read from WebSocket → detect disconnect
		done := make(chan struct{})

		go func() {
			defer close(done)
			for {
				_, opcode, err := wsReadFrame(conn)
				if err != nil || opcode == 8 { // 8 = CLOSE frame
					return
				}
			}
		}()

		for {
			select {
			case stats, ok := <-c.ch:
				if !ok {
					return
				}
				data, err := json.Marshal(stats)
				if err != nil {
					continue
				}
				if err := wsSendText(conn, data); err != nil {
					log.Printf("write error: %v", err)
					return
				}
			case <-done:
				log.Printf("client disconnected from %s", r.RemoteAddr)
				return
			}
		}
	}
}

// ─── Embedded HTML UI ─────────────────────────────────────────────────────────

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, htmlPage)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

// ─── Port helpers ─────────────────────────────────────────────────────────────

// portOwner returns the PID and process name currently listening on the given
// port by scanning /proc/net/tcp (IPv4) and /proc/net/tcp6 (IPv6), then
// cross-referencing /proc/<pid>/fd → socket inode.
func portOwner(port string) (pid int, name string) {
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return 0, ""
	}

	// Build the hex representation of the port as it appears in /proc/net/tcp
	// e.g. port 7070 → "1B9E" (big-endian hex, uppercase)
	hexPort := fmt.Sprintf("%04X", portNum)

	// Collect all inodes that are listening on this port from /proc/net/tcp and tcp6
	listeningInodes := map[string]struct{}{}
	for _, netFile := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(netFile)
		if err != nil {
			continue
		}
		for i, line := range strings.Split(string(data), "\n") {
			if i == 0 {
				continue // skip header
			}
			fields := strings.Fields(line)
			// fields[1] = local_address (hex ip:port), fields[3] = state (0A = listen)
			if len(fields) < 10 {
				continue
			}
			parts := strings.SplitN(fields[1], ":", 2)
			if len(parts) != 2 {
				continue
			}
			if strings.ToUpper(parts[1]) == hexPort && fields[3] == "0A" {
				// fields[9] is the inode number
				listeningInodes[fields[9]] = struct{}{}
			}
		}
	}

	if len(listeningInodes) == 0 {
		return 0, ""
	}

	// Walk every PID's fd directory looking for a socket matching one of our inodes
	pidDirs, _ := filepath.Glob("/proc/[0-9]*/fd")
	for _, fdDir := range pidDirs {
		// Extract PID from path e.g. /proc/1234/fd → 1234
		parts := strings.Split(fdDir, "/")
		if len(parts) < 3 {
			continue
		}
		pidStr := parts[2]
		p, _ := strconv.Atoi(pidStr)
		if p == 0 {
			continue
		}

		links, err := filepath.Glob(fdDir + "/*")
		if err != nil {
			continue
		}
		for _, link := range links {
			target, err := os.Readlink(link)
			if err != nil {
				continue
			}
			// socket symlinks look like: socket:[inode]
			if !strings.HasPrefix(target, "socket:[") {
				continue
			}
			inode := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
			if _, ok := listeningInodes[inode]; ok {
				// Found the owning PID — read its name from /proc/<pid>/comm
				nameBytes, _ := os.ReadFile("/proc/" + pidStr + "/comm")
				return p, strings.TrimSpace(string(nameBytes))
			}
		}
	}
	return 0, ""
}

// killPID sends SIGKILL to the given PID and waits up to 3 seconds for the
// port to actually be released by the kernel before returning.
func killPID(pid int, port string) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("cannot find process %d: %w", pid, err)
	}
	if err := proc.Kill(); err != nil {
		return fmt.Errorf("kill failed: %w", err)
	}

	// Wait for the kernel to reclaim the port (up to 3 s in 200 ms steps)
	fmt.Print("  waiting for port to be released")
	for i := 0; i < 15; i++ {
		time.Sleep(200 * time.Millisecond)
		fmt.Print(".")
		p, _ := portOwner(port)
		if p == 0 {
			fmt.Println(" freed!")
			return nil
		}
	}
	fmt.Println()
	return fmt.Errorf("port %s still busy after 3s", port)
}

// resolvePort checks whether the desired port is free. If it isn't, it shows
// who owns it and lets the user choose: kill the owner, enter a new port, or quit.
// It returns the port string to actually bind on.
func resolvePort(port string) string {
	reader := bufio.NewReader(os.Stdin)

	for {
		ownerPID, ownerName := portOwner(port)
		if ownerPID == 0 {
			// Port is free — we're good
			return port
		}

		// ── Pretty conflict banner ────────────────────────────────────────
		fmt.Println()
		fmt.Println("  ┌─────────────────────────────────────────────────────┐")
		fmt.Printf("  │  ⚠  port %s is already in use                     │\n", port)
		fmt.Println("  ├─────────────────────────────────────────────────────┤")
		fmt.Printf("  │  PID   : %-42d│\n", ownerPID)
		fmt.Printf("  │  name  : %-42s│\n", ownerName)
		fmt.Println("  ├─────────────────────────────────────────────────────┤")
		fmt.Println("  │  [k] kill that process and use this port            │")
		fmt.Println("  │  [p] enter a different port                         │")
		fmt.Println("  │  [q] quit                                           │")
		fmt.Println("  └─────────────────────────────────────────────────────┘")
		fmt.Print("  your choice: ")

		line, _ := reader.ReadString('\n')
		choice := strings.TrimSpace(strings.ToLower(line))

		switch choice {
		case "k":
			fmt.Printf("  killing PID %d (%s)…\n", ownerPID, ownerName)
			if err := killPID(ownerPID, port); err != nil {
				fmt.Printf("  ✗ %v\n", err)
				fmt.Println("  try a different port instead.")
				continue
			}
			// Port should now be free — loop will confirm on next iteration
			fmt.Printf("  ✓ process killed, using port %s\n\n", port)
			return port

		case "p":
			fmt.Print("  enter new port number: ")
			newLine, _ := reader.ReadString('\n')
			newPort := strings.TrimSpace(newLine)
			if _, err := strconv.Atoi(newPort); err != nil || newPort == "" {
				fmt.Println("  invalid port, try again.")
				continue
			}
			port = newPort
			// Loop back to re-check the new port

		case "q":
			fmt.Println("  bye.")
			os.Exit(0)

		default:
			fmt.Println("  please enter k, p, or q.")
		}
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	port := "7070"
	if p := os.Getenv("SYSMON_PORT"); p != "" {
		port = p
	}

	// Check port availability interactively before starting anything
	port = resolvePort(port)

	// Buffered channel between collector and broadcaster
	statsCh := make(chan Stats, 4)
	b := newBroadcaster()

	// Start goroutines
	go collector(statsCh) // goroutine 1: reads /proc
	go b.run(statsCh)     // goroutine 2: fans out to clients

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", indexHandler)
	mux.HandleFunc("/ws", wsHandler(b))

	addr := ":" + port
	fmt.Printf("  ✓ sysmon running → http://localhost%s\n", addr)
	fmt.Println("  press Ctrl+C to stop")
	fmt.Println()

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// ─── HTML/JS UI (embedded as a string) ───────────────────────────────────────

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>sysmon</title>
<style>
  :root {
    --bg: #0d0d0f;
    --surface: #16161a;
    --surface2: #1e1e24;
    --border: #2a2a32;
    --text: #e8e8f0;
    --muted: #6b6b80;
    --accent: #5b6ef5;
    --green: #3ecf8e;
    --amber: #f5a623;
    --red: #f56565;
    --cpu-c: #5b6ef5;
    --ram-c: #3ecf8e;
    --swap-c: #f5a623;
    --vram-c: #b06ef5;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'SF Mono','Fira Code','Consolas',monospace;
    font-size: 13px;
    min-height: 100vh;
    padding: 24px;
  }
  header {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid var(--border);
  }
  .logo { font-size: 20px; font-weight: 700; letter-spacing: -0.5px; }
  .logo span { color: var(--accent); }
  .status-row { display: flex; align-items: center; gap: 14px; font-size: 11px; color: var(--muted); }
  .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--muted); display: inline-block; }
  .dot.live { background: var(--green); animation: blink 1.4s ease-in-out infinite; }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.3} }

  /* ── system info bar ── */
  .sys-info {
    display: flex; gap: 20px; flex-wrap: wrap;
    font-size: 11px; color: var(--muted);
    margin-bottom: 20px; padding: 10px 14px;
    background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  }
  .sys-info span { color: var(--text); }

  /* ── metric cards ── */
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr)); gap: 10px; margin-bottom: 20px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
  .card-label { font-size: 10px; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; }
  .card-val { font-size: 24px; font-weight: 700; line-height: 1; margin-bottom: 3px; }
  .card-sub { font-size: 10px; color: var(--muted); }

  /* ── totals strip ── */
  .totals-strip {
    display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 20px;
  }
  .total-card {
    background: var(--surface2); border: 1px solid var(--border); border-radius: 10px;
    padding: 12px 16px; display: flex; flex-direction: column; gap: 6px;
  }
  .total-label { font-size: 10px; letter-spacing: .09em; text-transform: uppercase; color: var(--muted); }
  .total-row { display: flex; align-items: center; gap: 10px; }
  .total-used { font-size: 18px; font-weight: 700; }
  .total-sep { color: var(--muted); font-size: 13px; }
  .total-total { font-size: 13px; color: var(--muted); }
  .total-track { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .total-fill { height: 100%; border-radius: 2px; transition: width .6s cubic-bezier(.4,0,.2,1); }

  /* ── resource bars ── */
  .gauges {
    background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
    padding: 18px 20px; margin-bottom: 20px;
  }
  .gauge-title { font-size: 10px; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); margin-bottom: 14px; }
  .gauge-row { display: grid; grid-template-columns: 56px 1fr 52px; align-items: center; gap: 12px; margin-bottom: 12px; }
  .gauge-row:last-child { margin-bottom: 0; }
  .gauge-name { font-size: 11px; color: var(--muted); text-align: right; }
  .track { height: 5px; background: var(--border); border-radius: 3px; overflow: hidden; }
  .fill { height: 100%; border-radius: 3px; transition: width .6s cubic-bezier(.4,0,.2,1); }
  .gauge-pct { font-size: 11px; color: var(--text); text-align: right; }

  /* ── sparklines ── */
  .sparklines { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 20px; }
  .spark-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
  .spark-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
  .spark-label { font-size: 10px; letter-spacing: .08em; text-transform: uppercase; color: var(--muted); }
  .spark-val { font-size: 13px; font-weight: 700; }
  canvas { width: 100% !important; height: 44px !important; display: block; }

  /* ── process table ── */
  .proc-section { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  .proc-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid var(--border); }
  .proc-title { font-size: 10px; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  .proc-count { font-size: 11px; color: var(--muted); }
  table { width: 100%; border-collapse: collapse; }
  th {
    font-size: 10px; letter-spacing: .08em; text-transform: uppercase; color: var(--muted);
    text-align: left; padding: 9px 16px; border-bottom: 1px solid var(--border); font-weight: 500;
  }
  th:not(:first-child) { text-align: right; }
  td { padding: 8px 16px; font-size: 12px; border-bottom: 1px solid rgba(42,42,50,.5); vertical-align: middle; }
  td:not(:first-child) { text-align: right; color: var(--muted); }
  tr.group-row:last-child td { border-bottom: none; }
  tr.group-row:hover td { background: rgba(91,110,245,.05); cursor: pointer; }
  tr.group-row td { transition: background .15s; }

  /* group name cell */
  .name-cell { display: flex; align-items: center; gap: 8px; }
  .expand-btn {
    width: 16px; height: 16px; border-radius: 4px;
    background: var(--border); border: none; cursor: pointer;
    color: var(--muted); font-size: 10px; display: flex; align-items: center; justify-content: center;
    flex-shrink: 0; transition: background .15s, color .15s;
  }
  .expand-btn:hover { background: var(--accent); color: #fff; }
  .expand-btn.open { background: var(--accent); color: #fff; }
  .proc-name-text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 180px; color: var(--text); }
  .instance-badge {
    font-size: 9px; padding: 1px 6px; border-radius: 10px;
    background: rgba(91,110,245,.15); color: var(--accent); flex-shrink: 0;
  }

  /* expanded children rows */
  tr.child-row td { background: var(--surface2); color: var(--muted); font-size: 11px; padding: 5px 16px 5px 44px; border-bottom: 1px solid rgba(42,42,50,.3); }
  tr.child-row td:not(:first-child) { text-align: right; }
  tr.child-row:last-child td { border-bottom: 1px solid var(--border); }
  tr.child-row { display: none; }
  tr.child-row.visible { display: table-row; }

  .badge { display: inline-block; padding: 2px 7px; border-radius: 20px; font-size: 9px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
  .hi { background: rgba(245,101,101,.15); color: var(--red); }
  .md { background: rgba(245,166,35,.12); color: var(--amber); }
  .lo { background: rgba(62,207,142,.1); color: var(--green); }

  footer { margin-top: 20px; text-align: center; font-size: 10px; color: var(--muted); }
</style>
</head>
<body>

<header>
  <div class="logo">sys<span>mon</span></div>
  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span id="conn-status">connecting…</span>
    <span>|</span>
    <span id="ts">—</span>
    <span>| updates every 2s</span>
  </div>
</header>

<div class="sys-info">
  <div>cpu <span id="si-cpu">—</span></div>
  <div>cores <span id="si-cores">—</span></div>
  <div>uptime <span id="si-uptime">—</span></div>
  <div>load avg <span id="si-load">—</span></div>
  <div>processes <span id="si-procs">—</span></div>
</div>

<!-- live metric cards -->
<div class="cards">
  <div class="card">
    <div class="card-label">CPU</div>
    <div class="card-val" id="cv-cpu" style="color:var(--cpu-c)">—</div>
    <div class="card-sub" id="cs-cpu">—</div>
  </div>
  <div class="card">
    <div class="card-label">RAM used</div>
    <div class="card-val" id="cv-ram" style="color:var(--ram-c)">—</div>
    <div class="card-sub" id="cs-ram">—</div>
  </div>
  <div class="card">
    <div class="card-label">Swap used</div>
    <div class="card-val" id="cv-swap" style="color:var(--swap-c)">—</div>
    <div class="card-sub" id="cs-swap">—</div>
  </div>
  <div class="card">
    <div class="card-label">VRAM used</div>
    <div class="card-val" id="cv-vram" style="color:var(--vram-c)">—</div>
    <div class="card-sub" id="cs-vram">—</div>
  </div>
</div>

<!-- TOTALS STRIP: used / total with bar -->
<div class="totals-strip">
  <div class="total-card">
    <div class="total-label">RAM total</div>
    <div class="total-row">
      <span class="total-used" id="tt-ram-used" style="color:var(--ram-c)">—</span>
      <span class="total-sep">/</span>
      <span class="total-total" id="tt-ram-total">—</span>
    </div>
    <div class="total-row">
      <div class="total-track"><div class="total-fill" id="tf-ram" style="background:var(--ram-c)"></div></div>
      <span style="font-size:10px;color:var(--muted)" id="tt-ram-pct">—</span>
    </div>
  </div>
  <div class="total-card">
    <div class="total-label">Swap total</div>
    <div class="total-row">
      <span class="total-used" id="tt-swap-used" style="color:var(--swap-c)">—</span>
      <span class="total-sep">/</span>
      <span class="total-total" id="tt-swap-total">—</span>
    </div>
    <div class="total-row">
      <div class="total-track"><div class="total-fill" id="tf-swap" style="background:var(--swap-c)"></div></div>
      <span style="font-size:10px;color:var(--muted)" id="tt-swap-pct">—</span>
    </div>
  </div>
  <div class="total-card">
    <div class="total-label">VRAM total</div>
    <div class="total-row">
      <span class="total-used" id="tt-vram-used" style="color:var(--vram-c)">—</span>
      <span class="total-sep">/</span>
      <span class="total-total" id="tt-vram-total">—</span>
    </div>
    <div class="total-row">
      <div class="total-track"><div class="total-fill" id="tf-vram" style="background:var(--vram-c)"></div></div>
      <span style="font-size:10px;color:var(--muted)" id="tt-vram-pct">—</span>
    </div>
  </div>
</div>

<!-- resource bars -->
<div class="gauges">
  <div class="gauge-title">live resource bars</div>
  <div class="gauge-row">
    <div class="gauge-name">CPU</div>
    <div class="track"><div class="fill" id="bar-cpu" style="background:var(--cpu-c)"></div></div>
    <div class="gauge-pct" id="pct-cpu">—</div>
  </div>
  <div class="gauge-row">
    <div class="gauge-name">RAM</div>
    <div class="track"><div class="fill" id="bar-ram" style="background:var(--ram-c)"></div></div>
    <div class="gauge-pct" id="pct-ram">—</div>
  </div>
  <div class="gauge-row">
    <div class="gauge-name">Swap</div>
    <div class="track"><div class="fill" id="bar-swap" style="background:var(--swap-c)"></div></div>
    <div class="gauge-pct" id="pct-swap">—</div>
  </div>
  <div class="gauge-row">
    <div class="gauge-name">VRAM</div>
    <div class="track"><div class="fill" id="bar-vram" style="background:var(--vram-c)"></div></div>
    <div class="gauge-pct" id="pct-vram">—</div>
  </div>
</div>

<!-- sparklines -->
<div class="sparklines">
  <div class="spark-card">
    <div class="spark-header"><div class="spark-label">CPU history</div><div class="spark-val" id="sp-cpu" style="color:var(--cpu-c)">—</div></div>
    <canvas id="sc-cpu"></canvas>
  </div>
  <div class="spark-card">
    <div class="spark-header"><div class="spark-label">RAM history</div><div class="spark-val" id="sp-ram" style="color:var(--ram-c)">—</div></div>
    <canvas id="sc-ram"></canvas>
  </div>
  <div class="spark-card">
    <div class="spark-header"><div class="spark-label">Swap history</div><div class="spark-val" id="sp-swap" style="color:var(--swap-c)">—</div></div>
    <canvas id="sc-swap"></canvas>
  </div>
  <div class="spark-card">
    <div class="spark-header"><div class="spark-label">VRAM history</div><div class="spark-val" id="sp-vram" style="color:var(--vram-c)">—</div></div>
    <canvas id="sc-vram"></canvas>
  </div>
</div>

<!-- grouped process table -->
<div class="proc-section">
  <div class="proc-header">
    <div class="proc-title">processes — grouped by name  <span style="color:var(--muted);font-weight:400">▸ click row to expand PIDs</span></div>
    <div class="proc-count" id="proc-count">—</div>
  </div>
  <table>
    <thead>
      <tr>
        <th>name</th>
        <th>instances</th>
        <th>cpu%</th>
        <th>mem%</th>
        <th>ram MB</th>
        <th>level</th>
      </tr>
    </thead>
    <tbody id="proc-tbody">
      <tr><td colspan="6" style="text-align:center;color:var(--muted);padding:24px">waiting for data…</td></tr>
    </tbody>
  </table>
</div>

<footer>sysmon &mdash; go + websocket &mdash; reads /proc directly &mdash; zero external deps</footer>

<script>
const MAX_HISTORY = 60;
const hist = { cpu: [], ram: [], swap: [], vram: [] };

// Track which groups are expanded across renders
const expanded = new Set();

function sparkline(id, data, color) {
  const c = document.getElementById(id);
  const dpr = window.devicePixelRatio || 1;
  c.width = c.offsetWidth * dpr;
  c.height = 44 * dpr;
  const ctx = c.getContext('2d');
  ctx.scale(dpr, dpr);
  const w = c.offsetWidth, h = 44;
  ctx.clearRect(0, 0, w, h);
  if (data.length < 2) return;
  const step = w / (MAX_HISTORY - 1);
  ctx.beginPath();
  data.forEach((v, i) => {
    const x = i * step;
    const y = h - (v / 100) * (h - 4) - 2;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.stroke();
  ctx.lineTo((data.length - 1) * step, h); ctx.lineTo(0, h); ctx.closePath();
  ctx.fillStyle = color + '18'; ctx.fill();
}

function set(id, v) { const e = document.getElementById(id); if (e) e.textContent = v; }
function setW(id, pct) { const e = document.getElementById(id); if (e) e.style.width = Math.min(pct, 100) + '%'; }

function badge(mem) {
  if (mem >= 4) return '<span class="badge hi">high</span>';
  if (mem >= 2) return '<span class="badge md">mid</span>';
  return '<span class="badge lo">low</span>';
}

function mb(v) { return v >= 1024 ? (v/1024).toFixed(1) + ' GB' : v + ' MB'; }

function toggleGroup(name) {
  if (expanded.has(name)) expanded.delete(name);
  else expanded.add(name);
  // toggle visible class on all child rows for this group
  document.querySelectorAll('[data-parent="' + CSS.escape(name) + '"]').forEach(tr => {
    tr.classList.toggle('visible');
  });
  const btn = document.getElementById('btn-' + CSS.escape(name));
  if (btn) btn.classList.toggle('open');
}

function render(d) {
  set('ts', d.timestamp);
  set('si-cpu', d.cpuModel ? d.cpuModel.replace(/\(R\)|\(TM\)/g, '') : '—');
  set('si-cores', d.cpuCores + ' cores');
  set('si-uptime', d.uptime);
  set('si-load', d.loadAvg);
  set('si-procs', d.procCount + ' total');

  // metric cards
  set('cv-cpu', d.cpu.toFixed(1) + '%');
  set('cs-cpu', d.cpuCores + ' logical cores');
  set('cv-ram', d.ram.usedGB.toFixed(1) + ' GB');
  set('cs-ram', d.ram.usedGB.toFixed(1) + ' / ' + d.ram.totalGB.toFixed(1) + ' GB  (' + d.ram.pct.toFixed(0) + '%)');
  set('cv-swap', d.swap.usedGB.toFixed(1) + ' GB');
  set('cs-swap', d.swap.usedGB.toFixed(1) + ' / ' + d.swap.totalGB.toFixed(1) + ' GB  (' + d.swap.pct.toFixed(0) + '%)');
  set('cv-vram', d.vram.pct.toFixed(0) + '%');
  set('cs-vram', d.vram.name || 'N/A');

  // totals strip
  set('tt-ram-used',   d.ram.usedGB.toFixed(1) + ' GB');
  set('tt-ram-total',  d.ram.totalGB.toFixed(1) + ' GB');
  set('tt-ram-pct',    d.ram.pct.toFixed(0) + '%');
  setW('tf-ram', d.ram.pct);

  set('tt-swap-used',  d.swap.usedGB.toFixed(1) + ' GB');
  set('tt-swap-total', d.swap.totalGB.toFixed(1) + ' GB');
  set('tt-swap-pct',   d.swap.pct.toFixed(0) + '%');
  setW('tf-swap', d.swap.pct);

  const vramUsed  = d.vram.usedMB >= 1024 ? (d.vram.usedMB/1024).toFixed(1) + ' GB' : d.vram.usedMB + ' MB';
  const vramTotal = d.vram.totalMB >= 1024 ? (d.vram.totalMB/1024).toFixed(1) + ' GB' : (d.vram.totalMB || '—') + ' MB';
  set('tt-vram-used',  vramUsed);
  set('tt-vram-total', vramTotal);
  set('tt-vram-pct',   d.vram.pct.toFixed(0) + '%');
  setW('tf-vram', d.vram.pct);

  // bars
  setW('bar-cpu', d.cpu);    set('pct-cpu',  d.cpu.toFixed(1) + '%');
  setW('bar-ram', d.ram.pct); set('pct-ram', d.ram.pct.toFixed(0) + '%');
  setW('bar-swap', d.swap.pct); set('pct-swap', d.swap.pct.toFixed(0) + '%');
  setW('bar-vram', d.vram.pct); set('pct-vram', d.vram.pct.toFixed(0) + '%');

  // sparklines
  const push = (a, v) => { a.push(v); if (a.length > MAX_HISTORY) a.shift(); };
  push(hist.cpu, d.cpu); push(hist.ram, d.ram.pct);
  push(hist.swap, d.swap.pct); push(hist.vram, d.vram.pct);
  set('sp-cpu', d.cpu.toFixed(1) + '%'); set('sp-ram', d.ram.pct.toFixed(0) + '%');
  set('sp-swap', d.swap.pct.toFixed(0) + '%'); set('sp-vram', d.vram.pct.toFixed(0) + '%');
  sparkline('sc-cpu', hist.cpu, '#5b6ef5'); sparkline('sc-ram', hist.ram, '#3ecf8e');
  sparkline('sc-swap', hist.swap, '#f5a623'); sparkline('sc-vram', hist.vram, '#b06ef5');

  // grouped process table
  set('proc-count', d.procCount + ' processes  /  ' + (d.procs ? d.procs.length : 0) + ' groups shown');
  if (!d.procs || d.procs.length === 0) return;

  const tbody = document.getElementById('proc-tbody');
  let html = '';

  d.procs.forEach(g => {
    const safeId = g.name.replace(/[^a-zA-Z0-9_-]/g, '_');
    const isOpen = expanded.has(g.name);
    const btnClass = isOpen ? 'expand-btn open' : 'expand-btn';
    const hasMulti = g.count > 1;

    // group row — build with string concat to avoid backtick conflicts inside Go raw string
    const safeName = g.name.replace(/'/g, "\\'");
    html += '<tr class="group-row" onclick="toggleGroup(\'' + safeName + '\')">' +
      '<td><div class="name-cell">' +
      '<button class="' + btnClass + '" id="btn-' + safeId + '" onclick="event.stopPropagation();toggleGroup(\'' + safeName + '\')">' +
      (isOpen ? '▾' : '▸') +
      '</button>' +
      '<span class="proc-name-text" title="' + g.name + '">' + g.name + '</span>' +
      (hasMulti ? '<span class="instance-badge">&times;' + g.count + '</span>' : '') +
      '</div></td>' +
      '<td>' + g.count + '</td>' +
      '<td>' + g.cpu.toFixed(1) + '</td>' +
      '<td>' + g.memP.toFixed(1) + '</td>' +
      '<td>' + g.mb + '</td>' +
      '<td>' + badge(g.memP) + '</td>' +
      '</tr>';

    // child rows (one per PID) — shown only when expanded
    g.children.forEach(ch => {
      const visClass = isOpen ? 'child-row visible' : 'child-row';
      const pname = g.name.replace(/"/g, '&quot;');
      html += '<tr class="' + visClass + '" data-parent="' + pname + '">' +
        '<td>PID ' + ch.pid + '</td>' +
        '<td>—</td>' +
        '<td>' + ch.cpu.toFixed(1) + '</td>' +
        '<td>' + ch.memP.toFixed(1) + '</td>' +
        '<td>' + ch.mb + '</td>' +
        '<td></td>' +
        '</tr>';
    });
  });

  tbody.innerHTML = html;
}

function connect() {
  const ws = new WebSocket('ws://' + location.host + '/ws');
  ws.onopen = () => {
    set('conn-status', 'connected');
    document.getElementById('dot').classList.add('live');
  };
  ws.onmessage = e => { try { render(JSON.parse(e.data)); } catch(err) { console.error(err); } };
  ws.onclose = () => {
    set('conn-status', 'reconnecting…');
    document.getElementById('dot').classList.remove('live');
    setTimeout(connect, 2000);
  };
  ws.onerror = () => ws.close();
}

connect();
</script>
</body>
</html>`

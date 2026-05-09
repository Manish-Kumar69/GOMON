package main

// ══════════════════════════════════════════════════════════════════════════════
//  webbot — personal web automation tool
//
//  Architecture:
//    • Chrome launcher    — starts Chrome with --remote-debugging-port
//    • CDP client         — speaks Chrome DevTools Protocol over WebSocket
//    • Workflow engine    — executes steps (click, type, scroll, screenshot…)
//    • Scheduler          — runs workflows on a cron-like interval
//    • HTTP + WS server   — serves the visual builder UI + streams live logs
//    • Workflow store     — loads/saves JSON workflow files from ./workflows/
//
//  Usage:
//    go build -o webbot . && ./webbot
//    open http://localhost:8888
// ══════════════════════════════════════════════════════════════════════════════

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Data model ───────────────────────────────────────────────────────────────

// StepKind enumerates every action a workflow step can perform.
type StepKind string

const (
	StepNavigate   StepKind = "navigate"   // go to URL
	StepClick      StepKind = "click"      // click CSS selector
	StepType       StepKind = "type"       // type text into selector
	StepScroll     StepKind = "scroll"     // scroll by X,Y pixels
	StepWait       StepKind = "wait"       // wait N seconds
	StepWaitFor    StepKind = "waitFor"    // wait until selector appears
	StepScreenshot StepKind = "screenshot" // capture PNG
	StepScrape     StepKind = "scrape"     // extract innerText of selector
	StepEval       StepKind = "eval"       // run arbitrary JS
)

// Step is one action inside a workflow.
type Step struct {
	ID       string   `json:"id"`
	Kind     StepKind `json:"kind"`
	Selector string   `json:"selector,omitempty"` // CSS selector
	Value    string   `json:"value,omitempty"`    // text / JS / URL / seconds
	X        int      `json:"x,omitempty"`        // scroll X
	Y        int      `json:"y,omitempty"`        // scroll Y
}

// Schedule holds optional recurring run config.
type Schedule struct {
	Enabled      bool   `json:"enabled"`
	IntervalSecs int    `json:"intervalSecs"` // 0 = disabled
	NextRun      string `json:"nextRun,omitempty"`
}

// Workflow is the top-level unit users create and save.
type Workflow struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Steps       []Step   `json:"steps"`
	Schedule    Schedule `json:"schedule"`
	CreatedAt   string   `json:"createdAt"`
	UpdatedAt   string   `json:"updatedAt"`
}

// RunLog is a single line streamed to the UI during execution.
type RunLog struct {
	Time    string `json:"time"`
	Level   string `json:"level"` // info | ok | error | data
	Message string `json:"message"`
}

// RunResult is sent to the UI when a workflow finishes.
type RunResult struct {
	WorkflowID string            `json:"workflowId"`
	Status     string            `json:"status"` // running | done | error
	Logs       []RunLog          `json:"logs,omitempty"`
	Scraped    map[string]string `json:"scraped,omitempty"`
	Screenshot string            `json:"screenshot,omitempty"` // base64 PNG
}

// WSMessage is the envelope for all WebSocket messages.
type WSMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// ─── Workflow store ───────────────────────────────────────────────────────────

const workflowDir = "./workflows"

type Store struct {
	mu sync.RWMutex
}

func newStore() *Store {
	os.MkdirAll(workflowDir, 0755)
	return &Store{}
}

func (s *Store) List() ([]Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	files, err := filepath.Glob(filepath.Join(workflowDir, "*.json"))
	if err != nil {
		return nil, err
	}
	var workflows []Workflow
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var wf Workflow
		if err := json.Unmarshal(data, &wf); err != nil {
			continue
		}
		workflows = append(workflows, wf)
	}
	return workflows, nil
}

func (s *Store) Get(id string) (*Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(filepath.Join(workflowDir, id+".json"))
	if err != nil {
		return nil, err
	}
	var wf Workflow
	return &wf, json.Unmarshal(data, &wf)
}

func (s *Store) Save(wf *Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	wf.UpdatedAt = time.Now().Format(time.RFC3339)
	data, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(workflowDir, wf.ID+".json"), data, 0644)
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.Remove(filepath.Join(workflowDir, id+".json"))
}

// ─── CDP client ───────────────────────────────────────────────────────────────
// Speaks the Chrome DevTools Protocol over a raw WebSocket connection.
// CDP is JSON-RPC: send {"id":N,"method":"...","params":{...}}, receive {"id":N,"result":{...}}

type CDPClient struct {
	conn    net.Conn
	mu      sync.Mutex
	counter int64
	pending map[int64]chan json.RawMessage
	pmu     sync.Mutex
}

func dialCDP(wsURL string) (*CDPClient, error) {
	// wsURL looks like: ws://localhost:9222/devtools/page/XXXX
	// We need to do the WebSocket upgrade ourselves (stdlib only)
	addr := strings.TrimPrefix(wsURL, "ws://")
	slashIdx := strings.Index(addr, "/")
	host := addr[:slashIdx]
	path := addr[slashIdx:]

	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", host, err)
	}

	// WebSocket handshake
	key := base64.StdEncoding.EncodeToString([]byte("webbot-cdp-key-1"))
	handshake := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, host, key)
	conn.Write([]byte(handshake))

	// Read response headers
	buf := bufio.NewReader(conn)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == "\r\n" {
			break
		}
	}

	c := &CDPClient{
		conn:    conn,
		pending: make(map[int64]chan json.RawMessage),
	}

	// Start reader goroutine — routes CDP responses back to callers
	go c.readLoop(buf)
	return c, nil
}

// readLoop reads CDP WebSocket frames and routes responses to waiting callers.
func (c *CDPClient) readLoop(buf *bufio.Reader) {
	for {
		payload, err := wsReadFrameFromBuf(buf)
		if err != nil {
			return
		}

		var msg struct {
			ID     int64           `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg.ID == 0 {
			continue // event, not a response
		}

		c.pmu.Lock()
		ch, ok := c.pending[msg.ID]
		if ok {
			delete(c.pending, msg.ID)
		}
		c.pmu.Unlock()

		if ok {
			if msg.Error != nil {
				ch <- json.RawMessage(`{"__error":"` + msg.Error.Message + `"}`)
			} else {
				ch <- msg.Result
			}
		}
	}
}

// Send sends a CDP command and waits for the response.
func (c *CDPClient) Send(method string, params interface{}) (json.RawMessage, error) {
	id := atomic.AddInt64(&c.counter, 1)

	type cdpMsg struct {
		ID     int64       `json:"id"`
		Method string      `json:"method"`
		Params interface{} `json:"params"`
	}

	data, err := json.Marshal(cdpMsg{ID: id, Method: method, Params: params})
	if err != nil {
		return nil, err
	}

	ch := make(chan json.RawMessage, 1)
	c.pmu.Lock()
	c.pending[id] = ch
	c.pmu.Unlock()

	if err := c.wsSend(data); err != nil {
		c.pmu.Lock()
		delete(c.pending, id)
		c.pmu.Unlock()
		return nil, err
	}

	select {
	case result := <-ch:
		var errCheck map[string]string
		if json.Unmarshal(result, &errCheck) == nil {
			if e, ok := errCheck["__error"]; ok {
				return nil, fmt.Errorf("CDP error: %s", e)
			}
		}
		return result, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("CDP timeout for %s", method)
	}
}

// wsSend writes a WebSocket text frame (unmasked, server→client direction for CDP).
func (c *CDPClient) wsSend(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	length := len(data)
	var header []byte
	header = append(header, 0x81) // FIN + text opcode

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

	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.conn.Write(append(header, data...))
	return err
}

// wsReadFrameFromBuf reads one WebSocket frame from a buffered reader.
func wsReadFrameFromBuf(buf *bufio.Reader) ([]byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(buf, header); err != nil {
		return nil, err
	}

	masked := header[1]&0x80 != 0
	payloadLen := int(header[1] & 0x7F)

	if payloadLen == 126 {
		b := make([]byte, 2)
		io.ReadFull(buf, b)
		payloadLen = int(binary.BigEndian.Uint16(b))
	} else if payloadLen == 127 {
		b := make([]byte, 8)
		io.ReadFull(buf, b)
		payloadLen = int(binary.BigEndian.Uint64(b))
	}

	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		io.ReadFull(buf, maskKey)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(buf, payload); err != nil {
		return nil, err
	}

	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return payload, nil
}

// ─── Chrome launcher ──────────────────────────────────────────────────────────

type ChromeManager struct {
	mu        sync.Mutex
	cmd       *exec.Cmd
	debugPort int
	pageWSURL string
}

func newChromeManager() *ChromeManager {
	return &ChromeManager{debugPort: 9222}
}

// EnsureRunning starts Chrome if it isn't already running.
func (cm *ChromeManager) EnsureRunning(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.cmd != nil && cm.cmd.Process != nil {
		// Check if still alive
		if err := cm.cmd.Process.Signal(os.Signal(nil)); err == nil {
			return nil
		}
	}

	// Find Chrome binary
	candidates := []string{
		"google-chrome", "google-chrome-stable",
		"chromium", "chromium-browser",
		"/opt/google/chrome/chrome",
		"/usr/bin/chromium",
		"/usr/bin/chromium-browser",
	}
	var chromeBin string
	for _, c := range candidates {
		if path, err := exec.LookPath(c); err == nil {
			chromeBin = path
			break
		}
		if _, err := os.Stat(c); err == nil {
			chromeBin = c
			break
		}
	}
	if chromeBin == "" {
		return fmt.Errorf("chrome/chromium not found — please install Google Chrome or Chromium")
	}

	args := []string{
		"--headless=new",
		fmt.Sprintf("--remote-debugging-port=%d", cm.debugPort),
		"--no-sandbox",
		"--disable-gpu",
		"--disable-dev-shm-usage",
		"--disable-extensions",
		"--no-first-run",
		"--no-default-browser-check",
		"--user-data-dir=/tmp/webbot-chrome",
		"about:blank",
	}

	cm.cmd = exec.CommandContext(ctx, chromeBin, args...)
	cm.cmd.Stdout = io.Discard
	cm.cmd.Stderr = io.Discard

	if err := cm.cmd.Start(); err != nil {
		return fmt.Errorf("start chrome: %w", err)
	}

	// Wait for Chrome to be ready (up to 10s)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/json/version", cm.debugPort))
		if err == nil {
			resp.Body.Close()
			break
		}
	}

	return nil
}

// NewPage opens a new Chrome tab and returns its CDP WebSocket URL.
func (cm *ChromeManager) NewPage() (string, error) {
	url := fmt.Sprintf("http://localhost:%d/json/new", cm.debugPort)

	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var info struct {
		WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
	}

	if err := json.Unmarshal(body, &info); err != nil {
		return "", fmt.Errorf("invalid response from chrome: %s", string(body))
	}

	if info.WebSocketDebuggerURL == "" {
		return "", fmt.Errorf("no websocket debugger URL returned")
	}

	return info.WebSocketDebuggerURL, nil
}

// ClosePage closes a Chrome tab by its WebSocket URL.
func (cm *ChromeManager) ClosePage(wsURL string) {
	// Extract target ID from ws URL: ws://localhost:9222/devtools/page/<id>
	parts := strings.Split(wsURL, "/")
	if len(parts) == 0 {
		return
	}
	id := parts[len(parts)-1]
	http.Get(fmt.Sprintf("http://localhost:%d/json/close/%s", cm.debugPort, id))
}

// ─── Workflow engine ──────────────────────────────────────────────────────────

type Engine struct {
	chrome *ChromeManager
}

func newEngine(chrome *ChromeManager) *Engine {
	return &Engine{chrome: chrome}
}

// Run executes all steps in a workflow, streaming logs via the logFn callback.
// It returns a RunResult with any scraped data and the last screenshot.
func (e *Engine) Run(ctx context.Context, wf *Workflow, logFn func(RunLog)) RunResult {
	result := RunResult{
		WorkflowID: wf.ID,
		Status:     "running",
		Scraped:    make(map[string]string),
	}

	info := func(msg string) {
		logFn(RunLog{Time: time.Now().Format("15:04:05"), Level: "info", Message: msg})
	}
	ok := func(msg string) {
		logFn(RunLog{Time: time.Now().Format("15:04:05"), Level: "ok", Message: msg})
	}
	fail := func(msg string) {
		logFn(RunLog{Time: time.Now().Format("15:04:05"), Level: "error", Message: msg})
	}
	data := func(key, val string) {
		logFn(RunLog{Time: time.Now().Format("15:04:05"), Level: "data", Message: key + ": " + val})
		result.Scraped[key] = val
	}

	// Ensure Chrome is running
	info("starting Chrome…")
	if err := e.chrome.EnsureRunning(ctx); err != nil {
		fail(err.Error())
		result.Status = "error"
		return result
	}
	ok("Chrome ready")

	// Open a new tab
	info("opening new browser tab…")
	wsURL, err := e.chrome.NewPage()
	if err != nil {
		fail("could not open tab: " + err.Error())
		result.Status = "error"
		return result
	}
	defer e.chrome.ClosePage(wsURL)

	// Connect CDP
	cdp, err := dialCDP(wsURL)
	if err != nil {
		fail("CDP connect failed: " + err.Error())
		result.Status = "error"
		return result
	}

	// Enable required CDP domains
	cdp.Send("Page.enable", map[string]interface{}{})
	cdp.Send("Runtime.enable", map[string]interface{}{})
	cdp.Send("DOM.enable", map[string]interface{}{})

	ok(fmt.Sprintf("connected to browser (tab %s)", wsURL[len(wsURL)-8:]))

	// ── Execute steps ─────────────────────────────────────────────────────
	for i, step := range wf.Steps {
		select {
		case <-ctx.Done():
			fail("workflow cancelled")
			result.Status = "error"
			return result
		default:
		}

		info(fmt.Sprintf("[%d/%d] %s: %s", i+1, len(wf.Steps), step.Kind, stepDesc(step)))

		var stepErr error

		switch step.Kind {

		// ── Navigate ──────────────────────────────────────────────────────
		case StepNavigate:
			_, stepErr = cdp.Send("Page.navigate", map[string]interface{}{"url": step.Value})
			if stepErr == nil {
				// Wait for load
				_, stepErr = cdp.Send("Page.loadEventFired", nil)
				// Give JS a moment to hydrate
				time.Sleep(1 * time.Second)
				ok("navigated to " + step.Value)
			}

		// ── Click ─────────────────────────────────────────────────────────
		case StepClick:
			x, y, err := elementCenter(cdp, step.Selector)
			if err != nil {
				stepErr = err
				break
			}
			// Mouse press + release at element center
			cdp.Send("Input.dispatchMouseEvent", map[string]interface{}{
				"type": "mousePressed", "x": x, "y": y,
				"button": "left", "clickCount": 1,
			})
			cdp.Send("Input.dispatchMouseEvent", map[string]interface{}{
				"type": "mouseReleased", "x": x, "y": y,
				"button": "left", "clickCount": 1,
			})
			time.Sleep(300 * time.Millisecond)
			ok("clicked " + step.Selector)

		// ── Type ──────────────────────────────────────────────────────────
		case StepType:
			// Focus the element first
			_, err := cdp.Send("Runtime.evaluate", map[string]interface{}{
				"expression": fmt.Sprintf(`document.querySelector(%q).focus()`, step.Selector),
			})
			if err != nil {
				stepErr = err
				break
			}
			// Send each character as a key event
			for _, ch := range step.Value {
				char := string(ch)
				cdp.Send("Input.dispatchKeyEvent", map[string]interface{}{
					"type": "keyDown", "text": char,
				})
				cdp.Send("Input.dispatchKeyEvent", map[string]interface{}{
					"type": "keyUp", "text": char,
				})
				time.Sleep(30 * time.Millisecond)
			}
			ok(fmt.Sprintf("typed %d chars into %s", len(step.Value), step.Selector))

		// ── Scroll ────────────────────────────────────────────────────────
		case StepScroll:
			_, stepErr = cdp.Send("Runtime.evaluate", map[string]interface{}{
				"expression": fmt.Sprintf("window.scrollBy(%d, %d)", step.X, step.Y),
			})
			if stepErr == nil {
				time.Sleep(500 * time.Millisecond)
				ok(fmt.Sprintf("scrolled by (%d, %d)", step.X, step.Y))
			}

		// ── Wait (fixed duration) ─────────────────────────────────────────
		case StepWait:
			secs, _ := strconv.ParseFloat(step.Value, 64)
			if secs <= 0 {
				secs = 1
			}
			info(fmt.Sprintf("waiting %.1fs…", secs))
			select {
			case <-time.After(time.Duration(secs * float64(time.Second))):
			case <-ctx.Done():
			}
			ok(fmt.Sprintf("waited %.1fs", secs))

		// ── Wait for selector ─────────────────────────────────────────────
		case StepWaitFor:
			deadline := time.Now().Add(15 * time.Second)
			found := false
			for time.Now().Before(deadline) {
				res, err := cdp.Send("Runtime.evaluate", map[string]interface{}{
					"expression": fmt.Sprintf(`!!document.querySelector(%q)`, step.Selector),
				})
				if err == nil {
					var r struct {
						Result struct{ Value bool }
					}
					if json.Unmarshal(res, &r) == nil && r.Result.Value {
						found = true
						break
					}
				}
				time.Sleep(500 * time.Millisecond)
			}
			if !found {
				stepErr = fmt.Errorf("selector %q not found within 15s", step.Selector)
			} else {
				ok("element found: " + step.Selector)
			}

		// ── Screenshot ────────────────────────────────────────────────────
		case StepScreenshot:
			res, err := cdp.Send("Page.captureScreenshot", map[string]interface{}{
				"format": "png", "quality": 90,
			})
			if err != nil {
				stepErr = err
				break
			}
			var r struct {
				Data string `json:"data"`
			}
			if err := json.Unmarshal(res, &r); err != nil {
				stepErr = err
				break
			}
			result.Screenshot = r.Data
			// Optionally save to disk
			if step.Value != "" {
				imgData, _ := base64.StdEncoding.DecodeString(r.Data)
				fname := step.Value
				if !strings.HasSuffix(fname, ".png") {
					fname += ".png"
				}
				os.MkdirAll("./screenshots", 0755)
				os.WriteFile("./screenshots/"+fname, imgData, 0644)
				ok("screenshot saved: ./screenshots/" + fname)
			} else {
				ok("screenshot captured")
			}

		// ── Scrape ────────────────────────────────────────────────────────
		case StepScrape:
			res, err := cdp.Send("Runtime.evaluate", map[string]interface{}{
				"expression": fmt.Sprintf(
					`(function(){ var el = document.querySelector(%q); return el ? el.innerText.trim() : null })()`,
					step.Selector,
				),
				"returnByValue": true,
			})
			if err != nil {
				stepErr = err
				break
			}
			var r struct {
				Result struct {
					Type  string      `json:"type"`
					Value interface{} `json:"value"`
				} `json:"result"`
			}
			if err := json.Unmarshal(res, &r); err != nil {
				stepErr = err
				break
			}
			text := fmt.Sprintf("%v", r.Result.Value)
			key := step.Selector
			if step.Value != "" {
				key = step.Value
			}
			data(key, text)

		// ── Eval ──────────────────────────────────────────────────────────
		case StepEval:
			res, err := cdp.Send("Runtime.evaluate", map[string]interface{}{
				"expression":    step.Value,
				"returnByValue": true,
			})
			if err != nil {
				stepErr = err
				break
			}
			var r struct {
				Result struct{ Value interface{} }
			}
			if json.Unmarshal(res, &r) == nil && r.Result.Value != nil {
				data("eval", fmt.Sprintf("%v", r.Result.Value))
			} else {
				ok("eval executed")
			}
		}

		if stepErr != nil {
			fail(fmt.Sprintf("step %d failed: %v", i+1, stepErr))
			result.Status = "error"
			return result
		}
	}

	result.Status = "done"
	ok(fmt.Sprintf("workflow complete — %d steps executed", len(wf.Steps)))
	return result
}

// elementCenter finds the screen coordinates of the center of a CSS element.
func elementCenter(cdp *CDPClient, selector string) (float64, float64, error) {
	res, err := cdp.Send("Runtime.evaluate", map[string]interface{}{
		"expression": fmt.Sprintf(`
			(function(){
				var el = document.querySelector(%q);
				if (!el) return null;
				var r = el.getBoundingClientRect();
				return {x: r.left + r.width/2, y: r.top + r.height/2};
			})()`, selector),
		"returnByValue": true,
	})
	if err != nil {
		return 0, 0, err
	}
	var r struct {
		Result struct {
			Value *struct {
				X float64 `json:"x"`
				Y float64 `json:"y"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(res, &r); err != nil || r.Result.Value == nil {
		return 0, 0, fmt.Errorf("element not found: %s", selector)
	}
	return r.Result.Value.X, r.Result.Value.Y, nil
}

// stepDesc returns a short human-readable description of a step.
func stepDesc(s Step) string {
	switch s.Kind {
	case StepNavigate:
		return s.Value
	case StepClick:
		return s.Selector
	case StepType:
		txt := s.Value
		if len(txt) > 20 {
			txt = txt[:20] + "…"
		}
		return fmt.Sprintf(`"%s" into %s`, txt, s.Selector)
	case StepScroll:
		return fmt.Sprintf("(%d, %d)", s.X, s.Y)
	case StepWait:
		return s.Value + "s"
	case StepWaitFor:
		return s.Selector
	case StepScreenshot:
		if s.Value != "" {
			return s.Value
		}
		return "capture"
	case StepScrape:
		if s.Value != "" {
			return s.Selector + " → " + s.Value
		}
		return s.Selector
	case StepEval:
		js := s.Value
		if len(js) > 30 {
			js = js[:30] + "…"
		}
		return js
	}
	return ""
}

// ─── Scheduler ────────────────────────────────────────────────────────────────
// Polls the workflow store every 10s and fires any workflow whose schedule is due.

type Scheduler struct {
	store  *Store
	engine *Engine
	runFn  func(wf *Workflow)
	ctx    context.Context
}

func newScheduler(store *Store, engine *Engine, runFn func(wf *Workflow)) *Scheduler {
	return &Scheduler{store: store, engine: engine, runFn: runFn}
}

func (sc *Scheduler) Run(ctx context.Context) {
	sc.ctx = ctx
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sc.tick()
		}
	}
}

func (sc *Scheduler) tick() {
	workflows, err := sc.store.List()
	if err != nil {
		return
	}
	now := time.Now()
	for i := range workflows {
		wf := &workflows[i]
		if !wf.Schedule.Enabled || wf.Schedule.IntervalSecs <= 0 {
			continue
		}
		// Check if NextRun has passed
		if wf.Schedule.NextRun == "" {
			// First run — schedule it
			next := now.Add(time.Duration(wf.Schedule.IntervalSecs) * time.Second)
			wf.Schedule.NextRun = next.Format(time.RFC3339)
			sc.store.Save(wf)
			continue
		}
		nextRun, err := time.Parse(time.RFC3339, wf.Schedule.NextRun)
		if err != nil || now.Before(nextRun) {
			continue
		}
		// Due — run it and update NextRun
		next := now.Add(time.Duration(wf.Schedule.IntervalSecs) * time.Second)
		wf.Schedule.NextRun = next.Format(time.RFC3339)
		sc.store.Save(wf)
		go sc.runFn(wf)
	}
}

// ─── WebSocket hub (reused from sysmon pattern) ───────────────────────────────

type hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func newHub() *hub {
	return &hub{clients: make(map[chan []byte]struct{})}
}

func (h *hub) add(ch chan []byte) {
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	close(ch)
	h.mu.Unlock()
}

func (h *hub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (h *hub) send(data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		return
	}
	h.broadcast(b)
}

// ─── HTTP server ──────────────────────────────────────────────────────────────

type Server struct {
	store     *Store
	engine    *Engine
	hub       *hub
	scheduler *Scheduler
	runCtx    context.Context
	runCancel context.CancelFunc
	mu        sync.Mutex
	running   map[string]context.CancelFunc // workflowID → cancel
}

func newServer(store *Store, engine *Engine, h *hub, sched *Scheduler) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		store:     store,
		engine:    engine,
		hub:       h,
		scheduler: sched,
		runCtx:    ctx,
		runCancel: cancel,
		running:   make(map[string]context.CancelFunc),
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, htmlUI)
	})
	mux.HandleFunc("/ws", s.wsHandler)
	mux.HandleFunc("/api/workflows", s.apiWorkflows)
	mux.HandleFunc("/api/workflows/", s.apiWorkflow)
	mux.HandleFunc("/api/run/", s.apiRun)
	mux.HandleFunc("/api/stop/", s.apiStop)
	return mux
}

// apiWorkflows handles GET (list) and POST (create).
func (s *Server) apiWorkflows(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	switch r.Method {
	case http.MethodGet:
		workflows, _ := s.store.List()
		if workflows == nil {
			workflows = []Workflow{}
		}
		json.NewEncoder(w).Encode(workflows)

	case http.MethodPost:
		var wf Workflow
		if err := json.NewDecoder(r.Body).Decode(&wf); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if wf.ID == "" {
			wf.ID = fmt.Sprintf("wf_%d", time.Now().UnixNano())
		}
		if wf.CreatedAt == "" {
			wf.CreatedAt = time.Now().Format(time.RFC3339)
		}
		if err := s.store.Save(&wf); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(wf)

	default:
		http.Error(w, "method not allowed", 405)
	}
}

// apiWorkflow handles GET, PUT, DELETE for a single workflow.
func (s *Server) apiWorkflow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	id := strings.TrimPrefix(r.URL.Path, "/api/workflows/")

	switch r.Method {
	case http.MethodGet:
		wf, err := s.store.Get(id)
		if err != nil {
			http.Error(w, "not found", 404)
			return
		}
		json.NewEncoder(w).Encode(wf)

	case http.MethodPut:
		var wf Workflow
		if err := json.NewDecoder(r.Body).Decode(&wf); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		wf.ID = id
		if err := s.store.Save(&wf); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(wf)

	case http.MethodDelete:
		if err := s.store.Delete(id); err != nil {
			http.Error(w, "not found", 404)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})

	default:
		http.Error(w, "method not allowed", 405)
	}
}

// apiRun triggers execution of a workflow.
func (s *Server) apiRun(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/run/")
	wf, err := s.store.Get(id)
	if err != nil {
		http.Error(w, "workflow not found", 404)
		return
	}
	go s.execWorkflow(wf)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

// apiStop cancels a running workflow.
func (s *Server) apiStop(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/stop/")
	s.mu.Lock()
	cancel, ok := s.running[id]
	s.mu.Unlock()
	if ok {
		cancel()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"stopped": ok})
}

// execWorkflow runs a workflow and streams logs to all connected UIs.
func (s *Server) execWorkflow(wf *Workflow) {
	ctx, cancel := context.WithCancel(s.runCtx)
	s.mu.Lock()
	s.running[wf.ID] = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.running, wf.ID)
		s.mu.Unlock()
		cancel()
	}()

	// Announce start
	s.hub.send(WSMessage{Type: "run_start", Payload: map[string]string{
		"workflowId": wf.ID,
		"name":       wf.Name,
	}})

	var logs []RunLog
	result := s.engine.Run(ctx, wf, func(entry RunLog) {
		logs = append(logs, entry)
		// Stream each log line immediately
		s.hub.send(WSMessage{Type: "run_log", Payload: map[string]interface{}{
			"workflowId": wf.ID,
			"log":        entry,
		}})
	})
	result.Logs = logs

	s.hub.send(WSMessage{Type: "run_result", Payload: result})
}

// wsHandler handles WebSocket connections for the UI.
func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrade(w, r)
	if err != nil {
		return
	}

	ch := make(chan []byte, 16)
	s.hub.add(ch)
	defer s.hub.remove(ch)

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := bufio.NewReader(conn)
		for {
			_, err := wsReadFrameFromBuf(buf)
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := wsSendRaw(conn, msg); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

// ─── WebSocket server-side helpers (reused pattern from sysmon) ───────────────

func wsUpgrade(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	io.WriteString(h, key+magic)
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("hijack not supported")
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
	buf.WriteString(resp)
	buf.Flush()
	return conn, nil
}

func wsSendRaw(conn net.Conn, data []byte) error {
	l := len(data)
	var hdr []byte
	hdr = append(hdr, 0x81)
	switch {
	case l <= 125:
		hdr = append(hdr, byte(l))
	case l <= 65535:
		hdr = append(hdr, 126)
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(l))
		hdr = append(hdr, b...)
	default:
		hdr = append(hdr, 127)
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(l))
		hdr = append(hdr, b...)
	}
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := conn.Write(append(hdr, data...))
	return err
}

// ─── Port conflict resolution (from sysmon) ───────────────────────────────────

func portOwner(port string) (int, string) {
	portNum, _ := strconv.Atoi(port)
	hexPort := fmt.Sprintf("%04X", portNum)
	inodes := map[string]struct{}{}

	for _, f := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, _ := os.ReadFile(f)
		for i, line := range strings.Split(string(data), "\n") {
			if i == 0 {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 {
				continue
			}
			parts := strings.SplitN(fields[1], ":", 2)
			if len(parts) == 2 && strings.ToUpper(parts[1]) == hexPort && fields[3] == "0A" {
				inodes[fields[9]] = struct{}{}
			}
		}
	}
	if len(inodes) == 0 {
		return 0, ""
	}
	for _, fdDir := range func() []string { g, _ := filepath.Glob("/proc/[0-9]*/fd"); return g }() {
		parts := strings.Split(fdDir, "/")
		if len(parts) < 3 {
			continue
		}
		p, _ := strconv.Atoi(parts[2])
		links, _ := filepath.Glob(fdDir + "/*")
		for _, link := range links {
			target, err := os.Readlink(link)
			if err != nil {
				continue
			}
			if !strings.HasPrefix(target, "socket:[") {
				continue
			}
			inode := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
			if _, ok := inodes[inode]; ok {
				nameB, _ := os.ReadFile("/proc/" + parts[2] + "/comm")
				return p, strings.TrimSpace(string(nameB))
			}
		}
	}
	return 0, ""
}

func resolvePort(port string) string {
	reader := bufio.NewReader(os.Stdin)
	for {
		pid, name := portOwner(port)
		if pid == 0 {
			return port
		}
		fmt.Println()
		fmt.Println("  ┌──────────────────────────────────────────────────────┐")
		fmt.Printf("  │  ⚠  port %s is already in use                      │\n", port)
		fmt.Println("  ├──────────────────────────────────────────────────────┤")
		fmt.Printf("  │  PID  : %-43d│\n", pid)
		fmt.Printf("  │  name : %-43s│\n", name)
		fmt.Println("  ├──────────────────────────────────────────────────────┤")
		fmt.Println("  │  [k] kill that process and use this port             │")
		fmt.Println("  │  [p] enter a different port                          │")
		fmt.Println("  │  [q] quit                                            │")
		fmt.Println("  └──────────────────────────────────────────────────────┘")
		fmt.Print("  choice: ")
		line, _ := reader.ReadString('\n')
		switch strings.TrimSpace(strings.ToLower(line)) {
		case "k":
			proc, _ := os.FindProcess(pid)
			proc.Kill()
			fmt.Print("  waiting")
			for i := 0; i < 15; i++ {
				time.Sleep(200 * time.Millisecond)
				fmt.Print(".")
				if p, _ := portOwner(port); p == 0 {
					fmt.Println(" freed!")
					return port
				}
			}
			fmt.Println("\n  still busy, try a different port")
		case "p":
			fmt.Print("  new port: ")
			np, _ := reader.ReadString('\n')
			np = strings.TrimSpace(np)
			if _, err := strconv.Atoi(np); err == nil && np != "" {
				port = np
			}
		case "q":
			fmt.Println("  bye.")
			os.Exit(0)
		}
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	port := "8888"
	if p := os.Getenv("WEBBOT_PORT"); p != "" {
		port = p
	}
	port = resolvePort(port)

	ctx := context.Background()
	store := newStore()
	chrome := newChromeManager()
	engine := newEngine(chrome)
	h := newHub()

	srv := newServer(store, engine, h, nil)

	// Wire scheduler so it uses the same execWorkflow path
	sched := newScheduler(store, engine, func(wf *Workflow) {
		srv.execWorkflow(wf)
	})
	srv.scheduler = sched
	go sched.Run(ctx)

	addr := ":" + port
	fmt.Printf("\n  ✓ webbot running → http://localhost%s\n", addr)
	fmt.Println("  workflows saved to ./workflows/")
	fmt.Println("  screenshots saved to ./screenshots/")
	fmt.Println("  press Ctrl+C to stop\n")

	if err := http.ListenAndServe(addr, srv.routes()); err != nil {
		log.Fatal(err)
	}
}

// Compile-time check that bytes is used (suppress import errors during build iterations)
var _ = bytes.NewBuffer

// ─── Embedded UI ──────────────────────────────────────────────────────────────

const htmlUI = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>webbot</title>
<style>
:root {
  --bg: #0c0c0f;
  --s1: #13131a;
  --s2: #1a1a24;
  --s3: #22222e;
  --border: #2e2e3e;
  --text: #e2e2f0;
  --muted: #666680;
  --accent: #6c63ff;
  --green: #3ecf8e;
  --amber: #f5a623;
  --red: #f56565;
  --blue: #4da6ff;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'SF Mono','Fira Code',monospace;font-size:13px;display:flex;height:100vh;overflow:hidden}

/* ── sidebar ── */
.sidebar{width:260px;min-width:260px;background:var(--s1);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.sidebar-header{padding:16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.logo{font-size:16px;font-weight:700}.logo span{color:var(--accent)}
.btn{padding:6px 12px;border-radius:6px;border:none;cursor:pointer;font-family:inherit;font-size:11px;font-weight:600;letter-spacing:.04em;transition:all .15s}
.btn-primary{background:var(--accent);color:#fff}
.btn-primary:hover{background:#7c74ff}
.btn-sm{padding:4px 8px;font-size:10px}
.btn-ghost{background:transparent;color:var(--muted);border:1px solid var(--border)}
.btn-ghost:hover{border-color:var(--accent);color:var(--accent)}
.btn-danger{background:rgba(245,101,101,.15);color:var(--red);border:1px solid transparent}
.btn-danger:hover{border-color:var(--red)}
.btn-green{background:rgba(62,207,142,.15);color:var(--green);border:1px solid transparent}
.btn-green:hover{border-color:var(--green)}

.wf-list{flex:1;overflow-y:auto;padding:8px}
.wf-item{padding:10px 12px;border-radius:8px;cursor:pointer;margin-bottom:4px;border:1px solid transparent;transition:all .15s}
.wf-item:hover{background:var(--s2);border-color:var(--border)}
.wf-item.active{background:var(--s2);border-color:var(--accent)}
.wf-item-name{font-weight:600;color:var(--text);margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wf-item-meta{font-size:10px;color:var(--muted);display:flex;gap:8px;align-items:center}
.sched-dot{width:5px;height:5px;border-radius:50%;background:var(--green);display:inline-block}

/* ── main area ── */
.main{flex:1;display:flex;flex-direction:column;overflow:hidden}
.toolbar{padding:12px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;background:var(--s1)}
.wf-name-input{background:transparent;border:none;color:var(--text);font-family:inherit;font-size:15px;font-weight:700;outline:none;flex:1;min-width:0}
.wf-name-input::placeholder{color:var(--muted)}

.content{flex:1;display:flex;overflow:hidden}

/* ── step builder ── */
.builder{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:12px}
.step-card{background:var(--s1);border:1px solid var(--border);border-radius:10px;overflow:hidden;transition:border-color .15s}
.step-card:hover{border-color:var(--s3)}
.step-header{display:flex;align-items:center;gap:10px;padding:10px 14px;cursor:pointer}
.step-drag{color:var(--muted);font-size:14px;cursor:grab;user-select:none}
.step-num{font-size:10px;color:var(--muted);min-width:20px}
.step-kind-badge{font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:3px 8px;border-radius:4px}
.step-summary{flex:1;color:var(--muted);font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.step-del{background:transparent;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:2px 6px;border-radius:4px}
.step-del:hover{background:rgba(245,101,101,.15);color:var(--red)}

.step-body{padding:12px 14px;border-top:1px solid var(--border);display:grid;grid-template-columns:1fr 1fr;gap:10px}
.step-body.single{grid-template-columns:1fr}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-size:10px;color:var(--muted);letter-spacing:.06em;text-transform:uppercase}
.field input,.field select,.field textarea{background:var(--s2);border:1px solid var(--border);color:var(--text);font-family:inherit;font-size:12px;padding:7px 10px;border-radius:6px;outline:none;transition:border-color .15s;width:100%}
.field input:focus,.field select,.field textarea:focus{border-color:var(--accent)}
.field textarea{resize:vertical;min-height:60px}
.field select option{background:var(--s2)}

.add-step-bar{padding:0 20px 20px}
.kind-picker{display:flex;flex-wrap:wrap;gap:6px}
.kind-btn{padding:5px 12px;border-radius:6px;border:1px solid var(--border);background:var(--s2);color:var(--muted);cursor:pointer;font-family:inherit;font-size:11px;transition:all .15s}
.kind-btn:hover{border-color:var(--accent);color:var(--accent)}

/* badge colours by kind */
.k-navigate{background:rgba(77,166,255,.15);color:var(--blue)}
.k-click{background:rgba(108,99,255,.15);color:var(--accent)}
.k-type{background:rgba(62,207,142,.15);color:var(--green)}
.k-scroll{background:rgba(245,166,35,.15);color:var(--amber)}
.k-wait,.k-waitFor{background:rgba(102,102,128,.2);color:var(--muted)}
.k-screenshot{background:rgba(255,150,100,.15);color:#ff9664}
.k-scrape{background:rgba(200,100,255,.15);color:#c864ff}
.k-eval{background:rgba(245,101,101,.15);color:var(--red)}

/* ── right panel: logs + schedule ── */
.right-panel{width:320px;min-width:280px;border-left:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.panel-tabs{display:flex;border-bottom:1px solid var(--border)}
.tab{flex:1;padding:10px;text-align:center;cursor:pointer;font-size:11px;color:var(--muted);border-bottom:2px solid transparent;transition:all .15s}
.tab.active{color:var(--text);border-bottom-color:var(--accent)}

/* logs */
.log-pane{flex:1;overflow-y:auto;padding:12px;font-size:11px;display:flex;flex-direction:column;gap:3px}
.log-entry{display:flex;gap:8px;padding:4px 0;border-bottom:1px solid rgba(46,46,62,.4)}
.log-time{color:var(--muted);min-width:56px;flex-shrink:0}
.log-msg{flex:1;word-break:break-word}
.log-info .log-msg{color:var(--text)}
.log-ok .log-msg{color:var(--green)}
.log-error .log-msg{color:var(--red)}
.log-data .log-msg{color:#c864ff}
.status-bar{padding:10px 12px;border-top:1px solid var(--border);display:flex;align-items:center;gap:8px;font-size:11px}
.status-dot{width:6px;height:6px;border-radius:50%;background:var(--muted);flex-shrink:0}
.status-dot.running{background:var(--amber);animation:pulse 1s ease-in-out infinite}
.status-dot.done{background:var(--green)}
.status-dot.error{background:var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

/* schedule pane */
.sched-pane{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:14px}
.sched-toggle{display:flex;align-items:center;justify-content:space-between}
.toggle{position:relative;width:36px;height:20px}
.toggle input{opacity:0;width:0;height:0}
.slider{position:absolute;inset:0;background:var(--border);border-radius:10px;cursor:pointer;transition:.3s}
.slider::before{content:'';position:absolute;width:14px;height:14px;left:3px;bottom:3px;background:white;border-radius:50%;transition:.3s}
input:checked+.slider{background:var(--accent)}
input:checked+.slider::before{transform:translateX(16px)}
.sched-interval{display:flex;flex-direction:column;gap:6px}

/* screenshot panel */
.screenshot-pane{padding:12px;border-top:1px solid var(--border)}
.screenshot-pane img{width:100%;border-radius:6px;border:1px solid var(--border)}

/* scrape results */
.scrape-pane{padding:12px;border-top:1px solid var(--border)}
.scrape-row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid rgba(46,46,62,.4);font-size:11px}
.scrape-key{color:var(--muted)}
.scrape-val{color:#c864ff;max-width:160px;overflow:hidden;text-overflow:ellipsis;text-align:right}

/* empty state */
.empty{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;color:var(--muted);gap:12px;text-align:center;padding:32px}
.empty-icon{font-size:40px;opacity:.4}

/* ws badge */
.ws-badge{font-size:9px;padding:2px 7px;border-radius:10px;background:rgba(245,101,101,.15);color:var(--red)}
.ws-badge.live{background:rgba(62,207,142,.12);color:var(--green)}
</style>
</head>
<body>

<!-- SIDEBAR: workflow list -->
<div class="sidebar">
  <div class="sidebar-header">
    <div class="logo">web<span>bot</span></div>
    <button class="btn btn-primary btn-sm" onclick="newWorkflow()">+ new</button>
  </div>
  <div class="wf-list" id="wf-list">
    <div class="empty" style="padding:32px 16px">
      <div class="empty-icon">⚡</div>
      <div>no workflows yet<br>click <b>+ new</b> to start</div>
    </div>
  </div>
  <div style="padding:10px 12px;border-top:1px solid var(--border);display:flex;align-items:center;gap:6px">
    <div class="status-dot" id="ws-dot"></div>
    <span class="ws-badge" id="ws-badge">disconnected</span>
  </div>
</div>

<!-- MAIN: step builder -->
<div class="main">
  <div class="toolbar" id="toolbar" style="display:none">
    <input class="wf-name-input" id="wf-name" placeholder="workflow name…" oninput="markDirty()">
    <button class="btn btn-ghost btn-sm" onclick="saveWorkflow()">💾 save</button>
    <button class="btn btn-green btn-sm" id="run-btn" onclick="runWorkflow()">▶ run</button>
    <button class="btn btn-danger btn-sm" id="stop-btn" style="display:none" onclick="stopWorkflow()">■ stop</button>
    <button class="btn btn-danger btn-sm" onclick="deleteWorkflow()">🗑</button>
  </div>

  <div class="content">
    <div style="flex:1;display:flex;flex-direction:column;overflow:hidden" id="builder-wrap">
      <!-- empty state when no workflow selected -->
      <div class="empty" id="no-wf">
        <div class="empty-icon">🤖</div>
        <div>select or create a workflow<br>to start building</div>
      </div>

      <!-- step list -->
      <div class="builder" id="step-list" style="display:none"></div>

      <!-- add step bar -->
      <div class="add-step-bar" id="add-step-bar" style="display:none">
        <div style="font-size:10px;color:var(--muted);letter-spacing:.08em;text-transform:uppercase;margin-bottom:8px">add step</div>
        <div class="kind-picker">
          <button class="kind-btn" onclick="addStep('navigate')">🌐 navigate</button>
          <button class="kind-btn" onclick="addStep('click')">👆 click</button>
          <button class="kind-btn" onclick="addStep('type')">⌨️ type</button>
          <button class="kind-btn" onclick="addStep('scroll')">↕️ scroll</button>
          <button class="kind-btn" onclick="addStep('wait')">⏱ wait</button>
          <button class="kind-btn" onclick="addStep('waitFor')">👁 wait for</button>
          <button class="kind-btn" onclick="addStep('screenshot')">📸 screenshot</button>
          <button class="kind-btn" onclick="addStep('scrape')">🔍 scrape</button>
          <button class="kind-btn" onclick="addStep('eval')">⚙️ eval JS</button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- RIGHT PANEL: logs + schedule -->
<div class="right-panel">
  <div class="panel-tabs">
    <div class="tab active" id="tab-logs" onclick="switchTab('logs')">logs</div>
    <div class="tab" id="tab-sched" onclick="switchTab('sched')">schedule</div>
    <div class="tab" id="tab-data" onclick="switchTab('data')">results</div>
  </div>

  <!-- logs tab -->
  <div id="pane-logs" style="flex:1;display:flex;flex-direction:column;overflow:hidden">
    <div class="log-pane" id="log-pane">
      <div style="color:var(--muted);text-align:center;padding:20px">no logs yet — run a workflow</div>
    </div>
    <div class="status-bar">
      <div class="status-dot" id="run-dot"></div>
      <span id="run-status">idle</span>
      <span style="margin-left:auto;color:var(--muted);font-size:10px" id="run-time"></span>
    </div>
  </div>

  <!-- schedule tab -->
  <div id="pane-sched" style="flex:1;overflow:hidden;display:none">
    <div class="sched-pane">
      <div class="sched-toggle">
        <span style="color:var(--text)">Enable schedule</span>
        <label class="toggle">
          <input type="checkbox" id="sched-enabled" onchange="markDirty()">
          <span class="slider"></span>
        </label>
      </div>
      <div class="sched-interval">
        <div class="field">
          <label>interval (seconds)</label>
          <input type="number" id="sched-interval" min="10" placeholder="e.g. 3600 for every hour" oninput="markDirty()">
        </div>
        <div style="font-size:10px;color:var(--muted);line-height:1.6">
          common intervals:<br>
          60 = every minute &nbsp;|&nbsp; 3600 = hourly<br>
          86400 = daily
        </div>
      </div>
      <div class="field">
        <label>next scheduled run</label>
        <div style="color:var(--muted);font-size:11px" id="next-run-display">—</div>
      </div>
      <button class="btn btn-primary" style="width:100%" onclick="saveWorkflow()">save schedule</button>
    </div>
  </div>

  <!-- results tab -->
  <div id="pane-data" style="flex:1;overflow-y:auto;display:none">
    <div id="screenshot-wrap" style="display:none" class="screenshot-pane">
      <div style="font-size:10px;color:var(--muted);margin-bottom:6px;letter-spacing:.06em;text-transform:uppercase">last screenshot</div>
      <img id="screenshot-img" src="">
    </div>
    <div id="scrape-wrap" style="display:none" class="scrape-pane">
      <div style="font-size:10px;color:var(--muted);margin-bottom:6px;letter-spacing:.06em;text-transform:uppercase">scraped data</div>
      <div id="scrape-rows"></div>
    </div>
    <div id="no-results" style="color:var(--muted);text-align:center;padding:32px;font-size:11px">
      run a workflow to see results here
    </div>
  </div>
</div>

<script>
// ── state ──────────────────────────────────────────────────────────────────
let workflows = [];
let current = null;  // active workflow object (deep copy)
let dirty = false;
let currentRunId = null;
let ws = null;
let runStart = null;
let runTimer = null;

// ── WebSocket ──────────────────────────────────────────────────────────────
function connectWS() {
  ws = new WebSocket('ws://' + location.host + '/ws');
  ws.onopen = () => {
    document.getElementById('ws-dot').classList.add('done');
    const badge = document.getElementById('ws-badge');
    badge.textContent = 'live'; badge.className = 'ws-badge live';
  };
  ws.onclose = () => {
    document.getElementById('ws-dot').className = 'status-dot';
    const badge = document.getElementById('ws-badge');
    badge.textContent = 'disconnected'; badge.className = 'ws-badge';
    setTimeout(connectWS, 2000);
  };
  ws.onmessage = e => {
    try { handleWS(JSON.parse(e.data)); } catch(err) { console.error(err); }
  };
}

function handleWS(msg) {
  switch (msg.type) {
    case 'run_start':
      currentRunId = msg.payload.workflowId;
      setStatus('running', 'running ' + msg.payload.name + '…');
      clearLogs();
      runStart = Date.now();
      runTimer = setInterval(() => {
        const s = ((Date.now() - runStart) / 1000).toFixed(1);
        document.getElementById('run-time').textContent = s + 's';
      }, 100);
      document.getElementById('run-btn').style.display = 'none';
      document.getElementById('stop-btn').style.display = '';
      break;

    case 'run_log':
      appendLog(msg.payload.log);
      break;

    case 'run_result':
      clearInterval(runTimer);
      document.getElementById('run-btn').style.display = '';
      document.getElementById('stop-btn').style.display = 'none';
      const r = msg.payload;
      setStatus(r.status, r.status === 'done' ? '✓ done' : '✗ error');
      if (r.screenshot) {
        document.getElementById('screenshot-img').src = 'data:image/png;base64,' + r.screenshot;
        document.getElementById('screenshot-wrap').style.display = '';
        document.getElementById('no-results').style.display = 'none';
      }
      if (r.scraped && Object.keys(r.scraped).length > 0) {
        const rows = Object.entries(r.scraped)
          .map(([k,v]) => '<div class="scrape-row"><span class="scrape-key">'+k+'</span><span class="scrape-val" title="'+v+'">'+v+'</span></div>')
          .join('');
        document.getElementById('scrape-rows').innerHTML = rows;
        document.getElementById('scrape-wrap').style.display = '';
        document.getElementById('no-results').style.display = 'none';
        switchTab('data');
      }
      break;
  }
}

// ── log helpers ────────────────────────────────────────────────────────────
function clearLogs() {
  document.getElementById('log-pane').innerHTML = '';
  document.getElementById('run-time').textContent = '';
}

function appendLog(entry) {
  const pane = document.getElementById('log-pane');
  const div = document.createElement('div');
  div.className = 'log-entry log-' + entry.level;
  div.innerHTML = '<span class="log-time">' + entry.time + '</span>' +
                  '<span class="log-msg">' + entry.message + '</span>';
  pane.appendChild(div);
  pane.scrollTop = pane.scrollHeight;
}

function setStatus(state, msg) {
  const dot = document.getElementById('run-dot');
  dot.className = 'status-dot ' + state;
  document.getElementById('run-status').textContent = msg;
}

// ── tab switching ──────────────────────────────────────────────────────────
function switchTab(name) {
  ['logs','sched','data'].forEach(t => {
    document.getElementById('tab-' + t).className = 'tab' + (t === name ? ' active' : '');
    document.getElementById('pane-' + t).style.display = t === name ? (t === 'logs' ? 'flex' : 'block') : 'none';
  });
  if (name === 'sched') renderSched();
}

// ── workflow list ──────────────────────────────────────────────────────────
async function loadWorkflows() {
  const res = await fetch('/api/workflows');
  workflows = await res.json() || [];
  renderList();
}

function renderList() {
  const el = document.getElementById('wf-list');
  if (!workflows.length) {
    el.innerHTML = '<div class="empty" style="padding:32px 16px"><div class="empty-icon">⚡</div><div>no workflows yet<br>click <b>+ new</b> to start</div></div>';
    return;
  }
  el.innerHTML = workflows.map(wf => {
    const steps = wf.steps ? wf.steps.length : 0;
    const sched = wf.schedule && wf.schedule.enabled;
    const isActive = current && current.id === wf.id;
    return '<div class="wf-item' + (isActive ? ' active' : '') + '" onclick="selectWorkflow(\'' + wf.id + '\')">' +
      '<div class="wf-item-name">' + (wf.name || 'untitled') + '</div>' +
      '<div class="wf-item-meta">' +
        '<span>' + steps + ' steps</span>' +
        (sched ? '<span class="sched-dot"></span><span>' + (wf.schedule.intervalSecs/60).toFixed(0) + 'm</span>' : '') +
      '</div>' +
    '</div>';
  }).join('');
}

function selectWorkflow(id) {
  if (dirty && !confirm('You have unsaved changes. Continue?')) return;
  const wf = workflows.find(w => w.id === id);
  if (!wf) return;
  current = JSON.parse(JSON.stringify(wf));
  dirty = false;
  renderEditor();
  renderList();
}

// ── editor ─────────────────────────────────────────────────────────────────
function renderEditor() {
  if (!current) {
    document.getElementById('no-wf').style.display = 'flex';
    document.getElementById('step-list').style.display = 'none';
    document.getElementById('add-step-bar').style.display = 'none';
    document.getElementById('toolbar').style.display = 'none';
    return;
  }
  document.getElementById('no-wf').style.display = 'none';
  document.getElementById('step-list').style.display = 'flex';
  document.getElementById('add-step-bar').style.display = '';
  document.getElementById('toolbar').style.display = 'flex';
  document.getElementById('wf-name').value = current.name || '';
  renderSteps();
  renderSched();
}

function renderSteps() {
  const list = document.getElementById('step-list');
  if (!current.steps || !current.steps.length) {
    list.innerHTML = '<div class="empty"><div class="empty-icon">➕</div><div>no steps yet<br>add one below</div></div>';
    return;
  }
  list.innerHTML = current.steps.map((step, i) => stepCard(step, i)).join('');
}

const kindColors = {
  navigate:'k-navigate', click:'k-click', type:'k-type', scroll:'k-scroll',
  wait:'k-wait', waitFor:'k-waitFor', screenshot:'k-screenshot', scrape:'k-scrape', eval:'k-eval'
};

const kindIcons = {
  navigate:'🌐', click:'👆', type:'⌨️', scroll:'↕️',
  wait:'⏱', waitFor:'👁', screenshot:'📸', scrape:'🔍', eval:'⚙️'
};

function stepSummary(step) {
  switch(step.kind) {
    case 'navigate': return step.value || '—';
    case 'click': return step.selector || '—';
    case 'type': return '"' + (step.value || '').slice(0,24) + '" → ' + (step.selector || '—');
    case 'scroll': return '(' + (step.x||0) + ', ' + (step.y||0) + ')';
    case 'wait': return (step.value||1) + 's';
    case 'waitFor': return step.selector || '—';
    case 'screenshot': return step.value || 'capture';
    case 'scrape': return (step.selector||'—') + (step.value ? ' → '+step.value : '');
    case 'eval': return (step.value||'').slice(0,30) + '…';
    default: return '';
  }
}

function stepCard(step, i) {
  const color = kindColors[step.kind] || '';
  const icon = kindIcons[step.kind] || '•';

  let fields = '';
  switch(step.kind) {
    case 'navigate':
      fields = field('url', 'value', step.value, 'https://example.com', i, 'single'); break;
    case 'click':
    case 'waitFor':
      fields = field('css selector', 'selector', step.selector, '#btn-submit, .item, a[href]', i, 'single'); break;
    case 'type':
      fields = field('css selector', 'selector', step.selector, 'input[name="q"]', i) +
               field('text to type', 'value', step.value, 'search query…', i); break;
    case 'scroll':
      fields = field('scroll X (px)', 'x', step.x||0, '0', i, '', 'number') +
               field('scroll Y (px)', 'y', step.y||0, '500', i, '', 'number'); break;
    case 'wait':
      fields = field('seconds', 'value', step.value, '2', i, 'single', 'number'); break;
    case 'screenshot':
      fields = field('filename (optional)', 'value', step.value, 'screenshot-1', i, 'single'); break;
    case 'scrape':
      fields = field('css selector', 'selector', step.selector, '.price, #title', i) +
               field('save as (key)', 'value', step.value, 'price', i); break;
    case 'eval':
      fields = '<div class="field" style="grid-column:1/-1"><label>javascript</label>' +
               '<textarea onchange="updateStep(' + i + ',\'value\',this.value)" rows="3" placeholder="document.title">' + (step.value||'') + '</textarea></div>'; break;
  }

  const bodyClass = ['scroll','type','scrape'].includes(step.kind) ? 'step-body' : 'step-body single';

  return '<div class="step-card" id="step-'+i+'">' +
    '<div class="step-header">' +
      '<span class="step-drag">⠿</span>' +
      '<span class="step-num">' + (i+1) + '</span>' +
      '<span class="step-kind-badge ' + color + '">' + icon + ' ' + step.kind + '</span>' +
      '<span class="step-summary">' + stepSummary(step) + '</span>' +
      '<button class="step-del" onclick="removeStep('+i+')">✕</button>' +
    '</div>' +
    '<div class="' + bodyClass + '">' + fields + '</div>' +
  '</div>';
}

function field(label, fieldName, value, placeholder, stepIdx, extra, type) {
  const tag = fieldName === 'value' && extra === 'single' ?
    '<input type="' + (type||'text') + '" value="' + esc(value) + '" placeholder="' + placeholder + '" oninput="updateStep(' + stepIdx + ',\'' + fieldName + '\',this.value)">' :
    '<input type="' + (type||'text') + '" value="' + esc(value) + '" placeholder="' + placeholder + '" oninput="updateStep(' + stepIdx + ',\'' + fieldName + '\',this.value)">';
  return '<div class="field"><label>' + label + '</label>' + tag + '</div>';
}

function esc(v) {
  if (v === undefined || v === null) return '';
  return String(v).replace(/"/g, '&quot;');
}

// ── step mutations ─────────────────────────────────────────────────────────
function addStep(kind) {
  if (!current) return;
  if (!current.steps) current.steps = [];
  const step = {
    id: 's_' + Date.now(),
    kind: kind,
    selector: '', value: '', x: 0, y: 0
  };
  current.steps.push(step);
  markDirty();
  renderSteps();
  document.getElementById('step-list').scrollTop = 999999;
}

function removeStep(i) {
  current.steps.splice(i, 1);
  markDirty();
  renderSteps();
}

function updateStep(i, field, value) {
  if (!current || !current.steps[i]) return;
  if (field === 'x' || field === 'y') {
    current.steps[i][field] = parseInt(value) || 0;
  } else {
    current.steps[i][field] = value;
  }
  markDirty();
  // Update just the summary without full re-render
  const card = document.getElementById('step-' + i);
  if (card) {
    const sumEl = card.querySelector('.step-summary');
    if (sumEl) sumEl.textContent = stepSummary(current.steps[i]);
  }
}

// ── workflow CRUD ──────────────────────────────────────────────────────────
function newWorkflow() {
  if (dirty && !confirm('Unsaved changes. Continue?')) return;
  current = {
    id: 'wf_' + Date.now(),
    name: '',
    description: '',
    steps: [],
    schedule: { enabled: false, intervalSecs: 3600 },
    createdAt: new Date().toISOString()
  };
  dirty = true;
  renderEditor();
}

async function saveWorkflow() {
  if (!current) return;
  current.name = document.getElementById('wf-name').value || 'untitled';
  current.schedule = {
    enabled: document.getElementById('sched-enabled').checked,
    intervalSecs: parseInt(document.getElementById('sched-interval').value) || 3600,
    nextRun: current.schedule ? current.schedule.nextRun : ''
  };

  const existing = workflows.find(w => w.id === current.id);
  const method = existing ? 'PUT' : 'POST';
  const url = existing ? '/api/workflows/' + current.id : '/api/workflows';

  const res = await fetch(url, {
    method, headers: {'Content-Type':'application/json'},
    body: JSON.stringify(current)
  });
  const saved = await res.json();
  current = saved;
  dirty = false;

  await loadWorkflows();
  renderList();
  alert('✓ saved!');
}

async function deleteWorkflow() {
  if (!current || !confirm('Delete "' + (current.name||'untitled') + '"?')) return;
  await fetch('/api/workflows/' + current.id, { method: 'DELETE' });
  current = null;
  dirty = false;
  await loadWorkflows();
  renderEditor();
}

// ── run / stop ─────────────────────────────────────────────────────────────
async function runWorkflow() {
  if (!current) return;
  if (dirty) {
    if (!confirm('You have unsaved changes. Save and run?')) return;
    await saveWorkflow();
  }
  switchTab('logs');
  clearLogs();
  await fetch('/api/run/' + current.id, { method: 'POST' });
}

async function stopWorkflow() {
  if (!current) return;
  await fetch('/api/stop/' + current.id, { method: 'POST' });
}

// ── schedule UI ────────────────────────────────────────────────────────────
function renderSched() {
  if (!current) return;
  const s = current.schedule || {};
  document.getElementById('sched-enabled').checked = !!s.enabled;
  document.getElementById('sched-interval').value = s.intervalSecs || 3600;
  document.getElementById('next-run-display').textContent = s.nextRun
    ? new Date(s.nextRun).toLocaleString() : '—';
}

function markDirty() { dirty = true; }

// ── init ───────────────────────────────────────────────────────────────────
connectWS();
loadWorkflows();
</script>
</body>
</html>`

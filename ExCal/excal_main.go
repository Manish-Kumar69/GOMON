package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// =============================================
// Excalidraw-like Whiteboard MVP in Go
// =============================================
// Features:
// ✅ Realtime collaboration using WebSockets
// ✅ Freehand drawing
// ✅ Rectangle tool
// ✅ Circle tool
// ✅ Element synchronization
// ✅ Canvas rendering
// ✅ Multi-user broadcast
//
// Stack:
// Backend  : Go + Gorilla WebSocket
// Frontend : HTML5 Canvas + Vanilla JS
// =============================================

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Element struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Color     string      `json:"color"`
	LineWidth int         `json:"lineWidth"`
	Points    [][]float64 `json:"points,omitempty"`
	X         float64     `json:"x,omitempty"`
	Y         float64     `json:"y,omitempty"`
	Width     float64     `json:"width,omitempty"`
	Height    float64     `json:"height,omitempty"`
	Radius    float64     `json:"radius,omitempty"`
}

type Message struct {
	Type    string    `json:"type"`
	Element *Element  `json:"element,omitempty"`
	Board   []Element `json:"board,omitempty"`
}

var (
	clients   = make(map[*websocket.Conn]bool)
	board     []Element
	boardLock sync.Mutex
)

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	clients[conn] = true

	boardLock.Lock()
	initial := Message{
		Type:  "init",
		Board: board,
	}
	boardLock.Unlock()

	conn.WriteJSON(initial)

	for {
		var msg Message

		err := conn.ReadJSON(&msg)
		if err != nil {
			delete(clients, conn)
			conn.Close()
			break
		}

		switch msg.Type {
		case "draw":
			if msg.Element != nil {
				boardLock.Lock()
				board = append(board, *msg.Element)
				boardLock.Unlock()

				broadcast(msg)
			}

		case "clear":
			boardLock.Lock()
			board = []Element{}
			boardLock.Unlock()

			broadcast(msg)
		}
	}
}

func broadcast(msg Message) {
	for client := range clients {
		err := client.WriteJSON(msg)
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, indexHTML)
}

func main() {
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", handleWS)

	fmt.Println("🚀 Whiteboard running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

var indexHTML = `
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<title>Go Whiteboard</title>
<style>
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    overflow: hidden;
    font-family: Arial;
    background: #1e1e1e;
}

.toolbar {
    position: fixed;
    top: 10px;
    left: 10px;
    z-index: 999;
    background: white;
    padding: 10px;
    border-radius: 10px;
    display: flex;
    gap: 10px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.2);
}

button {
    padding: 8px 14px;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    background: #f1f1f1;
}

button.active {
    background: #007bff;
    color: white;
}

canvas {
    background: #ffffff;
}
</style>
</head>
<body>

<div class="toolbar">
    <button onclick="setTool('pencil')" id="pencilBtn" class="active">✏️ Pencil</button>
    <button onclick="setTool('rect')">⬛ Rectangle</button>
    <button onclick="setTool('circle')">⚪ Circle</button>
    <button onclick="clearBoard()">🗑 Clear</button>
</div>

<canvas id="canvas"></canvas>

<script>
const canvas = document.getElementById('canvas');
const ctx = canvas.getContext('2d');

canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

const ws = new WebSocket('ws://' + location.host + '/ws');

let tool = 'pencil';
let drawing = false;
let startX = 0;
let startY = 0;
let currentPoints = [];
let elements = [];

function setTool(t) {
    tool = t;

    document.querySelectorAll('button').forEach(btn => {
        btn.classList.remove('active');
    });

    if (tool === 'pencil') {
        document.getElementById('pencilBtn').classList.add('active');
    }
}

function generateId() {
    return Math.random().toString(36).substring(2);
}

function drawElement(el) {
    ctx.strokeStyle = el.color || '#000';
    ctx.lineWidth = el.lineWidth || 2;

    if (el.type === 'pencil') {
        ctx.beginPath();

        el.points.forEach((p, index) => {
            if (index === 0) {
                ctx.moveTo(p[0], p[1]);
            } else {
                ctx.lineTo(p[0], p[1]);
            }
        });

        ctx.stroke();
    }

    if (el.type === 'rect') {
        ctx.strokeRect(el.x, el.y, el.width, el.height);
    }

    if (el.type === 'circle') {
        ctx.beginPath();
        ctx.arc(el.x, el.y, el.radius, 0, Math.PI * 2);
        ctx.stroke();
    }
}

function redraw() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    elements.forEach(el => drawElement(el));
}

canvas.addEventListener('mousedown', (e) => {
    drawing = true;

    startX = e.clientX;
    startY = e.clientY;

    currentPoints = [[startX, startY]];
});

canvas.addEventListener('mousemove', (e) => {
    if (!drawing) return;

    if (tool === 'pencil') {
        currentPoints.push([e.clientX, e.clientY]);

        redraw();

        drawElement({
            type: 'pencil',
            points: currentPoints,
            color: '#000',
            lineWidth: 2
        });
    }
});

canvas.addEventListener('mouseup', (e) => {
    if (!drawing) return;

    drawing = false;

    let element;

    if (tool === 'pencil') {
        element = {
            id: generateId(),
            type: 'pencil',
            points: currentPoints,
            color: '#000',
            lineWidth: 2
        };
    }

    if (tool === 'rect') {
        element = {
            id: generateId(),
            type: 'rect',
            x: startX,
            y: startY,
            width: e.clientX - startX,
            height: e.clientY - startY,
            color: '#000',
            lineWidth: 2
        };
    }

    if (tool === 'circle') {
        const dx = e.clientX - startX;
        const dy = e.clientY - startY;

        element = {
            id: generateId(),
            type: 'circle',
            x: startX,
            y: startY,
            radius: Math.sqrt(dx * dx + dy * dy),
            color: '#000',
            lineWidth: 2
        };
    }

    elements.push(element);
    redraw();

    ws.send(JSON.stringify({
        type: 'draw',
        element
    }));
});

function clearBoard() {
    elements = [];
    redraw();

    ws.send(JSON.stringify({
        type: 'clear'
    }));
}

ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);

    if (msg.type === 'init') {
        elements = msg.board || [];
        redraw();
    }

    if (msg.type === 'draw') {
        elements.push(msg.element);
        redraw();
    }

    if (msg.type === 'clear') {
        elements = [];
        redraw();
    }
};

window.addEventListener('resize', () => {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    redraw();
});
</script>

</body>
</html>
`

func init() {
	_, _ = json.Marshal(time.Now())
}

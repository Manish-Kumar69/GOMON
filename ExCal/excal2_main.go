package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, htmlContent)
	})
	fmt.Println("🚀 ExcaliGo Ultra is running at http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}

const htmlContent = `<!DOCTYPE html>
<html>
<head>
    <title>ExcaliGo Ultra</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        :root { --bg: #f9f9f9; --text: #2c3e50; --accent: #6965db; }
        body { margin: 0; overflow: hidden; font-family: 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); }
        
        #toolbar { 
            position: absolute; top: 15px; left: 50%; transform: translateX(-50%);
            background: white; padding: 10px; border-radius: 12px; 
            box-shadow: 0 8px 30px rgba(0,0,0,0.12); display: flex; gap: 12px; align-items: center; z-index: 100;
        }

        .tool-group { display: flex; gap: 5px; border-right: 1px solid #eee; padding-right: 10px; }
        .tool-group:last-child { border-right: none; }

        button { 
            width: 40px; height: 40px; border: none; border-radius: 8px; 
            cursor: pointer; background: transparent; font-size: 1.1rem;
            transition: all 0.2s; display: flex; align-items: center; justify-content: center;
        }
        button:hover { background: #f0f0f0; }
        button.active { background: var(--accent); color: white; }
        
        #icon-panel {
            position: absolute; top: 80px; left: 20px; background: white;
            padding: 15px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            display: none; grid-template-columns: repeat(3, 1fr); gap: 10px;
        }

        canvas { display: block; cursor: crosshair; }
    </style>
</head>
<body>

<div id="toolbar">
    <div class="tool-group">
        <button onclick="setMode('rect')" id="rectBtn" class="active" title="Rectangle"><i class="fa-regular fa-square"></i></button>
        <button onclick="setMode('circle')" id="circleBtn" title="Circle"><i class="fa-regular fa-circle"></i></button>
        <button onclick="setMode('diamond')" id="diamondBtn" title="Diamond"><i class="fa-solid fa-diamond"></i></button>
        <button onclick="setMode('star')" id="starBtn" title="Star"><i class="fa-regular fa-star"></i></button>
    </div>
    <div class="tool-group">
        <button onclick="setMode('arrow')" id="arrowBtn" title="Arrow"><i class="fa-solid fa-arrow-right"></i></button>
        <button onclick="setMode('line')" id="lineBtn" title="Line"><i class="fa-solid fa-grip-lines-vertical" style="transform:rotate(45deg)"></i></button>
    </div>
    <div class="tool-group">
        <button onclick="toggleIcons()" id="iconBtn" title="Icons"><i class="fa-solid fa-icons"></i></button>
        <button onclick="setMode('text')" id="textBtn" title="Text"><i class="fa-solid fa-font"></i></button>
    </div>
    <input type="color" id="colorPicker" value="#1e1e1e" style="width:30px; border:none; background:none;">
    <button onclick="clearCanvas()" style="color: #e74c3c;"><i class="fa-solid fa-trash"></i></button>
    <button onclick="download()"><i class="fa-solid fa-download"></i></button>
</div>

<div id="icon-panel">
    <button onclick="selectIcon('fa-house')"><i class="fa-solid fa-house"></i></button>
    <button onclick="selectIcon('fa-user')"><i class="fa-solid fa-user"></i></button>
    <button onclick="selectIcon('fa-gear')"><i class="fa-solid fa-gear"></i></button>
    <button onclick="selectIcon('fa-envelope')"><i class="fa-solid fa-envelope"></i></button>
    <button onclick="selectIcon('fa-cloud')"><i class="fa-solid fa-cloud"></i></button>
    <button onclick="selectIcon('fa-bolt')"><i class="fa-solid fa-bolt"></i></button>
</div>

<canvas id="canvas"></canvas>

<script>
    const canvas = document.getElementById('canvas');
    const ctx = canvas.getContext('2d');
    let drawing = false, mode = 'rect', shapes = [], currentShape = null;

    function resize() { canvas.width = window.innerWidth; canvas.height = window.innerHeight; redraw(); }
    window.addEventListener('resize', resize);
    resize();

    function setMode(m) {
        mode = m;
        document.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        document.getElementById(m + 'Btn')?.classList.add('active');
        document.getElementById('icon-panel').style.display = 'none';
    }

    function toggleIcons() {
        const p = document.getElementById('icon-panel');
        p.style.display = p.style.display === 'grid' ? 'none' : 'grid';
    }

    function selectIcon(iconClass) {
        mode = 'icon:' + iconClass;
        toggleIcons();
    }

    canvas.addEventListener('mousedown', (e) => {
        drawing = true;
        const color = document.getElementById('colorPicker').value;
        currentShape = { 
            mode, color, x1: e.clientX, y1: e.clientY, 
            seed: Math.random() * 100 // Fixed seed to prevent shaking
        };
        
        if(mode === 'text') {
            const txt = prompt("Text:");
            if(txt) shapes.push({...currentShape, text: txt});
            drawing = false;
            redraw();
        }
    });

    canvas.addEventListener('mousemove', (e) => {
        if (!drawing) return;
        currentShape.x2 = e.clientX;
        currentShape.y2 = e.clientY;
        redraw();
        draw(currentShape);
    });

    canvas.addEventListener('mouseup', () => {
        if (drawing) shapes.push(currentShape);
        drawing = false;
        redraw();
    });

    function redraw() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        shapes.forEach(draw);
    }

    function draw(s) {
        ctx.strokeStyle = s.color; ctx.fillStyle = s.color;
        ctx.lineWidth = 2; ctx.lineCap = "round";
        
        // Stabilized jitter logic using shape seed
        const getJitter = (i) => (Math.sin(s.seed + i) * 3);

        const roughLine = (x1, y1, x2, y2) => {
            ctx.beginPath();
            ctx.moveTo(x1 + getJitter(1), y1 + getJitter(2));
            ctx.lineTo(x2 + getJitter(3), y2 + getJitter(4));
            ctx.stroke();
        };

        if (s.mode === 'rect') {
            roughLine(s.x1, s.y1, s.x2, s.y1); roughLine(s.x2, s.y1, s.x2, s.y2);
            roughLine(s.x2, s.y2, s.x1, s.y2); roughLine(s.x1, s.y2, s.x1, s.y1);
        } else if (s.mode === 'circle') {
            ctx.beginPath();
            ctx.ellipse(s.x1 + (s.x2-s.x1)/2, s.y1 + (s.y2-s.y1)/2, Math.abs(s.x2-s.x1)/2, Math.abs(s.y2-s.y1)/2, 0, 0, 2*Math.PI);
            ctx.stroke();
        } else if (s.mode === 'arrow') {
            roughLine(s.x1, s.y1, s.x2, s.y2);
            const angle = Math.atan2(s.y2 - s.y1, s.x2 - s.x1);
            roughLine(s.x2, s.y2, s.x2 - 15 * Math.cos(angle - 0.5), s.y2 - 15 * Math.sin(angle - 0.5));
            roughLine(s.x2, s.y2, s.x2 - 15 * Math.cos(angle + 0.5), s.y2 - 15 * Math.sin(angle + 0.5));
        } else if (s.mode === 'star') {
            let rot = Math.PI / 2 * 3, x = s.x1, y = s.y1, step = Math.PI / 5;
            ctx.beginPath();
            for (i = 0; i < 5; i++) {
                x = s.x1 + Math.cos(rot) * (s.x2 - s.x1); y = s.y1 + Math.sin(rot) * (s.x2 - s.x1);
                ctx.lineTo(x, y); rot += step;
                x = s.x1 + Math.cos(rot) * (s.x2 - s.x1) / 2; y = s.y1 + Math.sin(rot) * (s.x2 - s.x1) / 2;
                ctx.lineTo(x, y); rot += step;
            }
            ctx.closePath(); ctx.stroke();
        } else if (s.mode.startsWith('icon:')) {
            ctx.font = "40px 'Font Awesome 6 Free'";
            ctx.fontWeight = "900";
            const code = getIconUnicode(s.mode.split(':')[1]);
            ctx.fillText(code, s.x1, s.y1);
        } else if (s.mode === 'text') {
            ctx.font = "20px 'Comic Sans MS'";
            ctx.fillText(s.text, s.x1, s.y1);
        }
    }

    function getIconUnicode(cls) {
        // Simple mapping for demo, usually you'd use the font directly
        const map = {'fa-house': '\uf015', 'fa-user': '\uf007', 'fa-gear': '\uf013', 'fa-envelope': '\uf0e0', 'fa-cloud': '\uf0c2', 'fa-bolt': '\uf0e7'};
        return map[cls] || '\uf005';
    }

    function clearCanvas() { shapes = []; redraw(); }
    function download() {
        const link = document.createElement('a');
        link.download = 'sketch.png';
        link.href = canvas.toDataURL();
        link.click();
    }
</script>
</body>
</html>`

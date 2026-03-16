const http = require('http');
const WebSocket = require('ws');
const https = require('https');

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const TOTAL_QUESTIONS = 20;

// ── In-memory stores ──────────────────────────────────────────────
const rooms = new Map();   // roomCode -> Room
const globalStats = new Map(); // playerName -> { correct, total, games }

// ── HTTP server ───────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (req.method === 'GET' && req.url === '/leaderboard') {
    res.setHeader('Content-Type', 'application/json');
    const board = [...globalStats.entries()]
      .map(([name, s]) => ({
        name,
        pct: s.total > 0 ? Math.round((s.correct / s.total) * 100) : 0,
        games: s.games,
      }))
      .sort((a, b) => b.pct - a.pct)
      .slice(0, 20);
    res.end(JSON.stringify(board));
  } else if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    res.setHeader('Content-Type', 'text/html');
    res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
  } else {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ status: 'ok' }));
  }
});

// ── WebSocket server ──────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.playerId = null;
  ws.roomCode = null;
  ws.playerName = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      handle(ws, msg);
    } catch (e) {
      send(ws, { type: 'error', msg: 'Invalid message' });
    }
  });

  ws.on('close', () => {
    if (ws.roomCode) {
      const room = rooms.get(ws.roomCode);
      if (room) {
        room.players = room.players.filter(p => p.ws !== ws);
        if (room.players.length === 0) {
          rooms.delete(ws.roomCode);
        } else {
          broadcast(room, { type: 'player_left', name: ws.playerName, players: playerList(room) });
        }
      }
    }
  });
});

// ── Message handler ────────────────────────────────────────────────
function handle(ws, msg) {
  switch (msg.type) {
    case 'host': {
      const code = genCode();
      const room = {
        code,
        host: ws,
        players: [],
        questions: [],
        currentQ: -1,
        answers: new Map(),
        timer: null,
        lang: msg.lang || 'en',
        started: false,
      };
      addPlayer(room, ws, msg.name, true);
      rooms.set(code, room);
      send(ws, { type: 'hosted', code, players: playerList(room) });
      break;
    }
    case 'join': {
      const room = rooms.get(msg.code);
      if (!room) { send(ws, { type: 'error', msg: 'Room not found' }); return; }
      if (room.started) { send(ws, { type: 'error', msg: 'Game already started' }); return; }
      addPlayer(room, ws, msg.name, false);
      send(ws, { type: 'joined', code: msg.code, players: playerList(room) });
      broadcast(room, { type: 'player_joined', name: msg.name, players: playerList(room) });
      break;
    }
    case 'start': {
      const room = rooms.get(ws.roomCode);
      if (!room || room.host !== ws) return;
      if (room.players.length < 1) { send(ws, { type: 'error', msg: 'Need at least 1 player' }); return; }
      startGame(room);
      break;
    }
    case 'answer': {
      const room = rooms.get(ws.roomCode);
      if (!room || room.currentQ < 0) return;
      const qid = room.questions[room.currentQ]?.id;
      if (!qid) return;
      if (!room.answers.has(qid)) room.answers.set(qid, new Map());
      room.answers.get(qid).set(ws.playerName, msg.value);
      // Confirm receipt to sender
      send(ws, { type: 'answer_received' });
      // If all players answered, resolve early
      if (room.answers.get(qid).size >= room.players.length) {
        resolveQuestion(room);
      }
      break;
    }
    case 'leaderboard_update': {
      // Update global stats from client-reported result
      if (ws.playerName) {
        const s = globalStats.get(ws.playerName) || { correct: 0, total: 0, games: 0 };
        s.total += 1;
        if (msg.correct) s.correct += 1;
        if (msg.gameEnd) s.games += 1;
        globalStats.set(ws.playerName, s);
      }
      break;
    }
  }
}

// ── Game flow ──────────────────────────────────────────────────────
async function startGame(room) {
  room.started = true;
  broadcast(room, { type: 'game_starting' });

  try {
    room.questions = await generateQuestions(room.lang);
  } catch (e) {
    room.questions = getFallbackQuestions(room.lang);
  }

  // Init scores
  room.players.forEach(p => { p.score = 0; p.correctCount = 0; });
  room.currentQ = -1;
  nextQuestion(room);
}

function nextQuestion(room) {
  room.currentQ++;
  if (room.currentQ >= TOTAL_QUESTIONS || room.currentQ >= room.questions.length) {
    endGame(room);
    return;
  }

  const q = room.questions[room.currentQ];
  broadcast(room, {
    type: 'question',
    index: room.currentQ,
    total: TOTAL_QUESTIONS,
    question: {
      id: q.id,
      category: q.category,
      text: q.text,
      unit: q.unit,
      funny: q.funny || false,
      maxComment: q.maxComment,
    },
  });

  // 30s timer
  let timeLeft = 30;
  room.timer = setInterval(() => {
    timeLeft--;
    broadcast(room, { type: 'tick', t: timeLeft });
    if (timeLeft <= 0) resolveQuestion(room);
  }, 1000);
}

function resolveQuestion(room) {
  clearInterval(room.timer);
  const q = room.questions[room.currentQ];
  const qAnswers = room.answers.get(q.id) || new Map();

  // Build results per player
  const results = room.players.map(p => {
    const val = qAnswers.has(p.name) ? Number(qAnswers.get(p.name)) : null;
    const diff = val !== null ? Math.abs(val - q.answer) : Infinity;
    return { name: p.name, value: val, diff };
  }).sort((a, b) => a.diff - b.diff);

  // Award points: closest gets 100, second 60, third 30
  const points = [100, 60, 30];
  results.forEach((r, i) => {
    if (r.diff !== Infinity && i < 3) {
      const player = room.players.find(p => p.name === r.name);
      if (player) {
        player.score += points[i] || 0;
        if (i === 0) player.correctCount++;
      }
    }
  });

  const winner = results[0]?.diff !== Infinity ? results[0].name : null;

  broadcast(room, {
    type: 'round_result',
    answer: q.answer,
    unit: q.unit,
    maxComment: q.maxComment,
    results: results.map(r => ({
      name: r.name,
      value: r.value,
      diff: r.diff === Infinity ? null : r.diff,
    })),
    winner,
    scores: room.players.map(p => ({ name: p.name, score: p.score })),
  });

  // Next question after 5s
  setTimeout(() => nextQuestion(room), 5000);
}

function endGame(room) {
  const finalScores = room.players
    .map(p => ({ name: p.name, score: p.score, correctCount: p.correctCount }))
    .sort((a, b) => b.score - a.score);

  // Update global stats
  finalScores.forEach(p => {
    const s = globalStats.get(p.name) || { correct: 0, total: 0, games: 0 };
    s.correct += p.correctCount;
    s.total += TOTAL_QUESTIONS;
    s.games += 1;
    globalStats.set(p.name, s);
  });

  const globalBoard = [...globalStats.entries()]
    .map(([name, s]) => ({
      name,
      pct: s.total > 0 ? Math.round((s.correct / s.total) * 100) : 0,
      games: s.games,
    }))
    .sort((a, b) => b.pct - a.pct)
    .slice(0, 20);

  broadcast(room, { type: 'game_over', finalScores, globalBoard });
  rooms.delete(room.code);
}

// ── Claude AI question generation ──────────────────────────────────
function generateQuestions(lang) {
  return new Promise((resolve, reject) => {
    if (!ANTHROPIC_API_KEY) { reject(new Error('No API key')); return; }

    const langName = lang === 'tr' ? 'Turkish' : lang === 'de' ? 'German' : lang === 'fr' ? 'French' : 'English';
    const prompt = `Generate exactly ${TOTAL_QUESTIONS} Guinness World Records trivia questions for a multiplayer guessing game. 
The questions must all have NUMERICAL answers (counts, measurements, distances, weights, ages, speeds, etc.).
Language: ${langName}

Return ONLY a JSON array with exactly ${TOTAL_QUESTIONS} objects. Each object:
{
  "id": "q1",
  "category": "short category name",
  "text": "question text",
  "answer": 123,
  "unit": "km",
  "funny": true/false (true if the record is absurd/funny),
  "maxComment": "Max's funny/surprised reaction comment in ${langName} (1-2 sentences, personality: energetic game show host)"
}

Mix these categories: speed, size, age, distance, temperature, nature, food, sports, technology, animals.
Make some questions funny/surprising (funny: true) — e.g. longest fingernails, heaviest pumpkin, most hot dogs eaten.
All answers must be real verified Guinness records. Return ONLY the JSON array, no other text.`;

    const body = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 4000,
      messages: [{ role: 'user', content: prompt }],
    });

    const options = {
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Length': Buffer.byteLength(body),
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          const text = parsed.content?.[0]?.text || '';
          const clean = text.replace(/```json|```/g, '').trim();
          const questions = JSON.parse(clean);
          if (!Array.isArray(questions)) throw new Error('Not array');
          resolve(questions.slice(0, TOTAL_QUESTIONS));
        } catch (e) {
          reject(e);
        }
      });
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ── Fallback questions ─────────────────────────────────────────────
function getFallbackQuestions(lang) {
  const tr = lang === 'tr';
  return [
    { id:'q1', category: tr?'Boy':'Height', text: tr?'Tarihin en uzun boylu insanı Robert Wadlow kaç cm\'di?':'How tall in cm was Robert Wadlow, tallest person ever?', answer:272, unit:'cm', funny:false, maxComment: tr?'Bence sen bu rekoru küçümsüyorsun... ya çok büyük ya da çok küçük tahmin edeceksin!':'I bet you\'re going to be way off on this one... think bigger!' },
    { id:'q2', category: tr?'Hız':'Speed', text: tr?'Çita saatte kaç km hıza ulaşabilir?':'How fast in km/h can a cheetah run?', answer:112, unit:'km/h', funny:false, maxComment: tr?'Ipucu: Otoyolda seni geçer mi geçmez mi? Düşün!':'Hint: Would it overtake you on the highway? Think carefully!' },
    { id:'q3', category: tr?'Yaş':'Age', text: tr?'Jeanne Calment kaç yaşında vefat etti?':'How old was Jeanne Calment when she passed?', answer:122, unit: tr?'yaş':'years', funny:false, maxComment: tr?'Bu kadın 2 yüzyıl gördü mü acaba? Tahminini iyi yap!':'Did this lady see two centuries? Make your guess wisely!' },
    { id:'q4', category: tr?'Uzunluk':'Length', text: tr?'Nil Nehri kaç km uzunluğundadır?':'How many km long is the Nile River?', answer:6650, unit:'km', funny:false, maxComment: tr?'İpucu: Türkiye\'nin boyunun kaç katı olduğunu düşün...':'Hint: Think about how many times it crosses a continent...' },
    { id:'q5', category: tr?'Derinlik':'Depth', text: tr?'Baykal Gölü kaç metre derindir?':'How deep in meters is Lake Baikal?', answer:1642, unit:'m', funny:false, maxComment: tr?'Buraya taş atsaydın ne kadar sonra dibine ulaşırdı sence?':'If you dropped a stone in, how long before it hits the bottom?' },
    { id:'q6', category: tr?'Ağırlık':'Weight', text: tr?'En ağır balkabağı kaç kg\'dı?':'How many kg was the heaviest pumpkin ever?', answer:1226, unit:'kg', funny:true, maxComment: tr?'Bu balkabağını taşımak için kaç kişi lazım sence? 😏':'How many people do you think it took to carry this thing? 😏' },
    { id:'q7', category: tr?'Sıcaklık':'Temperature', text: tr?'Yeryüzünde ölçülen en yüksek sıcaklık kaç derece Celsius?':'Highest temperature ever recorded on Earth in Celsius?', answer:56, unit:'°C', funny:true, maxComment: tr?'Vücudun eridiği sıcaklığın kaç katı olduğunu düşün...':'Think about how many times hotter than your body temperature...' },
    { id:'q8', category: tr?'Tırnak':'Nails', text: tr?'En uzun tırnak rekoru (tek el) kaç cm\'di?':'Longest fingernails on one hand, in cm?', answer:909, unit:'cm', funny:true, maxComment: tr?'Bu soruyu okuyunca ilk tepkin ne oldu? Tahminini ona göre yap 😅':'What was your first reaction reading this? Base your guess on that 😅' },
    { id:'q9', category: tr?'Spor':'Sports', text: tr?'Usain Bolt\'un 100m rekoru kaç saniyeydi?':'How many seconds was Usain Bolt\'s 100m record?', answer:9, unit:'sn', funny:false, maxComment: tr?'Tek haneli mi çift haneli mi? İşte asıl soru bu!':'Single digit or double digit? That\'s the real question!' },
    { id:'q10', category: tr?'Bina':'Building', text: tr?'Burj Khalifa kaç metre yüksekliğindedir?':'How tall in meters is Burj Khalifa?', answer:828, unit:'m', funny:false, maxComment: tr?'Uçak yüksekliğiyle karşılaştırınca aklın durur...':'Compare it to a cruising airplane altitude and your mind will spin...' },
    { id:'q11', category: tr?'Hayvan':'Animal', text: tr?'Mavi balina kaç metre uzunluğa ulaşabilir?':'How many meters long can a blue whale grow?', answer:33, unit:'m', funny:false, maxComment: tr?'Bir otobüs 12 metre... bunu referans al!':'A bus is about 12 meters... use that as your reference!' },
    { id:'q12', category: tr?'Doğa':'Nature', text: tr?'Everest kaç metre yüksekliğindedir?':'How tall in meters is Mount Everest?', answer:8849, unit:'m', funny:false, maxComment: tr?'Uçaklar bu tepenin üzerinden geçebiliyor mu acaba?':'Can airplanes actually fly over this mountain? Think about it...' },
    { id:'q13', category: tr?'Yiyecek':'Food', text: tr?'En hızlı hamburger yeme rekoru kaç saniyede?':'Fastest time to eat a hamburger, in seconds?', answer:9, unit:'sn', funny:true, maxComment: tr?'Çiğnemek mi şart? Bu rekoru kırmak mümkün mü sence? 🍔':'Is chewing even required? Could you beat this record? 🍔' },
    { id:'q14', category: tr?'Uzay':'Space', text: tr?'Dünya\'nın Güneş\'e ortalama uzaklığı kaç milyon km?':'Earth\'s average distance from the Sun in million km?', answer:150, unit:'M km', funny:true, maxComment: tr?'Işık hızıyla gitsek kaç dakika sürer? Ondan hesapla!':'If light takes ~8 minutes to get there, do the math!' },
    { id:'q15', category: tr?'Teknoloji':'Tech', text: tr?'İlk iPhone ne yılında çıktı?':'What year did the first iPhone come out?', answer:2007, unit: tr?'yıl':'year', funny:false, maxComment: tr?'O zamanlar sen ne yapıyordun? Oradan hesapla 😄':'What were you doing back then? Calculate from there 😄' },
    { id:'q16', category: tr?'Coğrafya':'Geography', text: tr?'Sahara Çölü kaç milyon km² büyüklüğündedir?':'How many million km² is the Sahara Desert?', answer:9, unit:'M km²', funny:false, maxComment: tr?'ABD\'yi düşün, sonra onu katla... ya da katlatma!':'Think of the USA, then multiply... or don\'t!' },
    { id:'q17', category: tr?'İnsan':'Human', text: tr?'En uzun süre uyumama rekoru kaç saattir?':'Longest time without sleep record in hours?', answer:264, unit:'saat', funny:true, maxComment: tr?'Kaç günlük olduğunu düşün önce, saati o zaman bul!':'Think in days first, then convert to hours!' },
    { id:'q18', category: tr?'Spor':'Sports', text: tr?'En fazla olimpiyat altın madalyası kazanan sporcu Michael Phelps kaç altın kazandı?':'How many gold medals did Michael Phelps win at the Olympics?', answer:23, unit: tr?'altın':'gold', funny:false, maxComment: tr?'Bir olimpiyatta maksimum kaç altın kazanılabilir? Ondan fazla!':'More than what one athlete can win in a single Olympics!' },
    { id:'q19', category: tr?'Doğa':'Nature', text: tr?'Amazon Nehri saniyede kaç m³ su taşır (yaklaşık)?':'Amazon River flow rate in m³ per second (approx)?', answer:209000, unit:'m³/s', funny:true, maxComment: tr?'Bu sayıyı söylediğimde ağzın açık kalacak, söz veriyorum! 😂':'When I reveal the answer your jaw will drop, I promise! 😂' },
    { id:'q20', category: tr?'Hayvan':'Animal', text: tr?'Fil ortalama kaç yıl yaşar?':'How many years does an elephant live on average?', answer:70, unit: tr?'yıl':'years', funny:false, maxComment: tr?'İnsanla karşılaştır... sandığından yakın mı uzak mı?':'Compare it to a human lifespan... closer or further than you think?' },
  ];
}

// ── Helpers ────────────────────────────────────────────────────────
function genCode() {
  return 'G-' + Math.floor(1000 + Math.random() * 9000);
}

function addPlayer(room, ws, name, isHost) {
  ws.playerName = name;
  ws.roomCode = room.code;
  room.players.push({ ws, name, score: 0, correctCount: 0, isHost });
}

function playerList(room) {
  return room.players.map(p => ({ name: p.name, isHost: p.isHost }));
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(room, data) {
  room.players.forEach(p => send(p.ws, data));
}

server.listen(PORT, () => {
  console.log(`Guinness Tahmin server running on port ${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
  console.log(`Leaderboard: http://localhost:${PORT}/leaderboard`);
  if (!ANTHROPIC_API_KEY) console.warn('WARNING: ANTHROPIC_API_KEY not set — using fallback questions');
});

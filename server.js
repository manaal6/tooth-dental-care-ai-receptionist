import express from "express";
import http from "http";
import https from "https";
import { WebSocketServer } from "ws";
import WebSocket from "ws";
import dotenv from "dotenv";
import Groq from "groq-sdk";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import os from "os";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ─── .env ─────────────────────────────────────────────────────────────────────
const envPath = path.join(__dirname, ".env");
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
  console.log(`✅  Loaded .env from: ${envPath}`);
} else {
 console.log(`ℹ️  No .env file found — using platform environment variables`);
}

const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const GROQ_API_KEY     = process.env.GROQ_API_KEY;
const VOICE            = process.env.VOICE || "aura-asteria-en";
const PORT             = process.env.PORT  || 3000;
const HOST             = process.env.HOST  || "0.0.0.0";  // FIX 12: bind all interfaces

if (!DEEPGRAM_API_KEY || DEEPGRAM_API_KEY === "your_deepgram_api_key_here") {
  console.error("❌  DEEPGRAM_API_KEY missing"); process.exit(1);
}
if (!GROQ_API_KEY || GROQ_API_KEY === "your_groq_api_key_here") {
  console.error("❌  GROQ_API_KEY missing"); process.exit(1);
}

const groq = new Groq({ apiKey: GROQ_API_KEY });

// ─── Express app ──────────────────────────────────────────────────────────────
const app = express();

// FIX 11: JSON body parser with size limit
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: false, limit: "1mb" }));

// FIX 5: Security headers (inline — avoids extra dependency for minimal surface)
app.use((_req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-XSS-Protection", "1; mode=block");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Permissions-Policy", "camera=(), geolocation=(), microphone=(self)");
  next();
});

// FIX 6: Compression middleware
import compression from "compression";
app.use(compression());

// Serve static files
app.use(express.static(path.join(__dirname)));
app.get("/", (_req, res) => res.sendFile(path.join(__dirname, "index.html")));

// ─── Leads file ───────────────────────────────────────────────────────────────
const LEADS_FILE = path.join(__dirname, "leads.json");

// FIX 8: Health check endpoint
app.get("/api/health", (_req, res) => {
  res.json({
    status: "ok",
    uptime: Math.round(process.uptime()),
    timestamp: new Date().toISOString(),
    connections: activeConnections,
    memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
  });
});

app.get("/api/leads", (_req, res) => {
  try {
    const data = fs.existsSync(LEADS_FILE)
      ? JSON.parse(fs.readFileSync(LEADS_FILE, "utf8")) : [];
    res.json(Array.isArray(data) ? data : []);
  } catch { res.json([]); }
});

// ─── FIX 4: HTTPS support ─────────────────────────────────────────────────────
const certPath = path.join(__dirname, "cert.pem");
const keyPath  = path.join(__dirname, "key.pem");
let server;
if (fs.existsSync(certPath) && fs.existsSync(keyPath)) {
  server = https.createServer({
    cert: fs.readFileSync(certPath),
    key:  fs.readFileSync(keyPath),
  }, app);
  console.log("🔒 HTTPS mode (cert.pem + key.pem found)");
} else {
  server = http.createServer(app);
  console.log("🔓 HTTP mode (no cert.pem/key.pem — add them for HTTPS)");
}

// ─── FIX 7: Rate limiting — max 5 concurrent WebSocket connections ────────────
let activeConnections = 0;
const MAX_WS_CONNECTIONS = 5;

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;
  if (pathname !== "/ws") {
    socket.destroy();
    return;
  }
  if (activeConnections >= MAX_WS_CONNECTIONS) {
    console.warn(`  ⚠️  Rejecting WS connection — limit ${MAX_WS_CONNECTIONS} reached`);
    socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request);
  });
});

// ─── TTS with connection keep-alive agent (reuses TCP connection) ─────────────
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 6 });

function ttsAudioBase64(text) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ text });
    const req  = https.request({
      hostname: "api.deepgram.com",
      path:     `/v1/speak?model=${VOICE}&encoding=linear16&container=wav`,
      method:   "POST",
      agent:    httpsAgent,
      headers: {
        "Authorization":  `Token ${DEEPGRAM_API_KEY}`,
        "Content-Type":   "application/json",
        "Content-Length": Buffer.byteLength(body),
      },
    }, (res) => {
      if (res.statusCode !== 200) {
        let e = ""; res.on("data", d => e += d);
        res.on("end", () => reject(new Error(`TTS ${res.statusCode}: ${e}`))); return;
      }
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end",  () => resolve(Buffer.concat(chunks).toString("base64")));
      res.on("error", reject);
    });
    req.on("error", reject); req.write(body); req.end();
  });
}

// ─── Sentence splitter ────────────────────────────────────────────────────────
function splitSentences(text) {
  const parts = []; let last = 0;
  const re = /([.!?])\s+/g; let m;
  while ((m = re.exec(text)) !== null) {
    const chunk = text.slice(last, m.index + 1).trim();
    if (chunk.length > 2) parts.push(chunk);
    last = m.index + m[0].length;
  }
  const tail = text.slice(last).trim();
  if (tail.length > 2) parts.push(tail);
  return parts.length ? parts : [text.trim()];
}

// ─── Handle one user turn ─────────────────────────────────────────────────────
async function handleUserTurn(ws, text, history, speakingRef) {
  if (!text?.trim()) return;
  console.log(`  👤 User : ${text}`);
  history.push({ role: "user", content: text });
  safeSend(ws, { type: "user_transcript", text });

  try {
    const t0 = Date.now();

    const stream = await groq.chat.completions.create({
      model:       "llama-3.1-8b-instant",
      stream:      true,
      max_tokens:  180,
      temperature: 0.7,
      messages: [
        {
          role: "system",
          content: `You are Tooth Dental Care, the AI Reception Agent for "tooth" dental care in London, London (toothlondon.co.uk).
Speak warmly, professionally, and naturally — like a real, friendly front-desk receptionist on the phone.

CLINIC FACTS (use only if relevant/asked):
- Two practices on Lower Marsh, London, SE1: 45-46 Lower Marsh (general dentistry & hygiene) and 26 Lower Marsh (specialist centre: oral surgery, implants, periodontics, endodontics).
- Phone: 020 7928 2875. Also bookable via WhatsApp.
- Hours: Mon–Fri 8am–8pm, Sat 10am–4pm, closed Sunday.
- Services: check-ups, hygiene/cleaning, whitening, fillings, Invisalign, wisdom tooth extractions, root canal, implants, crowns, bridges, periodontal treatment, facial aesthetics.
- New Patient Exam offer: £45.
- Rated 4.9 stars from 380+ Google reviews, London's Best Dental Practice.

Follow this call flow STRICTLY in order. Do NOT skip any step.

STEP 1 — Greeting (first message only)
Greet warmly, introduce yourself as receptionist from tooth dental care in London. Max 2 sentences.

STEP 2 — Understand the Need
Ask: "What can I help you with today — are you looking to book an appointment, or do you have a question about a treatment?"

STEP 3 — Follow-up
Ask ONE follow-up question to understand what treatment or concern they have (e.g. check-up, hygiene, whitening, emergency, implants, etc).

STEP 4 — Collect Name
Ask for their name naturally if not already given. Example: "Lovely! Could I take your name please?"

STEP 5 — Collect Phone Number
Ask: "Could I also grab your phone number so our team can confirm your appointment?"
WAIT for a numeric response. Do NOT proceed until you have received an actual phone number from the caller.
If the caller gives something that is clearly NOT a phone number, politely ask again.
NEVER invent, guess, or fabricate a phone number. You must hear the digits from the caller.
If the number sounds incomplete (fewer than 10 digits), ask: "I just want to make sure I got that right — could you repeat the full number for me?"

STEP 6 — Confirm
Read back: name, phone number, and the treatment/service they need — all three. Keep it to 2 sentences.
Only include digits the caller explicitly said. If you are unsure of a digit, ask again rather than guessing.

STEP 7 — Close
Thank them and say someone from the tooth team will be in touch soon to confirm their appointment.

STRICT RULES:
- Maximum 2 sentences per reply. Never longer.
- NEVER say you have a phone number unless the caller explicitly said one in this conversation.
- NEVER fabricate, invent, or assume any phone number digits. Only repeat back exactly what the caller said.
- If a caller gives a single ambiguous word (like "yes", "no", "sure", "okay") in response to a name or phone question, ask for clarification — do not treat it as a name or number.
- Never quote exact prices beyond the £45 New Patient Exam unless asked — offer to have the team confirm pricing.
- Never output JSON, bullet points, or markdown.
- Never break character.
- Plain spoken language only.
- If asked something off-topic, gently steer back to booking or their dental needs.`,
        },
        ...history,
      ],
    });

    let fullReply  = "";
    let buffer     = "";
    let ttsChain   = Promise.resolve();
    let idx        = 0;
    let firstToken = true;

    // Mark Aura as speaking so STT ignores mic input during playback
    speakingRef.active = true;

    const flush = (force = false) => {
      const sentences = splitSentences(buffer);
      const toSend    = force ? sentences : sentences.slice(0, -1);
      const leftover  = force ? "" : (sentences[sentences.length - 1] || "");

      for (const s of toSend) {
        if (!s) continue;
        const i    = idx++;
        const text = s;

        ttsChain = ttsChain.then(async () => {
          try {
            if (i === 0) console.log(`  ⚡ First audio in ${Date.now() - t0}ms`);
            const audio = await ttsAudioBase64(text);
            safeSend(ws, { type: "ai_audio", audio, idx: i });
            console.log(`  🔊 TTS[${i}] sent (${Date.now() - t0}ms total)`);
          } catch (e) { console.error(`  ❌ TTS[${i}]:`, e.message); }
        });
      }
      buffer = leftover;
    };

    for await (const chunk of stream) {
      const token = chunk.choices[0]?.delta?.content ?? "";
      if (!token) continue;
      if (firstToken) {
        console.log(`  ⚡ First token in ${Date.now() - t0}ms`);
        firstToken = false;
      }
      fullReply += token;
      buffer    += token;
      if (/[.!?]\s/.test(buffer)) flush();
    }

    if (buffer.trim()) flush(true);
    await ttsChain;

    // Aura finished speaking — re-enable STT
    speakingRef.active = false;

    const reply = fullReply.trim();
    console.log(`  🤖 Aura : ${reply} (${Date.now() - t0}ms total)`);
    history.push({ role: "assistant", content: reply });
    safeSend(ws, { type: "ai_text", text: reply });

    // ── Push lead to Google Sheets — only when phone was actually collected ─────
    const lowerReply = reply.toLowerCase();
    const isClosing  =
      (lowerReply.includes("someone") && lowerReply.includes("follow")) ||
      (lowerReply.includes("team") && lowerReply.includes("touch")) ||
      lowerReply.includes("have a great day") ||
      lowerReply.includes("have a good day");
    if (isClosing) {
      const lead = extractLead(history);
      if (lead.name && lead.phone && !history._leadPushed) {
        history._leadPushed = true;
        await pushLeadToSheets(lead, history);
      } else if (!history._leadPushed) {
        console.log(`  ⚠️  Call closed but lead incomplete — name:"${lead.name}" phone:"${lead.phone}" — NOT pushed`);
      }
    }

  } catch (err) {
    speakingRef.active = false; // always reset even on error
    console.error("  ❌ handleUserTurn:", err.message);
    safeSend(ws, { type: "ai_text", text: "Sorry, something went wrong." });
  }
}

function safeSend(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

// ─── Google Sheets Lead Capture ───────────────────────────────────────────────
const SHEETS_WEBHOOK = process.env.SHEETS_WEBHOOK_URL;

async function pushLeadToSheets(lead, history) {
  // FIX 9: Atomic write — write to .tmp then rename
  const leadsArr = fs.existsSync(LEADS_FILE)
    ? (() => { try { return JSON.parse(fs.readFileSync(LEADS_FILE, "utf8")); } catch { return []; } })()
    : [];
  leadsArr.push({
    ...lead,
    timestamp: new Date().toISOString(),
    completed: true,
    duration: Math.round((Date.now() - (history._callStart || Date.now())) / 1000),
  });
  const tmpFile = LEADS_FILE + ".tmp";
  fs.writeFileSync(tmpFile, JSON.stringify(leadsArr, null, 2));
  fs.renameSync(tmpFile, LEADS_FILE);

  if (!SHEETS_WEBHOOK) return;
  try {
    await fetch(SHEETS_WEBHOOK, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(lead),
    });
    console.log("  📋 Lead pushed to Google Sheets:", lead.name);
  } catch (err) {
    console.error("  ❌ Sheets push failed:", err.message);
  }
}

// ─── FIX 1 + 2 + 3: Improved lead extraction ─────────────────────────────────

// Words that should never be treated as a person's name
const NAME_STOPWORDS = new Set([
  // common conversational
  "yes", "no", "yeah", "yep", "nope", "sure", "okay", "ok", "right", "um",
  "uh", "hi", "hello", "hey", "thanks", "thank", "please", "well", "so",
  "just", "actually", "really", "maybe", "absolutely", "definitely", "great",
  // dental / service terms
  "appointment", "booking", "check", "checkup", "cleaning", "whitening",
  "filling", "fillings", "invisalign", "implant", "implants", "crown",
  "crowns", "bridge", "bridges", "extraction", "root", "canal", "hygiene",
  "emergency", "tooth", "teeth", "dental", "treatment", "consultation",
  // flow-related
  "looking", "book", "question", "price", "prices", "cost", "new", "patient",
]);

function extractLead(history) {
  const wordToDigit = {
    zero:"0", oh:"0", o:"0",   // FIX 2: "oh" → 0
    one:"1", two:"2", three:"3", four:"4",
    five:"5", six:"6", seven:"7", eight:"8", nine:"9",
  };

  const userMsgs      = history.filter(m => m.role === "user");
  const assistantMsgs = history.filter(m => m.role === "assistant");
  const userText      = userMsgs.map(m => m.content).join(" ");

  // ── FIX 1: Name — context-aware extraction ──────────────────────────────────
  // Strategy: find where the assistant asked for a name, then the very next user
  // message is treated as the name response.
  let extractedName = "";

  // Method A: Check if assistant asked for name; next user reply is the name
  for (let i = 0; i < history.length; i++) {
    const msg = history[i];
    if (msg.role === "assistant" && /(?:your\s+name|take\s+your\s+name|may\s+i\s+(?:get|have)\s+your\s+name|what(?:'s|\s+is)\s+your\s+name|who\s+am\s+i\s+speaking)/i.test(msg.content)) {
      // Find the next user message after this assistant message
      for (let j = i + 1; j < history.length; j++) {
        if (history[j].role === "user") {
          const resp = history[j].content.trim();
          // Try "my name is X", "I'm X", "it's X", "this is X"
          const explicit = resp.match(/(?:my\s+name\s+is|i[''\u2019]?m|it[''\u2019]?s|this\s+is|call\s+me|i\s+am)\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)/i);
          if (explicit) {
            const candidate = explicit[1].trim();
            if (!NAME_STOPWORDS.has(candidate.toLowerCase())) {
              extractedName = candidate;
              break;
            }
          }
          // Otherwise, if the whole response is 1-3 words (just a name), take it
          const words = resp.replace(/[^a-zA-Z\s]/g, "").trim().split(/\s+/).filter(Boolean);
          if (words.length >= 1 && words.length <= 3) {
            const candidate = words.join(" ");
            if (!words.some(w => NAME_STOPWORDS.has(w.toLowerCase()))) {
              extractedName = candidate;
              break;
            }
          }
          break; // only check the first user reply after the name question
        }
      }
      if (extractedName) break;
    }
  }

  // Method B: Fallback — pattern match across all user text
  if (!extractedName) {
    const nameMatch =
      userText.match(/(?:my\s+name\s+is|i[''\u2019]?m|this\s+is|call\s+me|i\s+am)\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)/i) ||
      userText.match(/\bhi\.?\s+i[''\u2019]?m\s+([a-zA-Z]+)/i);
    if (nameMatch) {
      const candidate = nameMatch[1].trim();
      if (!NAME_STOPWORDS.has(candidate.toLowerCase())) {
        extractedName = candidate;
      }
    }
  }

  // Method C: Last resort — check if assistant addressed user by name
  if (!extractedName) {
    const addressed = assistantMsgs
      .map(m => m.content.match(/\b(?:hello|hi|thanks?|thank you)[,.]?\s+([A-Z][a-z]+)/i)?.[1])
      .filter(n => n && !NAME_STOPWORDS.has(n.toLowerCase()));
    if (addressed.length) extractedName = addressed[addressed.length - 1];
  }

  // ── FIX 2: Phone — improved digit extraction ────────────────────────────────
  // Handle "double X" → "XX", "triple X" → "XXX" patterns
  let phoneText = userText;

  // "double X" / "triple X" expansion
  phoneText = phoneText.replace(
    /\b(double|triple)\s+(zero|oh|o|one|two|three|four|five|six|seven|eight|nine|\d)\b/gi,
    (_, mult, digit) => {
      const d = wordToDigit[digit.toLowerCase()] ?? digit;
      return mult.toLowerCase() === "double" ? `${d} ${d}` : `${d} ${d} ${d}`;
    }
  );

  // Convert spoken words → digits
  phoneText = phoneText.replace(
    /\b(zero|oh|one|two|three|four|five|six|seven|eight|nine)\b/gi,
    w => wordToDigit[w.toLowerCase()] ?? w
  );

  // Collapse all digit sequences, then find runs of 5+ digits (FIX 2: min 5)
  const digitsOnly   = phoneText.replace(/[^0-9]/g, " ").trim();
  const digitMatches = digitsOnly.match(/[0-9]{5,}/g) || [];
  // Also try UK / PK-style full number
  const ukMatch      = phoneText.match(/(?:\+44[\s-]?|0)[0-9\s-]{9,13}/);
  const pkMatch      = phoneText.match(/(?:\+92[\s-]?|0)[0-9]{9,10}/);

  // Prefer longest digit run (most complete number), fall back to UK/PK match
  const bestDigitRun = digitMatches.sort((a, b) => b.length - a.length)[0] || "";
  const phoneRaw     = ukMatch?.[0]?.replace(/\D/g, "") ||
                       pkMatch?.[0]?.replace(/\D/g, "") ||
                       bestDigitRun;

  // ── Notes: AI's step-6 confirmation line ──────────────────────────────────────
  const confirmMsg = [...assistantMsgs].reverse().find(m =>
    /\d{4,}/.test(m.content.replace(/\b(zero|one|two|three|four|five|six|seven|eight|nine)\b/gi, d => wordToDigit[d.toLowerCase()]))
  );

  // ── Service ───────────────────────────────────────────────────────────────────
  const serviceFromConfirm = confirmMsg?.content.match(
    /(?:inquir(?:ing|ed)\s+about|looking\s+for|interested\s+in|regarding|about\s+(?:prices?\s+for)?|you\s+(?:were|are)\s+(?:looking\s+for|interested\s+in))\s+(?:prices?\s+(?:for|on)\s+)?([^,.!?\n]{3,60})/i
  );
  const serviceFromUser = userText.match(
    /(?:i\s+want(?:\s+to\s+(?:know|buy|get|purchase))?(?:\s+about)?(?:\s+the\s+prices?\s+of)?|i\s+need|looking\s+for|buy|get|purchase|know\s+about(?:\s+the\s+prices?\s+of)?)\s+(?:a\s+|your\s+)?([^,.!?\n]{3,60})/i
  );
  const service = (serviceFromConfirm?.[1] || serviceFromUser?.[1] || "").replace(/\s+/g, " ").trim();

  // Capitalise first letter of each word in name (STT gives lowercase)
  const name = extractedName.replace(/\b\w/g, c => c.toUpperCase());

  return {
    name,
    phone:   phoneRaw   || "",
    service: service    || "",
    notes:   confirmMsg?.content?.trim() || "",
  };
}

// ─── Raw WebSocket STT ────────────────────────────────────────────────────────
function createSTTSession(clientWs, history, processingRef, speakingRef, sessionRef) {
  let dgWs           = null;
  let keepAlive      = null;
  let shouldRun      = true;
  let reconnectTimer = null;
  let audioQueue     = [];
  let finalBuffer    = "";
  let silenceTimer   = null;
  let reconnectDelay = 1000;

  const DG_URL =
    'wss://api.deepgram.com/v1/listen' +
    '?model=nova-2' +
    '&language=en-US' +
    '&encoding=linear16' +
    '&sample_rate=16000' +
    '&channels=1' +
    '&punctuate=true' +
    '&interim_results=true' +
    '&endpointing=300';

  function connect() {
    if (!shouldRun) return;
    finalBuffer = "";
    if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; }
    console.log("  🟡 Deepgram STT: connecting…");

    dgWs = new WebSocket(DG_URL, {
      headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` },
    });

    dgWs.on("open", () => {
      console.log("  🟢 Deepgram STT open ✔");
      reconnectDelay = 1000;

      if (audioQueue.length > 0) {
        console.log(`  📤 Flushing ${audioQueue.length} queued audio frames`);
        audioQueue.forEach(frame => dgWs.send(frame));
        audioQueue = [];
      }

      keepAlive = setInterval(() => {
        if (dgWs && dgWs.readyState === 1) {
          dgWs.send(JSON.stringify({ type: "KeepAlive" }));
        }
      }, 8000);
    });

    dgWs.on("message", async (raw) => {
      let data;
      try { data = JSON.parse(raw.toString()); } catch { return; }
      if (data.type !== "Results") return;

      const transcript = data.channel?.alternatives?.[0]?.transcript?.trim();
      if (!transcript) return;

      if (!data.is_final) {
        if (!speakingRef.active) {
          safeSend(clientWs, { type: "interim_transcript", text: transcript });
        }
        return;
      }

      // Completely ignore is_final results while Aura is speaking
      if (speakingRef.active) return;

      finalBuffer = (finalBuffer + " " + transcript).trim();

      if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; }

      const flush = async () => {
        silenceTimer = null;
        if (!finalBuffer || processingRef.active) return;
        const utterance = finalBuffer;
        finalBuffer = "";
        processingRef.active = true;
        try   { await handleUserTurn(clientWs, utterance, history, speakingRef); }
        finally { processingRef.active = false; }
      };

      if (data.speech_final) {
        await flush();
      } else {
        silenceTimer = setTimeout(flush, 600);
      }
    });

    dgWs.on("error", (err) => {
      console.error("  ❌ STT WebSocket error:", err.message);
      if (err.message.includes("ENOTFOUND") || err.message.includes("ECONNREFUSED")) {
        reconnectDelay = Math.min(reconnectDelay * 2, 16000);
        console.log(`  ⏳ DNS failure — retrying in ${reconnectDelay / 1000}s`);
      }
    });

    dgWs.on("close", (code, reason) => {
      clearInterval(keepAlive);
      const msg = reason?.toString() || "";
      console.log(`  🔴 Deepgram STT closed (code ${code}${msg ? ": " + msg : ""})`);
      if (shouldRun && code !== 1000) {
        if (code === 1008) { shouldRun = false; return; }
        reconnectTimer = setTimeout(connect, reconnectDelay);
      }
    });
  }

  sessionRef.send = (data) => {
    if (speakingRef.active) return;
    if (dgWs && dgWs.readyState === 1) {
      dgWs.send(data);
    } else if (shouldRun) {
      audioQueue.push(data);
      if (audioQueue.length > 200) audioQueue.shift();
    }
  };

  sessionRef.stop = () => {
    shouldRun = false;
    audioQueue = [];
    finalBuffer = "";
    if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; }
    clearInterval(keepAlive);
    clearTimeout(reconnectTimer);
    if (dgWs) { try { dgWs.close(1000, "mic stopped"); } catch (_) {} dgWs = null; }
    console.log("  🎙️  Mic stopped — STT session closed");
  };

  connect();
}

// ─── WebSocket handler ────────────────────────────────────────────────────────
wss.on("connection", (ws) => {
  activeConnections++;
  console.log(`\n📞 Client connected (${activeConnections}/${MAX_WS_CONNECTIONS})`);

  const history       = [];
  const processingRef = { active: false };
  const speakingRef   = { active: false };
  const sessionRef    = {};
  history._callStart  = Date.now();

  createSTTSession(ws, history, processingRef, speakingRef, sessionRef);
  let sttActive = true;

  ws.on("message", async (data, isBinary) => {
    if (isBinary) {
      sessionRef.send?.(data);
      return;
    }
    try {
      const msg = JSON.parse(data.toString());
      switch (msg.type) {
        case "user_message":
          if (msg.text?.trim() && !processingRef.active) {
            processingRef.active = true;
            try   { await handleUserTurn(ws, msg.text.trim(), history, speakingRef); }
            finally { processingRef.active = false; }
          }
          break;
        case "start_mic":
          if (!sttActive) {
            createSTTSession(ws, history, processingRef, speakingRef, sessionRef);
            sttActive = true;
          }
          break;
        case "stop_mic":
          sessionRef.stop?.();
          sttActive = false;
          break;
      }
    } catch (e) { console.warn("  ⚠️  Parse error:", e.message); }
  });

  ws.on("close", () => {
    activeConnections = Math.max(0, activeConnections - 1);
    console.log(`📵 Client disconnected (${activeConnections}/${MAX_WS_CONNECTIONS})`);
    sessionRef.stop?.();
  });
  ws.on("error", (e) => console.error("  ❌ WS error:", e.message));
});

// ─── Start server ─────────────────────────────────────────────────────────────
const protocol = server instanceof https.Server ? "https" : "http";
const wsProto  = server instanceof https.Server ? "wss"   : "ws";

server.listen(PORT, HOST, () => {
  console.log(`\n✅  Aura AI Agent running → ${protocol}://${HOST}:${PORT}`);
  console.log(`   WebSocket endpoint   → ${wsProto}://${HOST}:${PORT}/ws`);
  console.log(`   Voice model          → ${VOICE}`);
  console.log(`   Max WS connections   → ${MAX_WS_CONNECTIONS}`);
  console.log(`   Health check         → ${protocol}://${HOST}:${PORT}/api/health\n`);
});

// ─── FIX 10: Graceful shutdown ────────────────────────────────────────────────
function gracefulShutdown(signal) {
  console.log(`\n⏹  ${signal} received — shutting down gracefully…`);

  // Stop accepting new connections
  server.close(() => {
    console.log("   HTTP server closed");
  });

  // Close all WebSocket connections
  wss.clients.forEach(ws => {
    try { ws.close(1001, "server shutting down"); } catch (_) {}
  });

  // Destroy the keep-alive agent to release sockets
  httpsAgent.destroy();

  // Give in-flight requests 5s to finish, then force exit
  setTimeout(() => {
    console.log("   Force exit after timeout");
    process.exit(0);
  }, 5000).unref();
}

process.on("SIGINT",  () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// index.mjs — Voice Assistant (Twilio WS) + Deepgram STT + Azure TTS + n8n (BD)
// TwiML: <Connect><Stream track="both_tracks" url="wss://TU_HOST/twilio" /></Connect>

import express from 'express';
import path from 'path';
import { WebSocketServer } from 'ws';
import { createClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import { processFrame, clearVadState } from './vad.mjs';

dotenv.config();

/* =========================
   ENV HELPERS
   ========================= */
const envStr = (k, d) => {
  const v = process.env[k];
  return (v === undefined || v === null || v === '') ? d : v;
};
const envNum = (k, d) => {
  const v = process.env[k];
  if (v === undefined || v === null || v === '') return d;
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};

/* =========================
   EXPRESS
   ========================= */
const app = express();
const port = envNum('PORT', 3000);
const __dirname = process.cwd();

app.use('/public', express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '10mb' }));

/* =========================
   EXTERNAL SERVICES
   ========================= */
const deepgram = createClient(envStr('DEEPGRAM_API_KEY', ''));

// Azure TTS
const AZURE_SPEECH_KEY    = envStr('AZURE_SPEECH_KEY', '');
const AZURE_SPEECH_REGION = envStr('AZURE_SPEECH_REGION', '');
const AZURE_TTS_VOICE     = envStr('AZURE_TTS_VOICE', 'es-MX-DaliaNeural');

// n8n
const N8N_BASE_URL      = envStr('N8N_BASE_URL', '');
const N8N_SHARED_SECRET = envStr('N8N_SHARED_SECRET', 'pizzeriadonnapoliSUPERSECRETO');
const N8N_WEBHOOK_URL   = envStr('N8N_WEBHOOK_URL', ''); // opcional logs

/* =========================
   n8n HELPERS
   ========================= */
async function callN8n(payloadObj) {
  if (!N8N_BASE_URL) {
    console.warn('⚠️ N8N_BASE_URL vacío');
    return null;
  }
  try {
    const res = await fetch(N8N_BASE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-shared-secret': N8N_SHARED_SECRET,
      },
      body: JSON.stringify(payloadObj),
    });
    const text = await res.text();
    if (!res.ok) {
      console.error('❌ n8n error', res.status, text);
      return null;
    }
    try { return JSON.parse(text); } catch { return text; }
  } catch (e) {
    console.error('⚠️ callN8n exception:', e?.message || e);
    return null;
  }
}

async function logToN8n(payload) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    fetch(N8N_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-shared-secret': N8N_SHARED_SECRET },
      body: JSON.stringify(payload),
    }).catch(()=>{});
  } catch {}
}

const toNullIfEmpty = (v) => {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  if (!s) return null;
  const low = s.toLowerCase();
  // limpia "null", "undefined", "NaN" como texto
  if (low === 'undefined' || low === 'null' || low === 'nan') return null;
  return s;
};

async function fetchClientMemory(callerId) {
  const out = await callN8n({ action: 'get_memory', callerId });
  if (!out || typeof out !== 'object') return {};
  const row = Array.isArray(out) ? out[0] : out;
  if (!row) return {};
  return {
    nombreCliente: toNullIfEmpty(row.nombre_cliente),
    numeroTelefono: toNullIfEmpty(row.numero_telefono),
    direccionConfirmada: !!row.direccion_confirmada,
    ultimoPedido: toNullIfEmpty(row.ultimo_pedido),
  };
}

async function fetchMenuAndPromos() {
  const out = await callN8n({ action: 'get_menu'||'get_promos' });
  if (!out) return { menu: [], promos: [] };
  let menu = [], promos = [];
  try {
    menu = Array.isArray(out.menu) ? out.menu : [];
    promos = Array.isArray(out.promos) ? out.promos : [];
  } catch {}
  return { menu, promos };
}

function sanitizeSnapshot(x = {}) {
  return {
    callerId: toNullIfEmpty(x.callerId),
    nombreCliente: toNullIfEmpty(x.nombreCliente),
    numeroTelefono: toNullIfEmpty(x.numeroTelefono),
    ultimoPedido: toNullIfEmpty(x.ultimoPedido),
    direccionConfirmada: !!x.direccionConfirmada,
    ultimoMensajeAsistente: toNullIfEmpty(x.ultimoMensajeAsistente),
    endedAt: toNullIfEmpty(x.endedAt),
  };
}

async function pushClientMemoryUpdate(snapshot) {
  const s = sanitizeSnapshot(snapshot);
  await callN8n({ action: 'update_memory', ...s });
}

async function fetchBotConfig() {
  const out = await callN8n({ action: 'get_config' });
  if (!out || typeof out !== 'object') return { greeting: null, systemPrompt: null };
  return {
    greeting: out.greeting_es || null,
    systemPrompt: out.system_prompt_es || null,
  };
}

/* =========================
   TEMPLATE & PROMPTS
   ========================= */
function renderTemplate(tpl, ctx = {}) {
  if (!tpl) return '';
  return tpl.replace(/\{\{([^}]+)\}\}/g, (_, expr) => {
    const [path, defRaw] = expr.split('|').map(s => s?.trim());
    const def = defRaw ?? '';
    const v = path?.split('.').reduce((acc, k) =>
      (acc && acc[k] !== undefined) ? acc[k] : undefined, ctx);
    return (v === undefined || v === null || v === '') ? def : String(v);
  });
}

function buildSystemPromptFromDbTemplate(systemPromptTpl, session) {
  const ctx = {
    callerId: session.callerId,
    nombreCliente: session.nombreCliente,
    numeroTelefono: session.numeroTelefono || session.callerId,
    direccionConfirmada: session.direccionConfirmada ? 'sí' : 'no',
  };
  let prompt = renderTemplate(systemPromptTpl, ctx);
  const menuStr = JSON.stringify(session.menu ?? []);
  const promosStr = JSON.stringify(session.promos ?? []);
  prompt += `\n\nMenú:\n${menuStr}\n\nPromociones:\n${promosStr}\n`;
  return prompt.trim();
}

function isNoiseUtterance(str) {
  const cleaned = (str || '').toLowerCase().replace(/[¿?¡!.,]/g,'').trim();
  const tokens = cleaned.split(/\s+/).filter(Boolean);
  if (tokens.length <= 3) {
    const common = ['alo','aló','hola','holaa','buenas','si','sí','me','escuchas','alooo','dime','diga','dígame','buenas tardes','hola?','aló?'];
    if (common.includes(tokens.join(' '))) return true;
  }
  const uniq = new Set(tokens);
  if (uniq.size === 1 && tokens.length <= 5) return true;
  return false;
}

function classifyIntent(text) {
  const t = (text || '').toLowerCase();
  if (/\b(menu|menú|sabores|pizzas?|precios?)\b/.test(t)) return 'ask_menu';
  if (/\bpromo(s|ciones)?|oferta(s)?|descuento(s)?\b/.test(t)) return 'ask_promos';
  if (/\b(confirmo|es correcto|esa es|sí,? (confirmo|está bien))\b/.test(t)) return 'confirm_address';
  if (/\b(gracias|solo eso|nada más|está bien|listo)\b/.test(t)) return 'closing';
  return 'chat';
}

/* =========================
   ENTITY EXTRACTION (heurística simple)
   ========================= */
async function extractEntities(userText) {
  const out = { nombreCliente: null, ultimoPedido: null };
  const m1 = userText.match(/\b(me llamo|soy)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)/i);
  if (m1) out.nombreCliente = m1[2].trim();

  const pedidoRegex = /\b(quiero|deme|pon|me das|me pones|me agregas|una|un)\s+([a-záéíóúñ0-9\s\-\.,]{4,})/i;
  const m3 = userText.match(pedidoRegex);
  if (m3) out.ultimoPedido = m3[2].replace(/\s{2,}/g, ' ').trim();

  if (!out.ultimoPedido) {
    const bebida = userText.match(/\b(coca[\-\s]?cola|sprite|fanta|gaseosa|cola)\b/i);
    const pizza = userText.match(/\b(pizza\s+(?:de\s+)?[a-záéíóúñ]+|pepperoni|margarita|hawaiana)\b/i);
    const extra = [];
    if (pizza) extra.push(pizza[0]);
    if (bebida) extra.push(bebida[0]);
    if (extra.length) out.ultimoPedido = extra.join(' y ');
  }

  out.nombreCliente = toNullIfEmpty(out.nombreCliente);
  out.ultimoPedido = toNullIfEmpty(out.ultimoPedido);
  return out;
}

/* =========================
   OPENAI CHAT
   ========================= */
async function answerWithOpenAI_usingSystemPrompt(st, userText, systemPrompt) {
  if (isNoiseUtterance(userText)) {
    const canned = (st.session.step !== 'saludo')
      ? 'Sí, te escucho claro. ¿Qué te gustaría pedir?'
      : 'Pizzería Don Napoli, hola. ¿Qué te gustaría pedir hoy?';
    st.lastReplyText = canned;
    return canned;
  }

  const apiKey = envStr('OPENAI_API_KEY', '');
  if (!apiKey) {
    const fallback = 'Claro, dime qué necesitas y te ayudo.';
    st.lastReplyText = fallback;
    return fallback;
  }

  const recentHistory = st.history.slice(-10);
  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      temperature: 0.45,
      max_tokens: 90,
      messages: [
        { role: 'system', content: systemPrompt || 'Responde útil y breve en español.' },
        ...recentHistory,
        { role: 'user', content: userText },
        st.lastReplyText
          ? { role: 'system', content: `No repitas literalmente tu última respuesta: "${st.lastReplyText}". Di algo nuevo si es posible.` }
          : null,
      ].filter(Boolean),
    }),
  });

  if (!resp.ok) {
    const body = await resp.text().catch(()=> '');
    console.error('❌ OpenAI Chat error', resp.status, body);
    const fallback = 'Claro, dime qué necesitas y te ayudo.';
    st.lastReplyText = fallback;
    return fallback;
  }
  const data = await resp.json();
  const reply = (data?.choices?.[0]?.message?.content || '').trim() || '¿En qué más te ayudo?';
  st.lastReplyText = reply;
  return reply;
}

/* =========================
   AUDIO UTILS / Mu-law pacing
   ========================= */
const ULAW_TAB = new Int16Array(256);
(function buildUlawTable() {
  for (let i = 0; i < 256; i++) {
    let u = ~i & 0xff;
    let t = ((u & 0x0f) << 3) + 0x84;
    t <<= ((u & 0x70) >> 4);
    t -= 0x84;
    ULAW_TAB[i] = ((u & 0x80) ? (0x84 - t) : (t - 0x84));
  }
})();
function ulawRms(buf) {
  if (!buf || buf.length === 0) return 0;
  let acc = 0;
  for (let i=0;i<buf.length;i++) {
    const s = ULAW_TAB[buf[i] & 0xff];
    acc += s*s;
  }
  return Math.sqrt(acc / buf.length) / 32768;
}

function sendClear(ws, sid) {
  try { ws.send(JSON.stringify({ event: 'clear', streamSid: sid })); } catch {}
}

function createMulawSender(ws, sid, frameBytes = 160, frameMs = 20, prebufferBytes = 160*10) {
  let buf = Buffer.alloc(0);
  let running = false;
  let nextTick = 0;
  let timer = null;

  function tick() {
    if (!running) return;
    if (ws.readyState !== 1) { stop(); return; }
    if (buf.length < frameBytes || (nextTick === 0 && buf.length < Math.max(prebufferBytes, frameBytes))) {
      timer = setTimeout(tick, frameMs);
      return;
    }
    if (ws.bufferedAmount > 64 * 1024) {
      timer = setTimeout(tick, Math.min(frameMs * 2, 50));
      return;
    }
    const slice = buf.subarray(0, frameBytes);
    buf = buf.subarray(frameBytes);
    const payload = slice.toString('base64');
    try {
      ws.send(JSON.stringify({ event: 'media', streamSid: sid, media: { payload } }));
    } catch (e) {
      console.error('❌ WS send error (paced):', e?.message || e);
      stop(); return;
    }
    if (nextTick === 0) nextTick = Date.now() + frameMs; else nextTick += frameMs;
    const wait = nextTick - Date.now();
    timer = setTimeout(tick, wait > 0 ? wait : 0);
  }

  function push(data) {
    buf = Buffer.concat([buf, data]);
    if (!running) { running = true; tick(); }
  }
  function stop() {
    running = false;
    if (timer) { clearTimeout(timer); timer = null; }
  }
  return { push, stop };
}

/* =========================
   Azure TTS
   ========================= */
function buildSSMLFromText(text) {
  const style  = envStr('AZURE_TTS_STYLE', 'customerservice');
  const rate   = envStr('AZURE_TTS_RATE', '1.3');
  const pitch  = envStr('AZURE_TTS_PITCH', '+2%');
  const volume = envStr('AZURE_TTS_VOLUME', 'default');
  const pause  = envNum('AZURE_TTS_PAUSE_MS', 250);
  const voice  = envStr('AZURE_TTS_VOICE', AZURE_TTS_VOICE);

  const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const sentences = esc(text).split(/(?<=[\.\!\?…])\s+/).map(s=>s.trim()).filter(Boolean);
  const body = sentences.map((s,i)=> `${s}${i<sentences.length-1 ? ` <break time="${pause}ms"/>` : ''}`).join(' ');

  return `<?xml version="1.0" encoding="utf-8"?>
<speak version="1.0"
       xmlns="http://www.w3.org/2001/10/synthesis"
       xmlns:mstts="http://www.w3.org/2001/mstts"
       xml:lang="es-MX">
  <voice name="${voice}">
    <mstts:express-as style="${style}">
      <prosody rate="${rate}" pitch="${pitch}" volume="${volume}">
        ${body}
      </prosody>
    </mstts:express-as>
  </voice>
</speak>`;
}

function stopTTS(ws, streamSid, reason='stop') {
  const callSid = streamToCall.get(streamSid);
  const st = callSid ? statesByCall.get(callSid) : null;
  if (!st) return;
  if (reason === 'barge') {
    logToN8n({ streamSid, type: 'assistant_interrupted', reason: 'caller_barged_in', ts: new Date().toISOString() });
  }
  st.ttsActive = false;
  st.bargeStreak = 0;
  st.lastTtsStopTime = Date.now();
  if (st.ttsSender) try { st.ttsSender.stop(); } catch {}
  sendClear(ws, streamSid);
  console.log('🤫 TTS detenido (' + reason + ')', streamSid);
}

async function speakWithAzureTTS(ws, streamSid, text) {
  try {
    if (!AZURE_SPEECH_KEY || !AZURE_SPEECH_REGION) {
      console.error('❌ Falta AZURE_SPEECH_KEY o AZURE_SPEECH_REGION');
      return;
    }
    console.log('🗣️ Asistente (TTS):', text);
    const ssml = buildSSMLFromText(text);
    const url = `https://${AZURE_SPEECH_REGION}.tts.speech.microsoft.com/cognitiveservices/v1`;

    const tts = await fetch(url, {
      method: 'POST',
      headers: {
        'Ocp-Apim-Subscription-Key': AZURE_SPEECH_KEY,
        'Content-Type': 'application/ssml+xml',
        'X-Microsoft-OutputFormat': 'raw-8khz-8bit-mono-mulaw',
        'User-Agent': 'twilio-voice-assistant'
      },
      body: ssml
    });

    if (!tts.ok) {
      const errText = await tts.text().catch(()=> '');
      console.error('❌ Azure TTS error', tts.status, errText);
      return;
    }
    const callSid = streamToCall.get(streamSid);
    const st = callSid ? statesByCall.get(callSid) : null;
    if (!st) return;

    if (!st.ttsSender) st.ttsSender = createMulawSender(ws, streamSid, 160, 20, 160*10);
    sendClear(ws, streamSid);

    const buf = Buffer.from(await tts.arrayBuffer());
    if (!buf || buf.length === 0) { console.warn('⚠️ Azure TTS devolvió audio vacío'); return; }

    const CHUNK = 160;
    for (let i = 0; i < buf.length; i += CHUNK) {
      const slice = buf.subarray(i, Math.min(i + CHUNK, buf.length));
      st.ttsSender?.push(slice);
    }
    // colita de silencio (~120ms)
    const tail = Buffer.alloc(160 * 6, 0xff);
    st.ttsSender?.push(tail);

    st.lastAssistantReplySent = text;
    st.ttsActive = true;
  } catch (err) {
    console.error('❌ speakWithAzureTTS exception', err);
  }
}

/* =========================
   STATE BY CALL
   ========================= */
const statesByCall = new Map(); // callSid -> state
const streamToCall = new Map(); // streamSid -> callSid
const streams      = new Map(); // streamSid -> twilio WS
let activePrimarySid = null;

function ensureCallState(callSid) {
  if (!statesByCall.has(callSid)) {
    statesByCall.set(callSid, {
      session: {
        callerId: null,
        callSid,
        nombreCliente: null,
        numeroTelefono: null,      // ← se llenará con el número del cliente
        direccionConfirmada: false,
        modo: 'venta',
        step: 'saludo',
        menu: [],
        promos: [],
        ultimoPedido: null,
        toNumber: null,
        callerName: null,
      },
      history: [],
      lastReplyText: '',
      lastAssistantReplySent: '',
      partialBuffer: '',
      lastUserTurnHandled: '',
      lastUserTurnAnsweredText: '',
      ttsSender: null,
      ttsActive: false,
      silenceTimer: null,
      bargeStreak: 0,
      handlingTurn: false,
      lastTtsStopTime: 0,
      hasGreeted: false,
      dgSocket: null,
      stopHeartbeat: null,
      _config: null, // greeting/systemPrompt cache
    });
  }
  return statesByCall.get(callSid);
}

function normalizeSid(v) {
  if (v == null) return '';
  return String(v).replace(/^=\s*/, '').trim();
}

/* =========================
   HEARTBEAT Deepgram
   ========================= */
const SILENCE_FRAME = Buffer.alloc(160, 0xff);
function startDeepgramHeartbeat(dgSocket) {
  const HEARTBEAT_MS = 5000;
  let alive = true;
  let lastSend = Date.now();

  const loop = () => {
    if (!alive) return;
    try {
      if (Date.now() - lastSend > HEARTBEAT_MS) {
        dgSocket.send(SILENCE_FRAME);
        lastSend = Date.now();
      }
    } catch {
      alive = false; return;
    }
    setTimeout(loop, HEARTBEAT_MS);
  };
  loop();
  return () => { alive = false; };
}

/* =========================
   TURN HANDLER
   ========================= */
async function persistSnapshotFromText(st, assistantReplyText) {
  const ents = await extractEntities(st.lastUserTurnHandled || st.partialBuffer || '');
  const text = (st.lastUserTurnHandled || '').toLowerCase();
  const direccionConfirmada = st.session.direccionConfirmada || /\b(confirmo|es correcto|sí,? confirmo|sí confirmo)\b/.test(text);

  // SIEMPRE guarda el número que llama en numeroTelefono
  const snapshot = {
    callerId: st.session.callerId || st.session.callSid || null,
    nombreCliente: ents.nombreCliente ?? st.session.nombreCliente ?? null,
    numeroTelefono: st.session.callerId || st.session.numeroTelefono || null,
    ultimoPedido: ents.ultimoPedido ?? st.session.ultimoPedido ?? null,
    direccionConfirmada,
    ultimoMensajeAsistente: assistantReplyText || st.lastReplyText || null,
  };

  // aplica al estado y persiste
  st.session.nombreCliente        = snapshot.nombreCliente;
  st.session.numeroTelefono       = snapshot.numeroTelefono;
  st.session.ultimoPedido         = snapshot.ultimoPedido;
  st.session.direccionConfirmada  = snapshot.direccionConfirmada;

  await pushClientMemoryUpdate(snapshot);
}

async function handleCompleteUserTurn(twilioSocket, streamSid, st) {
  if (st.handlingTurn) return;
  st.handlingTurn = true;
  try {
    const COOLDOWN_MS = 250;
    if (st.ttsActive) return;
    if (st.lastTtsStopTime && Date.now() - st.lastTtsStopTime < COOLDOWN_MS) return;

    const humanText = (st.partialBuffer || '').trim();
    if (!humanText) return;
    if (humanText === st.lastUserTurnAnsweredText) return;

    st.history.push({ role: 'user', content: humanText });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

    logToN8n({ streamSid, type: 'user_turn_finalized', role: 'user', mensaje: humanText, ts: new Date().toISOString() });

    const intent = classifyIntent(humanText);
    let reply;

    // Intenciones de datos primero (menú/promos)
    if (intent === 'ask_menu' || intent === 'ask_promos') {
      const data = await fetchMenuAndPromos();
      if (intent === 'ask_menu') {
        st.session.menu = data.menu || [];
        const top = (st.session.menu || []).slice(0, 3).map(m => `${m.nombre} ($${Number(m.precio).toFixed(2)})`).join(', ');
        reply = top ? `Te cuento algunas opciones: ${top}. ¿Cuál prefieres?` : 'Ahora mismo no tengo menú disponible. ¿Quieres que lo intente de nuevo más tarde?';
      } else {
        st.session.promos = data.promos || [];
        const act = (st.session.promos || []).filter(p => p.activa).slice(0, 2).map(p => `${p.titulo}: ${p.descripcion}`).join(' | ');
        reply = act ? `Promos activas: ${act}. ¿Te interesa alguna?` : 'Por ahora no hay promociones activas. Igual puedo recomendarte opciones ricas del menú.';
      }
    } else if (intent === 'confirm_address') {
      st.session.direccionConfirmada = true;
      reply = 'Gracias por confirmar. ¿Deseas agregar algo más o finalizamos tu pedido?';
    } else if (intent === 'closing') {
      reply = 'Gracias por tu pedido. ¡Que tengas un excelente día! 😊';
    } else {
      if (!st._config) st._config = await fetchBotConfig();
      const sysPrompt = buildSystemPromptFromDbTemplate(st._config?.systemPrompt || '', st.session);
      reply = await answerWithOpenAI_usingSystemPrompt(st, humanText, sysPrompt);
    }

    // Envía TTS
    stopTTS(twilioSocket, streamSid, 'before_new_reply');
    await speakWithAzureTTS(twilioSocket, streamSid, reply);

    // Historial y buffers
    st.history.push({ role: 'assistant', content: reply });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
    st.lastUserTurnHandled      = humanText;
    st.lastUserTurnAnsweredText = humanText;
    st.partialBuffer            = '';
    st.lastReplyText            = reply;

    // Persistir memoria con entidades detectadas y último mensaje
    await persistSnapshotFromText(st, reply);

  } finally {
    st.handlingTurn = false;
  }
}

/* =========================
   Twilio customParameters (robusto)
   ========================= */
function readStartParam(msg, key) {
  const raw = msg?.start?.customParameters;
  if (!raw) return null;

  // Array tipo [{name,value}]
  if (Array.isArray(raw)) {
    const hit = raw.find(p => (p?.name === key) || (p?.Name === key));
    return hit?.value ?? hit?.Value ?? null;
  }
  // Objeto tipo { from:"+593...", to:"...", callerName:"..." }
  if (typeof raw === 'object') {
    return raw[key] ?? raw[key?.toLowerCase?.()] ?? raw[key?.toUpperCase?.()] ?? null;
  }
  // String tipo querystring
  if (typeof raw === 'string') {
    try {
      const params = new URLSearchParams(raw);
      return params.get(key);
    } catch {}
  }
  return null;
}

/* =========================
   WSS / Twilio
   ========================= */
const wss = new WebSocketServer({ noServer: true });
const RMS_BARGE_THRESHOLD = 0.05;
const BARGE_STREAK_FRAMES = 3;

wss.on('connection', async (twilioSocket, req) => {
  console.log('📞 Conexión de Twilio desde', req?.socket?.remoteAddress, 'url:', req?.url);
  twilioSocket.on('ping', () => { try { twilioSocket.pong(); } catch {} });
  twilioSocket.on('error', e => console.error('❌ WS client error:', e?.message || e));

  twilioSocket.on('message', async (raw) => {
    const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    let msg; try { msg = JSON.parse(str); } catch { return; }

    /* ===== START ===== */
    if (msg.event === 'start') {
      const streamSid = normalizeSid(msg?.start?.streamSid);
      const callSid   = msg?.start?.callSid || streamSid || 'desconocido';
      if (!streamSid) { console.warn('⚠️ start sin streamSid'); return; }

      activePrimarySid = streamSid;
      twilioSocket.streamSid = streamSid;
      streams.set(streamSid, twilioSocket);
      streamToCall.set(streamSid, callSid);

      const st = ensureCallState(callSid);
      console.log('▶️ start streamSid:', streamSid, 'callSid:', callSid, '| streams activos:', streams.size);

      // Lee parámetros robustamente
      const fromParam = readStartParam(msg, 'from');        // número del cliente E.164
      const toParam   = readStartParam(msg, 'to');          // número de tu línea Twilio
      const nameParam = readStartParam(msg, 'callerName');  // opcional

      // Fallback por si Twilio no mandó customParameters
      const fallbackFrom = msg.start?.from || msg.start?.caller || null;

      // Guardar en sesión
      st.session.callerId       = toNullIfEmpty(fromParam) || toNullIfEmpty(fallbackFrom) || callSid || 'desconocido';
      st.session.numeroTelefono = st.session.callerId; // persistiremos este número
      st.session.toNumber       = toNullIfEmpty(toParam) || st.session.toNumber || null;
      st.session.callerName     = toNullIfEmpty(nameParam) || st.session.callerName || null;

      st.bargeStreak = 0;

      // Reusar DG si ya existe en la llamada; si no, crear
      if (!st.dgSocket) {
        const dgSocket = await deepgram.listen.live({
          model: 'nova-3',
          language: 'es',
          punctuate: true,
          encoding: 'mulaw',
          sample_rate: 8000,
          interim_results: true,
          vad_events: true,
          endpointing: envStr('DG_ENDPOINTING', '200'),
          smart_format: false,
        });
        st.dgSocket = dgSocket;

        dgSocket.on(LiveTranscriptionEvents.Open, async () => {
          console.log('🎤 DG abierto callSid:', callSid, 'streamSid:', streamSid);
          st.stopHeartbeat = startDeepgramHeartbeat(dgSocket);

          // Precarga inicial (una sola vez por llamada)
          if (!st._config) {
            st._config = await fetchBotConfig();
          }
          if ((st.session.menu?.length ?? 0) === 0 && (st.session.promos?.length ?? 0) === 0) {
            try {
              const [memData, menuData] = await Promise.all([
                fetchClientMemory(st.session.callerId),
                fetchMenuAndPromos(),
              ]);
              if (memData && typeof memData === 'object') {
                if (memData.nombreCliente)        st.session.nombreCliente        = memData.nombreCliente;
                if (memData.numeroTelefono)       st.session.numeroTelefono       = memData.numeroTelefono;
                if (memData.direccionConfirmada)  st.session.direccionConfirmada  = !!memData.direccionConfirmada;
                if (memData.ultimoPedido)         st.session.ultimoPedido         = memData.ultimoPedido;
              }
              st.session.menu   = menuData?.menu   || [];
              st.session.promos = menuData?.promos || [];
            } catch (e) { console.error('precarga error:', e?.message || e); }
          }

          // Saludo desde BD
          if (!st.hasGreeted) {
            const greetingTpl = st._config?.greeting ;
            const saludo = renderTemplate(greetingTpl, {
              callerId: st.session.callerId,
              nombreCliente: st.session.nombreCliente,
              numeroTelefono: st.session.numeroTelefono || st.session.callerId,
              direccionConfirmada: st.session.direccionConfirmada ? 'sí' : 'no'
            });

            logToN8n({ streamSid, type: 'assistant_auto_greeting', role: 'assistant', mensaje: saludo, ts: new Date().toISOString() });

            st.history.push({ role: 'assistant', content: saludo });
            if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

            st.lastReplyText = saludo;
            st.lastAssistantReplySent = saludo;
            st.session.step = 'en_conversacion';
            st.hasGreeted = true;

            stopTTS(twilioSocket, streamSid, 'before_greeting');
            await speakWithAzureTTS(twilioSocket, streamSid, saludo);
          }
        });

        dgSocket.on(LiveTranscriptionEvents.Transcript, async (data) => {
          if (streamSid !== activePrimarySid) return;
          const alt = data?.channel?.alternatives?.[0];
          const rawTxt = (alt?.transcript || '').trim();
          if (!rawTxt) return;

          const isFinal = data?.is_final || data?.speech_final;
          const normalized = rawTxt.replace(/\s+/g, ' ').trim();
          st.partialBuffer = normalized;

          console.log(isFinal ? '🗣️ Cliente (final):' : '🗣️ Cliente (parcial):', normalized);
          logToN8n({ streamSid, type: isFinal ? 'user_final_raw' : 'user_partial', role: 'user', mensaje: normalized, ts: new Date().toISOString() });

          if (isFinal) {
            if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
            await handleCompleteUserTurn(twilioSocket, streamSid, st);
          }
        });

        dgSocket.on(LiveTranscriptionEvents.Error, (err) => console.error('❌ Deepgram:', err));
        dgSocket.on(LiveTranscriptionEvents.Close, () => console.log('📴 Deepgram Close (espera hangup) callSid:', callSid));
      } else {
        console.log('🔁 Reusando DG/estado para callSid', callSid, 'nuevo streamSid', streamSid);
      }
      return;
    }

    /* ===== MEDIA ===== */
    if (msg.event === 'media') {
      const streamSid = twilioSocket.streamSid;
      if (!streamSid || streamSid !== activePrimarySid) return;

      const callSid = streamToCall.get(streamSid);
      const st = ensureCallState(callSid);

      const track = msg?.track || 'inbound';
      if (track !== 'inbound') return;

      const payload   = msg.media?.payload || '';
      const audioUlaw = Buffer.from(payload, 'base64');
      if (audioUlaw.length === 0) return;

      // Barge-in
      const vadInfoOpenLike = processFrame(streamSid, audioUlaw);
      const callerTalking   = !!vadInfoOpenLike.open;
      if (st.ttsActive) {
        if (callerTalking) {
          st.bargeStreak = (st.bargeStreak || 0) + 1;
        } else {
          const frameRms = vadInfoOpenLike.rms ?? ulawRms(audioUlaw);
          if (frameRms >= RMS_BARGE_THRESHOLD) st.bargeStreak = (st.bargeStreak || 0) + 1;
          else st.bargeStreak = 0;
        }
        if (st.bargeStreak >= BARGE_STREAK_FRAMES) {
          stopTTS(twilioSocket, streamSid, 'barge');
          st.bargeStreak = 0;
        }
      } else {
        st.bargeStreak = 0;
      }

      // enviar a Deepgram
      if (st.dgSocket) {
        try { st.dgSocket.send(audioUlaw); } catch (e) {
          console.error('❌ DG send error', e?.message || e);
        }
      }
      return;
    }

    /* ===== STOP ===== */
    if (msg.event === 'stop') {
      const streamSid = twilioSocket.streamSid;
      const callSid   = streamToCall.get(streamSid);
      console.log('⏹️ Twilio stop', streamSid, 'callSid', callSid);

      streams.delete(streamSid);
      streamToCall.delete(streamSid);

      if (streamSid === activePrimarySid) {
        stopTTS(twilioSocket, streamSid, 'stop');
        const st = callSid ? statesByCall.get(callSid) : null;
        if (st) {
          await pushClientMemoryUpdate({
            callerId: st.session.callerId || null,
            nombreCliente: st.session.nombreCliente || null,
            numeroTelefono: st.session.callerId || st.session.numeroTelefono || null,
            ultimoPedido: st.session.ultimoPedido || null,
            direccionConfirmada: !!st.session.direccionConfirmada,
            ultimoMensajeAsistente: st.lastReplyText || null,
            endedAt: new Date().toISOString()
          });
          if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
          if (st.dgSocket) { try { st.dgSocket.finish(); } catch {} st.dgSocket = null; }
          if (st.stopHeartbeat) { try { st.stopHeartbeat(); } catch {} st.stopHeartbeat = null; }
          statesByCall.delete(callSid);
        }
        activePrimarySid = null;
      }
      clearVadState(streamSid);
      return;
    }
  });

  twilioSocket.on('close', async () => {
    const streamSid = twilioSocket.streamSid;
    const callSid   = streamToCall.get(streamSid);
    console.log('❌ Twilio cerró la conexión', streamSid, 'callSid', callSid);

    streams.delete(streamSid);
    streamToCall.delete(streamSid);

    if (streamSid !== activePrimarySid) {
      clearVadState(streamSid);
      return; // socket viejo
    }

    stopTTS(twilioSocket, streamSid, 'close');
    const st = callSid ? statesByCall.get(callSid) : null;
    if (st) {
      await pushClientMemoryUpdate({
        callerId: st.session.callerId || null,
        nombreCliente: st.session.nombreCliente || null,
        numeroTelefono: st.session.callerId || st.session.numeroTelefono || null,
        ultimoPedido: st.session.ultimoPedido || null,
        direccionConfirmada: !!st.session.direccionConfirmada,
        ultimoMensajeAsistente: st.lastReplyText || null,
        endedAt: new Date().toISOString()
      });
      if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
      if (st.dgSocket) { try { st.dgSocket.finish(); } catch {} st.dgSocket = null; }
      if (st.stopHeartbeat) { try { st.stopHeartbeat(); } catch {} st.stopHeartbeat = null; }
      statesByCall.delete(callSid);
    }
    clearVadState(streamSid);
    if (streamSid === activePrimarySid) activePrimarySid = null;
  });
});

/* =========================
   KEEPALIVE WSS
   ========================= */
const PING_EVERY_MS = 25000;
setInterval(() => {
  for (const ws of wss.clients) { try { ws.ping(); } catch {} }
}, PING_EVERY_MS);

wss.on('error', (err) => console.error('❌ WSS error:', err));

/* =========================
   HTTP + UPGRADE
   ========================= */
app.server = app.listen(port, () => {
  console.log(`🚀 Servidor escuchando en http://localhost:${port}`);
});
app.server.on('upgrade', (req, socket, head) => {
  if (!req.url || !req.url.startsWith('/twilio')) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

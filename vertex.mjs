// index.mjs — Asistente de Voz (Twilio WS) + Deepgram STT + Google Cloud TTS + n8n (BD)
// TwiML: <Connect><Stream track="both_tracks" url="wss://TU_HOST/twilio" /></Connect>

import express from 'express';
import path from 'path';
import { WebSocketServer } from 'ws';
import { createClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import { processFrame, clearVadState } from './vad.mjs';

// === Google Cloud TTS ===
import textToSpeech from '@google-cloud/text-to-speech';
import fs from 'fs';
import os from 'os';

dotenv.config();

/* =========================
   AYUDAS DE ENTORNO
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
   SERVICIOS EXTERNOS
   ========================= */
const deepgram = createClient(envStr('DEEPGRAM_API_KEY', ''));

// === Config Google TTS ===
const GOOGLE_TTS_VOICE      = envStr('GOOGLE_TTS_VOICE', 'es-US-Chirp3-HD-Zephyr'); // Femenino
const GOOGLE_TTS_LANGUAGE   = envStr('GOOGLE_TTS_LANGUAGE', 'es-US');
const GOOGLE_TTS_RATE       = envStr('GOOGLE_TTS_RATE', '1.15'); // 0.25–4.0
const GOOGLE_TTS_PITCH      = envStr('GOOGLE_TTS_PITCH', '0.0'); // -20.0–20.0 semitonos aprox
const GOOGLE_TTS_EFFECTS    = envStr('GOOGLE_TTS_EFFECTS', 'telephony-class-application'); // perfíl para telefonía

// Soporte credenciales por base64 (opcional)
let gAuthClient = null;
{
  const keyB64 = process.env.GOOGLE_CLOUD_KEY_BASE64 || '';
  if (keyB64) {
    try {
      const json = Buffer.from(keyB64, 'base64').toString('utf8');
      const tmp = path.join(os.tmpdir(), `gcp-key-${Date.now()}.json`);
      fs.writeFileSync(tmp, json);
      process.env.GOOGLE_APPLICATION_CREDENTIALS = tmp;
    } catch (e) {
      console.warn('⚠️ No se pudo decodificar GOOGLE_CLOUD_KEY_BASE64:', e?.message || e);
    }
  }
}
const gTtsClient = new textToSpeech.TextToSpeechClient();

/* =========================
   n8n
   ========================= */
const N8N_BASE_URL      = envStr('N8N_BASE_URL', '');
const N8N_SHARED_SECRET = envStr('N8N_SHARED_SECRET', 'pizzeriadonnapoliSUPERSECRETO');
const N8N_WEBHOOK_URL   = envStr('N8N_WEBHOOK_URL', ''); // opcional para logs

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

/* ===== Helpers explícitos para menú y promos (desde BD vía n8n) ===== */
async function fetchMenuOnly() {
  const out = await callN8n({ action: 'get_menu' });
  const menu = Array.isArray(out?.menu) ? out.menu : (Array.isArray(out) ? out : []);
  return menu.map(r => ({
    id: r.id,
    nombre: r.nombre,
    descripcion: r.descripcion,
    precio: Number(r.precio),
    disponible: !!r.disponible
  }));
}

async function fetchPromosOnly() {
  const out = await callN8n({ action: 'get_promos' });
  const promos = Array.isArray(out?.promos) ? out.promos : (Array.isArray(out) ? out : []);
  return promos.map(r => ({
    id: r.id,
    titulo: r.titulo,
    descripcion: r.descripcion,
    activa: !!r.activa
  }));
}

async function fetchMenuAndPromos() {
  const [menu, promos] = await Promise.all([
    fetchMenuOnly().catch(()=>[]),
    fetchPromosOnly().catch(()=>[])
  ]);
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
   PLANTILLAS Y PROMPTS
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

function buildSystemPromptFromDbTemplate(systemPromptTpl, session, hint = '') {
  const ctx = {
    callerId: session.callerId,
    nombreCliente: session.nombreCliente,
    numeroTelefono: session.numeroTelefono || session.callerId,
    direccionConfirmada: session.direccionConfirmada ? 'sí' : 'no',
  };
  let prompt = renderTemplate(systemPromptTpl, ctx);
  const menuStr   = JSON.stringify(session.menu ?? []);
  const promosStr = JSON.stringify(session.promos ?? []);
  prompt += `

Menú (JSON):
${menuStr}

Promociones (JSON):
${promosStr}
`.trim();

  if (hint) {
    prompt += `

Instrucción de turno:
${hint}`.trim();
  }
  return prompt;
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
   EXTRACCIÓN DE ENTIDADES
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
    const pizza  = userText.match(/\b(pizza\s+(?:de\s+)?[a-záéíóúñ]+|pepperoni|margarita|hawaiana)\b/i);
    const extra = [];
    if (pizza) extra.push(pizza[0]);
    if (bebida) extra.push(bebida[0]);
    if (extra.length) out.ultimoPedido = extra.join(' y ');
  }

  out.nombreCliente = toNullIfEmpty(out.nombreCliente);
  out.ultimoPedido  = toNullIfEmpty(out.ultimoPedido);
  return out;
}

/* =========================
   OPENAI CHAT
   ========================= */
async function answerWithOpenAI_usingSystemPrompt(st, userText, systemPrompt) {
  if (isNoiseUtterance(userText)) {
    const canned = (st.session.step !== 'saludo')
      ? 'Sí, te escucho claro. ¿Qué te gustaría pedir?'
      : 'Pizza Nostra, hola. ¿Qué te gustaría pedir hoy?';
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
      max_tokens: 120,
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
   AUDIO / pacing µ-law
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
   GOOGLE CLOUD TTS
   ========================= */

// Construye SSML simple con pausas cortas entre oraciones (opcional)
function buildGoogleSSMLFromText(text) {
  const pauseMs = envNum('GOOGLE_TTS_PAUSE_MS', 220);
  const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const sentences = esc(text).split(/(?<=[\.\!\?…:;])\s+/).map(s=>s.trim()).filter(Boolean);
  const body = sentences.map((s,i)=> `${s}${i<sentences.length-1 ? ` <break time="${pauseMs}ms"/>` : ''}`).join(' ');
  return `<speak>${body}</speak>`;
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

async function speakWithGoogleTTS(ws, streamSid, text) {
  try {
    // Prepara estado
    const callSid = streamToCall.get(streamSid);
    const st = callSid ? statesByCall.get(callSid) : null;
    if (!st) return;

    console.log('🗣️ Asistente (Google TTS):', text);

    // Construimos SSML o TEXT (prefiero SSML para pausas)
    const useSSML = true;
    const input = useSSML ? { ssml: buildGoogleSSMLFromText(text) } : { text };

    const request = {
      input,
      voice: {
        languageCode: GOOGLE_TTS_LANGUAGE,
        name: GOOGLE_TTS_VOICE, // p.ej. "es-US-Chirp3-HD-Zephyr"
        // ssmlGender no es necesario si se usa name
      },
      audioConfig: {
        audioEncoding: 'MULAW',
        sampleRateHertz: 8000,
        speakingRate: Number(GOOGLE_TTS_RATE),
        pitch: Number(GOOGLE_TTS_PITCH),
        effectsProfileId: GOOGLE_TTS_EFFECTS ? [GOOGLE_TTS_EFFECTS] : []
      },
    };

    const [resp] = await gTtsClient.synthesizeSpeech(request);
    const audio = resp?.audioContent ? Buffer.from(resp.audioContent, 'base64') : null;
    if (!audio || audio.length === 0) {
      console.warn('⚠️ Google TTS devolvió audio vacío'); return;
    }

    if (!st.ttsSender) st.ttsSender = createMulawSender(ws, streamSid, 160, 20, 160*10);
    sendClear(ws, streamSid);

    const CHUNK = 160;
    for (let i = 0; i < audio.length; i += CHUNK) {
      const slice = audio.subarray(i, Math.min(i + CHUNK, audio.length));
      st.ttsSender?.push(slice);
    }
    // pequeña cola de silencio (~120ms)
    const tail = Buffer.alloc(160 * 6, 0xff);
    st.ttsSender?.push(tail);

    st.lastAssistantReplySent = text;
    st.ttsActive = true;
  } catch (err) {
    console.error('❌ speakWithGoogleTTS exception', err?.message || err);
  }
}

/* =========================
   ESTADO POR LLAMADA
   ========================= */
const statesByCall = new Map(); // callSid -> estado
const streamToCall = new Map(); // streamSid -> callSid
const streams      = new Map(); // streamSid -> socket Twilio
let activePrimarySid = null;

function ensureCallState(callSid) {
  if (!statesByCall.has(callSid)) {
    statesByCall.set(callSid, {
      session: {
        callerId: null,
        callSid,
        nombreCliente: null,
        numeroTelefono: null,
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
      _config: null,
    });
  }
  return statesByCall.get(callSid);
}

function normalizeSid(v) {
  if (v == null) return '';
  return String(v).replace(/^=\s*/, '').trim();
}

/* =========================
   HEARTBEAT PARA DEEPGRAM
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
   MANEJO DE TURNOS
   ========================= */
async function persistSnapshotFromText(st, assistantReplyText) {
  const ents = await extractEntities(st.lastUserTurnHandled || st.partialBuffer || '');
  const text = (st.lastUserTurnHandled || '').toLowerCase();
  const direccionConfirmada = st.session.direccionConfirmada || /\b(confirmo|es correcto|sí,? confirmo|sí confirmo)\b/.test(text);

  const snapshot = {
    callerId: st.session.callSid || st.session.callerId || null,
    nombreCliente: ents.nombreCliente ?? st.session.nombreCliente ?? null,
    numeroTelefono: st.session.callerId || st.session.numeroTelefono || null,
    ultimoPedido: ents.ultimoPedido ?? st.session.ultimoPedido ?? null,
    direccionConfirmada,
    ultimoMensajeAsistente: assistantReplyText || st.lastReplyText || null,
  };

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

    // Solo hint → LLM decide; sin textos estáticos
    let hint = '';
    if (intent === 'ask_menu') {
      st.session.menu = await fetchMenuOnly();
      hint = 'El usuario pidió MENÚ: usa 3–5 opciones con nombre y precio desde el JSON Menú. No inventes.';
    } else if (intent === 'ask_promos') {
      st.session.promos = await fetchPromosOnly();
      hint = 'El usuario pidió PROMOCIONES: usa 1–2 activas del JSON Promociones. No inventes.';
    } else if (intent === 'confirm_address') {
      st.session.direccionConfirmada = true;
      hint = 'El usuario confirmó dirección: continúa el flujo de cierre según reglas.';
    } else if (intent === 'closing') {
      hint = 'El usuario está cerrando: confirma pedido final y despídete cordialmente.';
    }

    if (!st._config) st._config = await fetchBotConfig();
    const sysPrompt = buildSystemPromptFromDbTemplate(st._config?.systemPrompt || '', st.session, hint);

    const reply = await answerWithOpenAI_usingSystemPrompt(st, humanText, sysPrompt);

    // Enviar TTS con Google
    stopTTS(twilioSocket, streamSid, 'before_new_reply');
    await speakWithGoogleTTS(twilioSocket, streamSid, reply);

    st.history.push({ role: 'assistant', content: reply });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
    st.lastUserTurnHandled      = humanText;
    st.lastUserTurnAnsweredText = humanText;
    st.partialBuffer            = '';
    st.lastReplyText            = reply;

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
  if (Array.isArray(raw)) {
    const hit = raw.find(p => (p?.name === key) || (p?.Name === key));
    return hit?.value ?? hit?.Value ?? null;
  }
  if (typeof raw === 'object') {
    return raw[key] ?? raw[key?.toLowerCase?.()] ?? raw[key?.toUpperCase?.()] ?? null;
  }
  if (typeof raw === 'string') {
    try {
      const params = new URLSearchParams(raw);
      return params.get(key);
    } catch {}
  }
  return null;
}

/* =========================
   WEBSOCKET / Twilio
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

      const fromParam = readStartParam(msg, 'from');
      const toParam   = readStartParam(msg, 'to');
      const nameParam = readStartParam(msg, 'callerName');
      const fallbackFrom = msg.start?.from || msg.start?.caller || null;

      st.session.callerId       = toNullIfEmpty(fromParam) || toNullIfEmpty(fallbackFrom) || callSid || 'desconocido';
      st.session.numeroTelefono = st.session.callerId;
      st.session.toNumber       = toNullIfEmpty(toParam) || st.session.toNumber || null;
      st.session.callerName     = toNullIfEmpty(nameParam) || st.session.callerName || null;

      st.bargeStreak = 0;

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

          if (!st._config) {
            st._config = await fetchBotConfig();
          }
          if ((st.session.menu?.length ?? 0) === 0 && (st.session.promos?.length ?? 0) === 0) {
            try {
              const [memData, data] = await Promise.all([
                fetchClientMemory(st.session.callerId),
                fetchMenuAndPromos(),
              ]);
              if (memData && typeof memData === 'object') {
                if (memData.nombreCliente)        st.session.nombreCliente        = memData.nombreCliente;
                if (memData.numeroTelefono)       st.session.numeroTelefono       = memData.numeroTelefono;
                if (memData.direccionConfirmada)  st.session.direccionConfirmada  = !!memData.direccionConfirmada;
                if (memData.ultimoPedido)         st.session.ultimoPedido         = memData.ultimoPedido;
              }
              st.session.menu   = data?.menu   || [];
              st.session.promos = data?.promos || [];
            } catch (e) { console.error('precarga error:', e?.message || e); }
          }

          // Saludo inicial desde BD (Google TTS)
          if (!st.hasGreeted) {
            const greetingTpl = st._config?.greeting || '¡Hola! Hablas con Martina de Pizza Nostra. ¿Qué te gustaría pedir hoy?';
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
            await speakWithGoogleTTS(twilioSocket, streamSid, saludo);
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

      // enviar audio a Deepgram
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
            callerId: st.session.callSid || null,
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
        callerId: st.session.callSid || null,
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
   KEEPALIVE DEL WSS
   ========================= */
const PING_EVERY_MS = 25000;
setInterval(() => {
  for (const ws of wss.clients) { try { ws.ping(); } catch {} }
}, PING_EVERY_MS);

wss.on('error', (err) => console.error('❌ WSS error:', err));

/* =========================
   HTTP + UPGRADE A WS
   ========================= */
app.server = app.listen(port, () => {
  console.log(`🚀 Servidor escuchando en http://localhost:${port}`);
});
app.server.on('upgrade', (req, socket, head) => {
  if (!req.url || !req.url.startsWith('/twilio')) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  a
});

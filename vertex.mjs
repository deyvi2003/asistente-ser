// index.mjs — Asistente de Voz (Twilio WS) + Deepgram STT + Google Cloud TTS + n8n (BD)
// Mejora de latencia: LLM streaming por frases -> TTS inmediato, prompt SLIM, endpointing 300ms
// TwiML: <Connect><Stream track="both_tracks" url="wss://TU_HOST/twilio" /></Connect>

import express from 'express';
import path from 'path';
import { WebSocketServer } from 'ws';
import { createClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import dotenv from 'dotenv';
const fetch = globalThis.fetch; // Node 18+
import { processFrame, clearVadState } from './vad.mjs';

// === Google Cloud TTS ===
import textToSpeech from '@google-cloud/text-to-speech';
import fs from 'fs';
import os from 'os';

dotenv.config();

/* =========================
   LOGGING (niveles + helpers)
   ========================= */
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase(); // debug|info|warn|error
const LOG_MEDIA = String(process.env.LOG_MEDIA || '0') === '1';     // cuenta frames media
const LOG_SSE_CHUNKS = String(process.env.LOG_SSE_CHUNKS || '0') === '1';
const LOG_N8N_BODIES = String(process.env.LOG_N8N_BODIES || '0') === '1';

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const CUR = LEVELS[LOG_LEVEL] ?? 1;
const ts = () => new Date().toISOString().replace('T', ' ').replace('Z', '');

const LOG = {
  debug: (...a) => { if (CUR <= 0) console.log('🟦', ts(), ...a); },
  info:  (...a) => { if (CUR <= 1) console.log('🟩', ts(), ...a); },
  warn:  (...a) => { if (CUR <= 2) console.warn('🟨', ts(), ...a); },
  error: (...a) => { if (CUR <= 3) console.error('🟥', ts(), ...a); },
};

LOG.info('[boot] LOG_LEVEL=%s | LOG_MEDIA=%s | LOG_SSE_CHUNKS=%s | LOG_N8N_BODIES=%s',
  LOG_LEVEL, LOG_MEDIA ? 'on' : 'off', LOG_SSE_CHUNKS ? 'on' : 'off', LOG_N8N_BODIES ? 'on' : 'off');

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
const GOOGLE_TTS_VOICE    = envStr('GOOGLE_TTS_VOICE', 'es-US-Chirp3-HD-Zephyr');
const GOOGLE_TTS_LANGUAGE = envStr('GOOGLE_TTS_LANGUAGE', 'es-US');
const GOOGLE_TTS_RATE     = envStr('GOOGLE_TTS_RATE', '1.10');
const GOOGLE_TTS_PITCH    = envStr('GOOGLE_TTS_PITCH', '0.0');
const GOOGLE_TTS_EFFECTS  = envStr('GOOGLE_TTS_EFFECTS', 'telephony-class-application');
// ↓ más corto por defecto para respuesta rápida
const GOOGLE_TTS_PAUSE_MS = envNum('GOOGLE_TTS_PAUSE_MS', 80);

LOG.info('[config] PORT=%d | TTS voice=%s rate=%s pitch=%s', port, GOOGLE_TTS_VOICE, GOOGLE_TTS_RATE, GOOGLE_TTS_PITCH);

/* ---------- Resolución robusta de credenciales Google ---------- */
function setupGoogleCreds() {
  const keyB64 = (process.env.GOOGLE_CLOUD_KEY_BASE64 || '').trim();
  const keyJsonInline = (process.env.GOOGLE_CLOUD_KEY_JSON || '').trim();
  let credPath = (process.env.GOOGLE_APPLICATION_CREDENTIALS || '').trim();

  if (keyB64) {
    try {
      const json = Buffer.from(keyB64, 'base64').toString('utf8');
      const tmp = path.join(os.tmpdir(), `gcp-key-${Date.now()}.json`);
      fs.writeFileSync(tmp, json, { encoding: 'utf8', mode: 0o600 });
      process.env.GOOGLE_APPLICATION_CREDENTIALS = tmp;
      LOG.info('🔐 Google creds: usando GOOGLE_CLOUD_KEY_BASE64 (temp file)');
      return tmp;
    } catch (e) {
      LOG.warn('⚠️ No se pudo decodificar GOOGLE_CLOUD_KEY_BASE64:', e?.message || e);
    }
  }

  if (keyJsonInline && keyJsonInline.startsWith('{')) {
    try {
      const tmp = path.join(os.tmpdir(), `gcp-key-${Date.now()}.json`);
      fs.writeFileSync(tmp, keyJsonInline, { encoding: 'utf8', mode: 0o600 });
      process.env.GOOGLE_APPLICATION_CREDENTIALS = tmp;
      LOG.info('🔐 Google creds: usando GOOGLE_CLOUD_KEY_JSON (temp file)');
      return tmp;
    } catch (e) {
      LOG.warn('⚠️ No se pudo escribir GOOGLE_CLOUD_KEY_JSON:', e?.message || e);
    }
  }

  if (credPath) {
    if (!path.isAbsolute(credPath)) credPath = path.join(process.cwd(), credPath);
    if (fs.existsSync(credPath)) {
      process.env.GOOGLE_APPLICATION_CREDENTIALS = credPath;
      LOG.info('🔐 Google creds: usando archivo en %s', credPath);
      return credPath;
    } else {
      LOG.warn('⚠️ GOOGLE_APPLICATION_CREDENTIALS apunta a un archivo inexistente: %s', credPath);
    }
  }

  LOG.warn('⚠️ No se detectaron credenciales de Google. TTS fallará con UNAUTHENTICATED.');
  return null;
}
setupGoogleCreds();

// Cliente TTS
const gTtsClient = new textToSpeech.TextToSpeechClient(
  process.env.GOOGLE_APPLICATION_CREDENTIALS
    ? { keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS }
    : {}
);

/* =========================
   n8n
   ========================= */
const N8N_BASE_URL      = envStr('N8N_BASE_URL', '');
const N8N_SHARED_SECRET = envStr('N8N_SHARED_SECRET', 'pizzeriadonnapoliSUPERSECRETO');
const N8N_WEBHOOK_URL   = envStr('N8N_WEBHOOK_URL', ''); // opcional para logs

async function callN8n(payloadObj) {
  if (!N8N_BASE_URL) {
    LOG.warn('⚠️ N8N_BASE_URL vacío');
    return null;
  }
  try {
    LOG.debug('🌐 [n8n:req] POST %s body=%o', N8N_BASE_URL, payloadObj);
    const t0 = Date.now();
    const res = await fetch(N8N_BASE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-shared-secret': N8N_SHARED_SECRET,
      },
      body: JSON.stringify(payloadObj),
    });
    const text = await res.text();
    const dt = Date.now() - t0;
    if (!res.ok) {
      LOG.error('❌ [n8n:res] %s %dms status=%d body=%s', N8N_BASE_URL, dt, res.status, text?.slice(0, 400));
      return null;
    }
    LOG.info('✅ [n8n:res] %s %dms status=%d', N8N_BASE_URL, dt, res.status);
    if (LOG_N8N_BODIES) LOG.debug('📦 [n8n:body] %s', text?.slice(0, 2000));
    try { return JSON.parse(text); } catch { return text; }
  } catch (e) {
    LOG.error('⚠️ callN8n exception: %s', e?.message || e);
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
  LOG.debug('🧠 memoria n8n -> %o', out);
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

/* ===== Helpers menú/promos ===== */
async function fetchMenuOnly() {
  const out = await callN8n({ action: 'get_menu' });
  const menu = Array.isArray(out?.menu) ? out.menu : (Array.isArray(out) ? out : []);
  LOG.info('🍕 menú recibido: %d items', menu.length);
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
  LOG.info('🎁 promos recibidas: %d items', promos.length);
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
  LOG.debug('📝 update_memory -> %o', s);
  await callN8n({ action: 'update_memory', ...s });
}

async function fetchBotConfig() {
  const out = await callN8n({ action: 'get_config' });
  LOG.info('⚙️ config n8n recibida');
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
    const [pathStr, defRaw] = expr.split('|').map(s => s?.trim());
    const def = defRaw ?? '';
    const v = pathStr?.split('.').reduce((acc, k) =>
      (acc && acc[k] !== undefined) ? acc[k] : undefined, ctx);
    return (v === undefined || v === null || v === '') ? def : String(v);
  });
}

// SLIM prompt: top N y campos mínimos para reducir tokens/latencia
function buildSystemPromptFromDbTemplate(systemPromptTpl, session, hint = '') {
  const ctx = {
    callerId: session.callerId,
    nombreCliente: session.nombreCliente,
    numeroTelefono: session.numeroTelefono || session.callerId,
    direccionConfirmada: session.direccionConfirmada ? 'sí' : 'no',
  };
  let prompt = renderTemplate(systemPromptTpl, ctx);

  const menuSlim = (session.menu || [])
    .filter(i => i?.disponible)
    .slice(0, 8)
    .map(({ nombre, precio }) => ({ nombre, precio }));

  const promosSlim = (session.promos || [])
    .filter(p => p?.activa)
    .slice(0, 3)
    .map(({ titulo, descripcion }) => ({ titulo, descripcion }));

  prompt += `

Menú (máximo 8, nombre y precio):
${JSON.stringify(menuSlim)}

Promociones (máximo 3):
${JSON.stringify(promosSlim)}
`.trim();

  if (hint) {
    prompt += `

Instrucción de turno:
${hint}`.trim();
  }
  LOG.debug('📐 systemPrompt length=%d', prompt.length);
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

/* === Speech token === */
function newSpeechToken() {
  return `${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
}

/* =========================
   EXTRACCIÓN DE ENTIDADES
   ========================= */
async function extractEntities(userText) {
  const out = { nombreCliente: null, ultimoPedido: null };
  const m1 = userText.match(/\b(me llamo|soy)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)/i);
  if (m1) out.nombreCliente = m1[2].trim();

  const pedidoRegex = /\b(quiero|deme|pon|me das|me pones|me agregas|una|un)\s+([a-zÁÉÍÓÚÑáéíóúñ0-9\s\-\.,]{4,})/i;
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
  LOG.debug('🔎 entidades extraídas: %o', out);
  return out;
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

function createMulawSender(ws, sid, frameBytes = 160, frameMs = 20, prebufferBytes = 160*20) {
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
      LOG.error('❌ WS send error (paced): %s', e?.message || e);
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
function buildGoogleSSMLFromText(text) {
  const pauseMs = GOOGLE_TTS_PAUSE_MS;
  const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const sentences = esc(text).split(/(?<=[\.\!\?…:;])\s+/).map(s=>s.trim()).filter(Boolean);
  const body = sentences.map((s,i)=> `${s}${i<sentences.length-1 ? ` <break time="${pauseMs}ms"/>` : ''}`).join(' ');
  return `<speak>${body}</speak>`;
}

async function synthesizeGoogle(text) {
  const input = { ssml: buildGoogleSSMLFromText(text) };
  const request = {
    input,
    voice: { languageCode: GOOGLE_TTS_LANGUAGE, name: GOOGLE_TTS_VOICE },
    audioConfig: {
      audioEncoding: 'MULAW',
      sampleRateHertz: 8000,
      speakingRate: Number(GOOGLE_TTS_RATE),
      pitch: Number(GOOGLE_TTS_PITCH),
      effectsProfileId: GOOGLE_TTS_EFFECTS ? [GOOGLE_TTS_EFFECTS] : []
    },
  };
  const t0 = Date.now();
  const [resp] = await gTtsClient.synthesizeSpeech(request);
  const dt = Date.now() - t0;
  const audio = resp?.audioContent ? Buffer.from(resp.audioContent, 'base64') : null;
  LOG.info('🔊 TTS synth len=%s bytes tiempo=%dms', audio?.length ?? 0, dt);
  return audio ?? null;
}

function pushAudioToCaller(st, ws, streamSid, audioBuf) {
  if (!audioBuf || audioBuf.length === 0) return;
  if (!st.ttsSender) st.ttsSender = createMulawSender(ws, streamSid, 160, 20, 160*20);
  if (!st._ttsClearedThisTurn) {
    sendClear(ws, streamSid);
    st._ttsClearedThisTurn = true;
    LOG.debug('🧽 clear audio buffer (inicio de turno) %s', streamSid);
  }
  const CHUNK = 160;
  LOG.debug('📤 enviando audio a Twilio: %d bytes (~%d ms)', audioBuf.length, Math.round(audioBuf.length / 160 * 20));
  for (let i = 0; i < audioBuf.length; i += CHUNK) {
    const slice = audioBuf.subarray(i, Math.min(i + CHUNK, audioBuf.length));
    st.ttsSender?.push(slice);
  }
  const tail = Buffer.alloc(160 * 8, 0xff);
  st.ttsSender?.push(tail);
}

function resetTtsQueue(st) {
  st._ttsQueue = [];
  st._ttsBusy = false;
  st._ttsClearedThisTurn = false;
  LOG.debug('🧺 TTS queue reset');
}

/* =========================
   SINGLE-SHOT TTS QUEUE (descarta audio viejo por token)
   ========================= */
async function processTtsQueue(st, ws, streamSid) {
  if (st._ttsBusy) return;
  const item = st._ttsQueue?.shift();
  if (!item) return;

  st._ttsBusy = true;
  LOG.debug('⏳ TTS dequeued (%d chars) token=%s', item.text.length, item.token);
  try {
    const audio = await synthesizeGoogle(item.text);
    if (!audio) return;
    if (item.token !== st.speechToken) {
      LOG.debug('🪓 audio descartado por token viejo (%s != %s)', item.token, st.speechToken);
      return;
    }
    pushAudioToCaller(st, ws, streamSid, audio);
  } catch (e) {
    LOG.error('❌ TTS cola (sintetizar) error: %s', e?.message || e);
  } finally {
    st._ttsBusy = false;
    if (st._ttsQueue && st._ttsQueue.length > 0) processTtsQueue(st, ws, streamSid);
  }
}

function enqueueTts(st, ws, streamSid, text) {
  if (!text || !text.trim()) return;
  const t = text.trim().replace(/\s+/g,' ');
  if (!st._ttsQueue) st._ttsQueue = [];

  const token = st.speechToken;
  st._ttsQueue.push({ text: t, token });
  LOG.debug('➕ enqueue TTS (%d chars) queue=%d token=%s', t.length, st._ttsQueue.length, token);

  processTtsQueue(st, ws, streamSid);
}

function stopTTS(ws, streamSid, reason='stop') {
  const callSid = streamToCall.get(streamSid);
  const st = callSid ? statesByCall.get(callSid) : null;
  if (!st) return;

  // invalida TODO audio pendiente
  st.speechToken = newSpeechToken();

  if (reason === 'barge') {
    logToN8n({ streamSid, type: 'assistant_interrupted', reason: 'caller_barged_in', ts: new Date().toISOString() });
  }
  st.ttsActive = false;
  st.bargeStreak = 0;
  st.lastTtsStopTime = Date.now();
  if (st.ttsSender) try { st.ttsSender.stop(); } catch {}
  resetTtsQueue(st);
  LOG.info('🤫 TTS detenido (%s) %s', reason, streamSid);
}

async function speakWithGoogleTTS(ws, streamSid, text) {
  try {
    const callSid = streamToCall.get(streamSid);
    const st = callSid ? statesByCall.get(callSid) : null;
    if (!st) return;

    LOG.info('🗣️ Asistente (Google TTS): "%s"', text);

    const myToken = st.speechToken;
    const audio = await synthesizeGoogle(text);
    if (!audio) { LOG.warn('⚠️ Google TTS devolvió audio vacío'); return; }

    if (myToken !== st.speechToken) {
      LOG.debug('🪓 speak: audio descartado por token viejo');
      return;
    }

    if (!st.ttsSender) st.ttsSender = createMulawSender(ws, streamSid, 160, 20, 160*20);
    if (!st._ttsClearedThisTurn) {
      sendClear(ws, streamSid);
      st._ttsClearedThisTurn = true;
      LOG.debug('🧽 clear audio buffer (saludo)');
    }
    pushAudioToCaller(st, ws, streamSid, audio);

    st.lastAssistantReplySent = text;
    st.ttsActive = true;
  } catch (err) {
    LOG.error('❌ speakWithGoogleTTS exception %s', err?.message || err);
  }
}

/* =========================
   OPENAI CHAT — Streaming por frases -> TTS
   ========================= */
async function streamAndSpeakOpenAI(st, ws, streamSid, userText, systemPrompt) {
  if (isNoiseUtterance(userText)) {
    return (st.session.step !== 'saludo') ? 'Te escucho…' : 'Hola…';
  }

  const apiKey = envStr('OPENAI_API_KEY', '');
  if (!apiKey) return 'Dime y te ayudo.';

  LOG.info('🤖 OpenAI streaming por frases: prompt_len=%d user_len=%d', (systemPrompt||'').length, userText.length);

  const recentHistory = st.history.slice(-10);
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      temperature: 0.45,
      stream: true,
      // Limita longitud para bajar latencia. Ajusta 160–220 si quieres más/menos.
      max_tokens: 200,
      messages: [
        { role: 'system', content: systemPrompt || 'Responde útil y breve en español.' },
        ...recentHistory,
        { role: 'user', content: userText },
        st.lastReplyText
          ? { role: 'system', content: `No repitas literalmente tu última respuesta: "${st.lastReplyText}". Sé conciso y avanza el flujo.` }
          : null,
      ].filter(Boolean),
    }),
  });

  if (!res.ok || !res.body) {
    const body = await res.text().catch(()=> '');
    LOG.error('❌ OpenAI stream error %d: %s', res.status, body?.slice(0, 400));
    return 'Entendido. ¿Qué necesitas?';
  }

  // Preparar turno: invalidar audio anterior y limpiar cola
  stopTTS(ws, streamSid, 'before_new_reply');
  st.speechToken = newSpeechToken();
  resetTtsQueue(st);

  const reader = res.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let done = false;
  let accText = '';
  let buffer = '';
  let sseBytes = 0, ssePackets = 0;

  const flushChunk = (hard=false) => {
    const chunk = buffer.trim();
    if (!chunk) return;
    if (!hard && chunk.length < 10) return; // evita micro-trozos
    enqueueTts(st, ws, streamSid, chunk);
    LOG.debug('🟢 speak-chunk (%d chars): "%s"', chunk.length, chunk.slice(0, 80));
    buffer = '';
  };

  while (!done) {
    const { value, done: doneNow } = await reader.read();
    done = doneNow;
    if (value) {
      sseBytes += value.byteLength;
      const chunk = decoder.decode(value, { stream: true });
      if (LOG_SSE_CHUNKS) LOG.debug('📡 SSE chunk bytes=%d', value.byteLength);
      const lines = chunk.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const dataStr = trimmed.replace(/^data:\s*/, '');
        ssePackets++;
        if (dataStr === '[DONE]') { done = true; break; }
        try {
          const json = JSON.parse(dataStr);
          const delta = json?.choices?.[0]?.delta?.content || '';
          if (!delta) continue;
          accText += delta;
          buffer += delta;

          // si cerró una frase o el buffer creció, hablar ya
          if (/[.?!…:;]\s$/.test(buffer) || buffer.length >= 160) {
            flushChunk(false);
          }
        } catch {}
      }
    }
  }

  // Último resto
  flushChunk(true);

  LOG.info('✅ OpenAI stream fin | bytes=%d | pkts=%d | reply_len=%d', sseBytes, ssePackets, accText.trim().length);

  st.ttsActive = true;
  return accText.trim() || '¿En qué más te ayudo?';
}

/* =========================
   ESTADO POR LLAMADA
   ========================= */
const statesByCall = new Map();
const streamToCall = new Map();
const streams      = new Map();
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
      _ttsQueue: [],
      _ttsBusy: false,
      _ttsClearedThisTurn: false,

      // control duro de cancelación
      speechToken: newSpeechToken(),

      // flag VAD para pausar/stop
      callerTalking: false,
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

  LOG.debug('💾 persist snapshot %o', snapshot);
  await pushClientMemoryUpdate(snapshot);
}

async function handleCompleteUserTurn(twilioSocket, streamSid, st) {
  if (st.handlingTurn) return;
  st.handlingTurn = true;
  try {
    const COOLDOWN_MS = 200;
    if (st.ttsActive) return;
    if (st.lastTtsStopTime && Date.now() - st.lastTtsStopTime < COOLDOWN_MS) return;

    const humanText = (st.partialBuffer || '').trim();
    if (!humanText) return;
    if (humanText === st.lastUserTurnAnsweredText) return;

    st.history.push({ role: 'user', content: humanText });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

    LOG.info('👤 Turno usuario (final): "%s"', humanText);
    logToN8n({ streamSid, type: 'user_turn_finalized', role: 'user', mensaje: humanText, ts: new Date().toISOString() });

    const intent = classifyIntent(humanText);
    LOG.debug('🧭 intent=%s', intent);

    let hint = '';
    if (intent === 'ask_menu') {
      st.session.menu = await fetchMenuOnly();
      hint = 'El usuario pidió MENÚ: usa 3–5 opciones con nombre y precio desde el JSON Menú (no inventes). Sé breve.';
    } else if (intent === 'ask_promos') {
      st.session.promos = await fetchPromosOnly();
      hint = 'El usuario pidió PROMOCIONES: usa 1–2 activas del JSON Promociones (no inventes). Sé breve.';
    } else if (intent === 'confirm_address') {
      st.session.direccionConfirmada = true;
      hint = 'El usuario confirmó dirección: continúa el flujo de cierre según reglas.';
    } else if (intent === 'closing') {
      hint = 'El usuario está cerrando: confirma pedido final y despídete cordialmente.';
    }

    if (!st._config) st._config = await fetchBotConfig();
    const sysPrompt = buildSystemPromptFromDbTemplate(st._config?.systemPrompt || '', st.session, hint);

    // === Obtener y HABLAR mientras llega (frases)
    const finalText = await streamAndSpeakOpenAI(st, twilioSocket, streamSid, humanText, sysPrompt);

    LOG.info('🤖 Respuesta final LLM (%d chars)', finalText.length);

    // Guarda historial
    st.history.push({ role: 'assistant', content: finalText });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
    st.lastUserTurnHandled      = humanText;
    st.lastUserTurnAnsweredText = humanText;
    st.partialBuffer            = '';
    st.lastReplyText            = finalText;

    await persistSnapshotFromText(st, finalText);
    st.ttsActive = true;

  } finally {
    st.handlingTurn = false;
  }
}

/* =========================
   Twilio customParameters
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
// Umbrales de barge-in (ajustables por ENV)
const RMS_BARGE_THRESHOLD = Number(envStr('RMS_BARGE_THRESHOLD', '0.03'));
const BARGE_STREAK_FRAMES = Number(envStr('BARGE_STREAK_FRAMES', '2'));

wss.on('connection', async (twilioSocket, req) => {
  LOG.info('📞 Conexión de Twilio desde %s url: %s', req?.socket?.remoteAddress, req?.url);
  twilioSocket.on('ping', () => { try { twilioSocket.pong(); } catch {} });
  twilioSocket.on('error', e => LOG.error('❌ WS client error: %s', e?.message || e));

  let mediaFrames = 0;
  let mediaLastTs = Date.now();

  twilioSocket.on('message', async (raw) => {
    const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    let msg; try { msg = JSON.parse(str); } catch { return; }

    if (msg.event === 'start') {
      const streamSid = normalizeSid(msg?.start?.streamSid);
      const callSid   = msg?.start?.callSid || streamSid || 'desconocido';
      if (!streamSid) { LOG.warn('⚠️ start sin streamSid'); return; }

      activePrimarySid = streamSid;
      twilioSocket.streamSid = streamSid;
      streams.set(streamSid, twilioSocket);
      streamToCall.set(streamSid, callSid);

      const st = ensureCallState(callSid);
      LOG.info('▶️ start streamSid=%s callSid=%s | activos=%d', streamSid, callSid, streams.size);

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
          language:'es-419',
          punctuate: true,
          encoding: 'mulaw',
          sample_rate: 8000,
          interim_results: true,
          vad_events: true,
          // ↓ default más bajo para cerrar turnos rápido (ajustable por ENV)
          endpointing: envStr('DG_ENDPOINTING', '300'),
          smart_format: false,
        });
        st.dgSocket = dgSocket;

        dgSocket.on(LiveTranscriptionEvents.Open, async () => {
          LOG.info('🎤 DG OPEN callSid=%s streamSid=%s', callSid, streamSid);
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
              LOG.debug('🔄 precarga menú/promos y memoria OK');
            } catch (e) { LOG.error('precarga error: %s', e?.message || e); }
          }

          if (!st.hasGreeted) {
            const greetingTpl = st._config?.greeting || '¡Hola! ¿Qué te gustaría pedir hoy?';
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
            resetTtsQueue(st);
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

          if (isFinal) {
            LOG.info('🗣️ Cliente (final): %s', normalized);
          } else {
            LOG.debug('🗣️ Cliente (parcial): %s', normalized);
          }
          logToN8n({ streamSid, type: isFinal ? 'user_final_raw' : 'user_partial', role: 'user', mensaje: normalized, ts: new Date().toISOString() });

          if (isFinal) {
            if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
            await handleCompleteUserTurn(twilioSocket, streamSid, st);
          }
        });

        dgSocket.on(LiveTranscriptionEvents.Error, (err) => LOG.error('❌ Deepgram:', err));
        dgSocket.on(LiveTranscriptionEvents.Close, () => LOG.warn('📴 Deepgram CLOSE (espera hangup) callSid=%s', callSid));
      } else {
        LOG.info('🔁 Reusando DG/estado callSid=%s nuevo streamSid=%s', callSid, streamSid);
      }
      return;
    }

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

      if (LOG_MEDIA) {
        mediaFrames++;
        const now = Date.now();
        if (now - mediaLastTs >= 1000) {
          LOG.debug('🎛️ media frames last 1s: %d', mediaFrames);
          mediaFrames = 0;
          mediaLastTs = now;
        }
      }

      // VAD + barge-in
      const vadInfoOpenLike = processFrame(streamSid, audioUlaw);
      const callerTalking   = !!vadInfoOpenLike.open;
      st.callerTalking = callerTalking;

      if (st.ttsActive) {
        if (callerTalking) {
          st.bargeStreak = (st.bargeStreak || 0) + 1;
        } else {
          const frameRms = vadInfoOpenLike.rms ?? ulawRms(audioUlaw);
          if (frameRms >= RMS_BARGE_THRESHOLD) st.bargeStreak = (st.bargeStreak || 0) + 1;
          else st.bargeStreak = 0;
        }
        if (st.bargeStreak >= BARGE_STREAK_FRAMES) {
          LOG.info('✋ barge-in detectado');
          stopTTS(twilioSocket, streamSid, 'barge');
          st.bargeStreak = 0;
        }
      } else {
        st.bargeStreak = 0;
      }

      if (st.dgSocket) {
        try { st.dgSocket.send(audioUlaw); } catch (e) {
          LOG.error('❌ DG send error %s', e?.message || e);
        }
      }
      return;
    }

    if (msg.event === 'stop') {
      const streamSid = twilioSocket.streamSid;
      const callSid   = streamToCall.get(streamSid);
      LOG.warn('⏹️ Twilio stop %s callSid=%s', streamSid, callSid);

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
    LOG.warn('❌ Twilio cerró la conexión %s callSid=%s', streamSid, callSid);

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

wss.on('error', (err) => LOG.error('❌ WSS error: %s', err));

/* =========================
   HTTP + UPGRADE A WS
   ========================= */
app.server = app.listen(port, () => {
  LOG.info(`🚀 Servidor escuchando en http://localhost:${port}`);
});
app.server.on('upgrade', (req, socket, head) => {
  if (!req.url || !req.url.startsWith('/twilio')) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

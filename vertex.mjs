// index.mjs — Voice Assistant RT cálido e interactivo (anti-acumulación)
// Deepgram + Twilio + AzureTTS (streaming) + OpenAI (fallback)
// Menú/promos/memoria vía n8n (sin inventar)

import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import dotenv from 'dotenv';
import sdk from 'microsoft-cognitiveservices-speech-sdk';
import { processFrame, clearVadState } from './vad.mjs';

dotenv.config();

/* =============== ENV HELPERS =============== */
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

/* =============== EXPRESS / PORT =============== */
const app = express();
const port = envNum('PORT', 3000);
app.use(express.static('.'));
app.use(express.json({ limit: '10mb' }));

/* =============== KEYS / CLIENTES =============== */
const deepgram = createClient(envStr('DEEPGRAM_API_KEY', ''));

// Azure TTS
const AZURE_SPEECH_KEY    = envStr('AZURE_SPEECH_KEY', '');
const AZURE_SPEECH_REGION = envStr('AZURE_SPEECH_REGION', '');
const AZURE_TTS_VOICE     = envStr('AZURE_TTS_VOICE', 'es-MX-DaliaNeural');

// n8n
const N8N_BASE_URL       = envStr('N8N_BASE_URL', '');
const N8N_SHARED_SECRET  = envStr('N8N_SHARED_SECRET', 'pizzeriadonnapoliSUPERSECRETO');
const N8N_WEBHOOK_URL    = envStr('N8N_WEBHOOK_URL', ''); // opcional

/* =============== LATENCIAS / CONTROL =============== */
const DG_ENDPOINTING        = envStr('DG_ENDPOINTING', '150');
const SILENCE_DECISION_MS   = envNum('SILENCE_DECISION_MS', 420);
const RMS_BARGE_THRESHOLD   = envNum('RMS_BARGE_THRESHOLD', 0.07);
const BARGE_STREAK_FRAMES   = envNum('BARGE_STREAK_FRAMES', 4);

/* Paginación de voz */
const MENU_PAGE_SIZE        = envNum('MENU_PAGE_SIZE', 3);
const PROMO_PAGE_SIZE       = envNum('PROMO_PAGE_SIZE', 3);

/* =============== HELPERS n8n =============== */
async function callN8n(payload) {
  if (!N8N_BASE_URL) { console.warn('⚠️ N8N_BASE_URL vacío'); return null; }
  try {
    const res = await fetch(N8N_BASE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-shared-secret': N8N_SHARED_SECRET },
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    if (!res.ok) { console.error('❌ n8n error', res.status, text); return null; }
    try { return JSON.parse(text); } catch { return text; }
  } catch (e) { console.error('⚠️ callN8n error:', e?.message || e); return null; }
}
async function fetchClientMemory(callerId) {
  const out = await callN8n({ action: 'get_memory', callerId });
  if (!out || typeof out !== 'object') return {};
  const row = Array.isArray(out) ? out[0] : out;
  return {
    nombreCliente: row?.nombre_cliente ?? null,
    direccionFavorita: row?.direccion_favorita ?? null,
    direccionConfirmada: row?.direccion_confirmada ?? false,
    ultimoPedido: row?.ultimo_pedido ?? null,
  };
}
async function fetchMenuAndPromos() {
  const out = await callN8n({ action: 'get_menu' });
  if (!out || typeof out !== 'object') return { menu: [], promos: [] };
  return {
    menu: Array.isArray(out.menu) ? out.menu : [],
    promos: Array.isArray(out.promos) ? out.promos : [],
  };
}
async function pushClientMemoryUpdate(snapshot) {
  await callN8n({ action: 'update_memory', ...snapshot });
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

/* =============== WS infra =============== */
const wss = new WebSocketServer({ noServer: true });
const streams = new Map();
const memory  = new Map();
const SILENCE_FRAME = Buffer.alloc(160, 0xff); // 20ms mulaw
let activePrimarySid = null;

/* =============== STATE =============== */
function ensureState(sid) {
  if (!memory.has(sid)) {
    memory.set(sid, {
      session: {
        callerId: null,
        nombreCliente: null,
        direccionFavorita: null,
        direccionConfirmada: false,
        step: 'saludo',
        menu: [],
        promos: [],
        menuPages: [],
        promoPages: [],
        menuPageIdx: 0,
        promoPageIdx: 0,
        readingFullMenu: false,
        readingPromos: false,
        carrito: [],
      },
      history: [],
      lastReplyText: '',
      lastAssistantReplySent: '',
      partialBuffer: '',
      lastUserTurnHandled: '',
      lastUserTurnAnsweredText: '',
      // audio/tts
      ttsSender: null,
      ttsActive: false,
      azureSynth: null,
      __primed: false,
      // timers
      silenceTimer: null,
      bargeStreak: 0,
      handlingTurn: false,
      lastTtsStopTime: 0,
      // stt socket
      hasGreeted: false,
      dgSocket: null,
      stopHeartbeat: null,
      // cancelación de respuestas en curso
      replyAbort: null,
    });
  }
  return memory.get(sid);
}
const normalizeSid = v => (v == null ? '' : String(v).replace(/^=\s*/, '').trim());

/* =============== AUDIO UTILS =============== */
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
  for (let i = 0; i < buf.length; i++) {
    const s = ULAW_TAB[buf[i] & 0xff];
    acc += s * s;
  }
  return Math.sqrt(acc / buf.length) / 32768;
}

/* =============== TTS pacing/control =============== */
function sendClear(ws, sid) { try { ws.send(JSON.stringify({ event: 'clear', streamSid: sid })); } catch {} }
function disposeAzureSynth(st) { try { if (st.azureSynth) { st.azureSynth.close(); st.azureSynth = null; } } catch {} }
function stopTTS(ws, sid, reason='stop') {
  const st = ensureState(sid);
  // abortar cualquier respuesta/LLM en curso
  try { st.replyAbort?.abort(); } catch {}
  st.replyAbort = null;

  if (!st.ttsActive && !st.azureSynth) { sendClear(ws, sid); return; }
  if (reason === 'barge') {
    logToN8n({ streamSid: sid, type: 'assistant_interrupted', reason: 'caller_barged_in', ts: new Date().toISOString() });
  }
  try { disposeAzureSynth(st); } catch {}
  if (st.ttsSender) st.ttsSender.stop();
  st.ttsActive = false;
  st.lastTtsStopTime = Date.now();
  sendClear(ws, sid);
}
function createMulawSender(ws, sid, frameBytes=160, frameMs=20, prebufferBytes=160*4) {
  const st = ensureState(sid);
  let buf = Buffer.alloc(0);
  let running = false;
  let nextTick = 0;
  let timer = null;
  function tick() {
    if (!running) return;
    if (ws.readyState !== 1) { stop(); return; }
    if (buf.length < frameBytes || (nextTick===0 && buf.length < Math.max(prebufferBytes, frameBytes))) {
      timer = setTimeout(tick, frameMs); return;
    }
    if (ws.bufferedAmount > 64*1024) { timer = setTimeout(tick, Math.min(frameMs*2, 50)); return; }
    const slice = buf.subarray(0, frameBytes);
    buf = buf.subarray(frameBytes);
    const payload = slice.toString('base64');
    try {
      ws.send(JSON.stringify({ event:'media', streamSid:sid, media:{ payload } }));
    } catch { stop(); return; }
    if (nextTick === 0) nextTick = Date.now() + frameMs; else nextTick += frameMs;
    const wait = nextTick - Date.now();
    timer = setTimeout(tick, wait > 0 ? wait : 0);
  }
  function push(data) { buf = Buffer.concat([buf, data]); if (!running) { running = true; st.ttsActive = true; tick(); } }
  function stop() { running = false; st.ttsActive = false; if (timer) { clearTimeout(timer); timer=null; } }
  return { push, stop };
}

/* =============== PREEMPT: corta y habla ya =============== */
async function speakChunkStreaming(ws, sid, text) {
  const st = ensureState(sid);
  if (!text || !text.trim()) return;
  if (!st.ttsSender) st.ttsSender = createMulawSender(ws, sid, 160, 20, 160*4);
  sendClear(ws, sid);
  const synth = ensureAzureSynth(st);
  if (!synth) return;
  st.ttsActive = true;

  synth.synthesizing = (_s, e) => {
    try {
      if (!st.__primed) { st.ttsSender.push(Buffer.alloc(160*2, 0xFF)); st.__primed = true; }
      const buf = Buffer.from(e.result.audioData || []);
      if (buf.length) st.ttsSender.push(buf);
    } catch (err) { console.error('Azure synthesizing err:', err); }
  };
  synth.SynthesisCompleted = () => { st.ttsActive = false; };
  synth.SynthesisCanceled  = () => { st.ttsActive = false; };

  const ssml = buildSSMLFromText(text);
  return new Promise(resolve => {
    synth.speakSsmlAsync(
      ssml,
      () => { st.ttsActive = false; resolve(); },
      err => { console.error('Azure speak err:', err); st.ttsActive = false; resolve(); }
    );
  });
}
async function speakNow(ws, sid, st, text) {
  if (!text || !text.trim()) return;
  stopTTS(ws, sid, 'preempt');
  await speakChunkStreaming(ws, sid, text);
}

/* =============== NLU/NLG helpers =============== */
function isNoiseUtterance(str) {
  const cleaned = str.toLowerCase().replace(/[¿?¡!.,]/g,'').trim();
  const tokens = cleaned.split(/\s+/).filter(Boolean);
  if (tokens.length <= 3) {
    const common = ['alo','aló','hola','holaa','buenas','buenas?','si','sí','me','escuchas','meescuchas','alo?','aló?','hola?','alooo','aloooo','dime','diga','dígame','buenas tardes','buenas?'];
    if (common.includes(tokens.join(' '))) return true;
  }
  const uniq = new Set(tokens);
  if (uniq.size === 1 && tokens.length <= 5) return true;
  return false;
}
function normalizeUserText(text) {
  let t = (text || '').toLowerCase();
  const fixes = [
    ['graciosa','gaseosa'],
    ['graciosas','gaseosas'],
    ['grasiosa','gaseosa'],
    ['gaziosa','gaseosa'],
    ['dinealco','dime algo'],
    ['linealco','dime algo'],
  ];
  for (const [a,b] of fixes) t = t.replace(new RegExp(`\\b${a}\\b`,'g'), b);
  t = t.replace(/\bque mas tienen\b/g, 'qué más tienen');
  t = t.replace(/\bqué mas tienen\b/g, 'qué más tienen');
  return t.normalize('NFC');
}
function userTurnReady(st) {
  const txt = (st.partialBuffer || '').trim();
  if (!txt) return false;
  if (txt.length < 12 && !/[\.!\?]/.test(txt)) return false;
  if (/\b(quiero|necesito|dime|tienen|cual|cuál|menu|menú|gaseosa)$/.test(txt)) return false;
  return true;
}

/* =============== Prompt base (fallback LLM) =============== */
function buildSystemPrompt(session) {
  const menuStr   = JSON.stringify(session.menu ?? [],   null, 2);
  const promosStr = JSON.stringify(session.promos ?? [], null, 2);

  const persona = `
Eres Clara, la asistente virtual de Pizzería Don Napoli.
Tu personalidad es súper amigable, cálida, paciente y servicial. Usa un lenguaje natural y cercano.
Mantén respuestas cortas (1–3 oraciones). Español latino neutro. No suenes robótica.
`.trim();

  const reglas = `
REGLAS ESTRICTAS:
1) Usa SIEMPRE el menú/promos provistos. No inventes.
2) Si piden todo el menú/promos: léelo por partes de 3 ítems y pregunta si quieren "siguiente".
3) Si piden "qué tienen": resume 2–3 ítems reales y ofrece leer todo el menú.
4) Solo devuelve texto para hablar, sin notas.
`.trim();

  return `
${persona}

Menú:
${menuStr}

Promos:
${promosStr}

${reglas}
`.trim();
}

/* =============== Habla / formatos cálidos =============== */
function sayPrice(p) {
  if (p == null || p === '') return '';
  const [d, c='00'] = String(p).split('.');
  const dd = Number(d);
  const cc = Number((c + '00').slice(0,2));
  if (!Number.isFinite(dd) || !Number.isFinite(cc)) return `${p} dólares`;
  if (cc === 0) return `${dd} dólares`;
  return `${dd} dólares con ${cc} centavos`;
}
function normalizeMenuItem(m) {
  return {
    id: m.id ?? null,
    nombre: m.nombre ?? m.name ?? 'Producto',
    descripcion: m.descripcion ?? m.description ?? '',
    precio: m.precio ?? m.price ?? '',
    disponible: (m.disponible !== false),
  };
}
function paginate(arr, size) {
  const pages = [];
  for (let i=0; i<arr.length; i+=size) pages.push(arr.slice(i, i+size));
  return pages;
}
function firstWords(str, n=8) { return String(str||'').split(/\s+/).slice(0, n).join(' '); }
function renderMenuLine(m) {
  const precio = sayPrice(m.precio);
  const corto = firstWords(m.descripcion, 8);
  const tail  = corto ? ` — ${corto}` : '';
  return `• ${m.nombre}: ${precio}${tail}`;
}
function renderPageSpeech(items, tipo='menú') {
  if (!items || !items.length) return `No tengo datos de ${tipo} en este momento.`;
  return items.map(renderMenuLine).join('\n');
}
function wrapWarmList(intro, list, outro) { return `${intro}\n${list}\n${outro}`; }

/* =============== Intents =============== */
function detectIntent(txt) {
  const t = txt.toLowerCase();
  if (/(todo el menú|todo el menu|menú completo|menu completo|léeme todo|leer todo|todo por favor)/.test(t)) return 'ask_full_menu';
  if (/(menu|menú|qué tienen|que tienen|opciones|carta|sabores)/.test(t)) return 'ask_menu';
  if (/(promo|promos|promociones|ofertas|descuento)/.test(t)) return 'ask_promos';
  if (/(sigue|siguiente|continúa|continuar|más|mas)/.test(t)) return 'next';
  if (/(antes|anterior|retrocede|atrás|atras)/.test(t)) return 'prev';
  if (/(repite|otra vez|de nuevo)/.test(t)) return 'repeat';
  if (/(gaseosa|refresco|bebida|cola|sprite|fanta)/.test(t)) return 'ask_drinks';
  if (/(alas|alitas|bbq)/.test(t)) return 'ask_wings';
  return null;
}

/* =============== Azure TTS (streaming) =============== */
function buildSSMLFromText(text) {
  const style  = envStr('AZURE_TTS_STYLE', 'friendly');
  const rate   = envStr('AZURE_TTS_RATE', '1.05');
  const pitch  = envStr('AZURE_TTS_PITCH', '+0%');
  const volume = envStr('AZURE_TTS_VOLUME', 'default');
  const pause  = envNum('AZURE_TTS_PAUSE_MS', 140);
  const voice  = envStr('AZURE_TTS_VOICE', AZURE_TTS_VOICE);
  const styleDegree = envStr('AZURE_TTS_STYLE_DEGREE', '1.4');

  const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const sentences = esc(text).split(/(?<=[\.\!\?…])\s+/).map(s => s.trim()).filter(Boolean);
  const body = sentences.map((s,i)=> `${s}${i<sentences.length-1?` <break time="${pause}ms"/>`:''}`).join(' ');

  return `<?xml version="1.0" encoding="utf-8"?>
<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis"
       xmlns:mstts="http://www.w3.org/2001/mstts" xml:lang="es-MX">
  <voice name="${voice}">
    <mstts:express-as style="${style}" styledegree="${styleDegree}">
      <prosody rate="${rate}" pitch="${pitch}" volume="${volume}">
        ${body}
      </prosody>
    </mstts:express-as>
  </voice>
</speak>`;
}
function ensureAzureSynth(st) {
  if (st.azureSynth) return st.azureSynth;
  if (!AZURE_SPEECH_KEY || !AZURE_SPEECH_REGION) { console.error('❌ Falta AZURE_SPEECH_*'); return null; }
  const speechConfig = sdk.SpeechConfig.fromSubscription(AZURE_SPEECH_KEY, AZURE_SPEECH_REGION);
  speechConfig.speechSynthesisLanguage = 'es-MX';
  speechConfig.speechSynthesisVoiceName = envStr('AZURE_TTS_VOICE', 'es-MX-DaliaNeural');
  speechConfig.speechSynthesisOutputFormat = sdk.SpeechSynthesisOutputFormat.Raw8Khz8BitMonoMULaw;
  const synth = new sdk.SpeechSynthesizer(speechConfig, undefined);
  st.azureSynth = synth;
  return synth;
}

/* =============== Fraseo incremental (no se usa para hablar) =============== */
function* phraseChunksFromTokens() {
  let buf = '';
  const ender = /[\.!\?…:;,\)]$/;
  while (true) {
    const token = yield;
    if (token == null) { if (buf.trim()) yield { text: buf.trim(), final: true }; break; }
    buf += token;
    const trimmed = buf.trim();
    if ((ender.test(trimmed) && trimmed.length >= 40) || trimmed.length >= 120) {
      const out = trimmed; buf = ''; yield { text: out, final: false };
    }
  }
}

/* =============== Micro-ack humano instantáneo (PREEMPT) =============== */
async function quickAck(ws, sid, kind='generic') {
  const st = ensureState(sid);
  let text = 'Claro, ya te cuento.';
  if (kind === 'menu')   text = 'Claro, te cuento el menú.';
  if (kind === 'promos') text = 'Perfecto, te digo las promos.';
  if (kind === 'order')  text = 'Súper, te lo preparo.';
  await speakNow(ws, sid, st, text);
}

/* =============== Motor de Respuesta por Intención =============== */
function buildPagesInSession(st) {
  const menu = (st.session.menu || []).map(normalizeMenuItem).filter(x => x.disponible !== false);
  const promos = (st.session.promos || []).map(normalizeMenuItem);
  st.session.menuPages = paginate(menu, MENU_PAGE_SIZE);
  st.session.promoPages = paginate(promos, PROMO_PAGE_SIZE);
  if (st.session.menuPageIdx >= st.session.menuPages.length) st.session.menuPageIdx = 0;
  if (st.session.promoPageIdx >= st.session.promoPages.length) st.session.promoPageIdx = 0;
}

function craftAnswerFromIntent(st, userText) {
  const intent = detectIntent(userText);
  const S = st.session;

  const nameHi = S?.nombreCliente ? ` ${S.nombreCliente}` : '';
  const warmAskNext = '¿Sigo con los siguientes?';
  const warmClose   = '¿Quieres pedir alguno o te cuento otro par?';

  if (intent === 'ask_full_menu') {
    S.readingFullMenu = true; S.readingPromos = false;
    if (!S.menuPages.length) return 'Uy, no encuentro el menú ahora mismo.';
    const page = S.menuPages[S.menuPageIdx] || [];
    const speech = renderPageSpeech(page, 'menú');
    const more = (S.menuPageIdx < S.menuPages.length - 1) ? `${warmAskNext}` : 'Eso es todo el menú disponible.';
    return wrapWarmList(`Perfecto${nameHi}, aquí va el menú:`, speech, more);
  }

  if (intent === 'ask_menu') {
    S.readingFullMenu = false; S.readingPromos = false;
    if (!S.menuPages.length) return 'Uy, no encuentro el menú ahora mismo.';
    S.menuPageIdx = 0;
    const page = S.menuPages[0] || [];
    const speech = renderPageSpeech(page, 'menú');
    const outro = (S.menuPages.length > 1)
      ? 'Si quieres, te leo todo el menú. Puedes decir “todo el menú” o “siguiente”.'
      : warmClose;
    return wrapWarmList(`Claro${nameHi}, te comparto algunas opciones:`, speech, outro);
  }

  if (intent === 'ask_promos') {
    S.readingFullMenu = false; S.readingPromos = true;
    if (!S.promoPages.length) return 'Por ahora no tengo promociones activas.';
    const page = S.promoPages[S.promoPageIdx] || [];
    const speech = renderPageSpeech(page, 'promociones');
    const more = (S.promoPageIdx < S.promoPages.length - 1) ? `${warmAskNext}` : 'Esas son todas las promos vigentes.';
    return wrapWarmList(`¡Buenísimo${nameHi}! Estas son las promos:`, speech, more);
  }

  if (intent === 'next') {
    if (S.readingPromos) {
      if (!S.promoPages.length) return 'No tengo promociones cargadas.';
      if (S.promoPageIdx < S.promoPages.length - 1) S.promoPageIdx++;
      const page = S.promoPages[S.promoPageIdx] || [];
      const speech = renderPageSpeech(page, 'promociones');
      const more = (S.promoPageIdx < S.promoPages.length - 1) ? warmAskNext : 'Y listo, esas fueron todas.';
      return wrapWarmList('Vamos con las siguientes promos:', speech, more);
    }
    if (!S.menuPages.length) return 'No tengo menú disponible ahora mismo.';
    if (S.menuPageIdx < S.menuPages.length - 1) S.menuPageIdx++;
    const page = S.menuPages[S.menuPageIdx] || [];
    const speech = renderPageSpeech(page, 'menú');
    const more = (S.menuPageIdx < S.menuPages.length - 1) ? warmAskNext : 'Y con eso, completamos todo el menú.';
    return wrapWarmList('Dale, seguimos con:', speech, more);
  }

  if (intent === 'prev') {
    if (S.readingPromos) {
      if (!S.promoPages.length) return 'No tengo promociones cargadas.';
      if (S.promoPageIdx > 0) S.promoPageIdx--;
      const page = S.promoPages[S.promoPageIdx] || [];
      return wrapWarmList('Retrocedo un bloque:', renderPageSpeech(page, 'promociones'), '¿Te repito algo o sigo?');
    }
    if (!S.menuPages.length) return 'No tengo menú disponible ahora mismo.';
    if (S.menuPageIdx > 0) S.menuPageIdx--;
    const page = S.menuPages[S.menuPageIdx] || [];
    return wrapWarmList('Listo, voy a la parte anterior:', renderPageSpeech(page, 'menú'), '¿Te repito algo o sigo?');
  }

  if (intent === 'repeat') {
    if (S.readingPromos) {
      if (!S.promoPages.length) return 'No tengo promociones cargadas.';
      const page = S.promoPages[S.promoPageIdx] || [];
      return wrapWarmList('Te repito esa parte:', renderPageSpeech(page, 'promociones'), '¿Así está bien?');
    }
    if (!S.menuPages.length) return 'No tengo menú disponible ahora mismo.';
    const page = S.menuPages[S.menuPageIdx] || [];
    return wrapWarmList('Te repito esa parte:', renderPageSpeech(page, 'menú'), '¿Así está mejor?');
  }

  // Preguntas específicas (bebidas, alitas)
  if (intent === 'ask_drinks') {
    const drinks = (S.menu || []).map(normalizeMenuItem)
      .filter(m => /gaseosa|bebida|cola|sprite|fanta/i.test(`${m.nombre} ${m.descripcion}`));
    if (drinks.length) {
      const lines = drinks.slice(0,3).map(renderMenuLine).join('\n');
      return wrapWarmList(`Sí${nameHi}, tenemos bebidas:`, lines, '¿Te leo todas o te sirvo una Coca-Cola de 1.5 litros?');
    }
    return `Sí${nameHi}, contamos con gaseosas. ¿Quieres que te lea el detalle del menú completo?`;
  }
  if (intent === 'ask_wings') {
    const wings = (S.menu || []).map(normalizeMenuItem)
      .find(m => /alitas/i.test(`${m.nombre} ${m.descripcion}`));
    if (wings) return `Tenemos ${wings.nombre} por ${sayPrice(wings.precio)}. ¿Te agrego una orden?`;
    return `Solemos tener alitas BBQ. ¿Quieres que verifique disponibilidad?`;
  }

  return null;
}

/* =============== Detección de pedidos (carrito) =============== */
function detectOrder(text, menu) {
  const t = text.toLowerCase();
  if (!/(quiero|necesito|pón|pon|agrega|sumar|pedir)/.test(t)) return null;

  const items = [];
  for (const raw of (menu || [])) {
    const m = normalizeMenuItem(raw);
    const name = (m.nombre||'').toLowerCase();
    if (!name) continue;
    const key = name.split(/\s+/).find(w => w.length >= 4);
    if (key && t.includes(key)) items.push(m);
    if (/pepperoni/.test(t) && /pepperoni/i.test(name)) items.push(m);
    if (/margarita|margherita/.test(t) && /margarita|margherita/i.test(name)) items.push(m);
    if (/hawaiana|hawai/.test(t) && /hawaiana/i.test(name)) items.push(m);
    if (/alita|bbq/.test(t) && /alitas/i.test(name)) items.push(m);
    if (/coca|cola|sprite|fanta|gaseosa|bebida/.test(t) && /(gaseosa|cola|sprite|fanta)/i.test(name)) items.push(m);
  }
  if (!items.length) return null;
  const unique = [];
  const seen = new Set();
  for (const m of items) { const k = (m.nombre||'').toLowerCase(); if (!seen.has(k)) { seen.add(k); unique.push(m); } }
  return unique.slice(0, 3);
}

/* =============== LLM (fallback) — MODO final_only =============== */
async function streamAnswerAndSpeak(twilioSocket, sid, st, userTextRaw) {
  const userText = normalizeUserText(userTextRaw);

  if (isNoiseUtterance(userText)) {
    const yaSalude = (st.session.step !== 'saludo');
    const canned = yaSalude
      ? 'Sí, te escucho claro. ¿Qué te gustaría pedir?'
      : 'Pizzería Don Napoli, hola. ¿Qué te gustaría pedir hoy?';
    st.lastReplyText = canned;
    await speakNow(twilioSocket, sid, st, canned);
    return canned;
  }

  // Pedido directo
  const maybeItems = detectOrder(userText, st.session.menu);
  if (maybeItems && maybeItems.length) {
    const added = [];
    for (const m of maybeItems) {
      st.session.carrito.push({ nombre: m.nombre, precio: m.precio });
      added.push(`${m.nombre} (${sayPrice(m.precio)})`);
    }
    const listado = added.join(' y ');
    const upsell = st.session.carrito.length === 1
      ? '¿Quieres acompañar con gaseosa o alitas?'
      : '¿Algo más para acompañar?';
    const msg = `Listo${st.session.nombreCliente ? ' '+st.session.nombreCliente : ''}, te agrego ${listado}. ${upsell}`;
    st.lastReplyText = msg;
    st.history.push({ role: 'assistant', content: msg });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
    await quickAck(twilioSocket, sid, 'order');
    await speakNow(twilioSocket, sid, st, msg);
    return msg;
  }

  // Intención clara (no LLM)
  const intentId = detectIntent(userText);
  if (intentId === 'ask_menu' || intentId === 'ask_full_menu' || intentId === 'ask_promos') {
    await quickAck(twilioSocket, sid, intentId === 'ask_promos' ? 'promos' : 'menu');
  }
  const intentReply = craftAnswerFromIntent(st, userText);
  if (intentReply) {
    st.lastReplyText = intentReply;
    st.history.push({ role: 'assistant', content: intentReply });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
    await speakNow(twilioSocket, sid, st, intentReply);
    return intentReply;
  }

  // Fallback LLM (una sola salida; cancelable)
  const controller = new AbortController();
  st.replyAbort = controller;

  let fullText = '';
  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: { Authorization: `Bearer ${envStr('OPENAI_API_KEY','')}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        temperature: 0.85,
        top_p: 0.95,
        max_tokens: 140,
        stream: true,
        messages: [
          { role: 'system', content: buildSystemPrompt(st.session) },
          ...st.history.slice(-10),
          { role: 'user', content: userText },
          st.lastReplyText ? { role: 'system', content: `No repitas literalmente tu última respuesta: "${st.lastReplyText}".` } : null,
        ].filter(Boolean),
      }),
    });

    if (!resp.ok || !resp.body) throw new Error(`LLM HTTP ${resp.status}`);

    const reader = resp.body.getReader();
    const dec = new TextDecoder();

    let doneStream = false;
    while (!doneStream) {
      const { value, done } = await reader.read();
      if (done) break;
      const chunk = dec.decode(value, { stream: true });
      for (const line of chunk.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const dataStr = line.slice(5).trim();
        if (dataStr === '[DONE]') { doneStream = true; break; }
        let data; try { data = JSON.parse(dataStr); } catch { continue; }
        const t = data?.choices?.[0]?.delta?.content ?? '';
        if (t) fullText += t;
      }
    }
  } catch (err) {
    if (err?.name === 'AbortError') {
      // cancelado por barge/new turn -> no hablar nada
      return '';
    }
    console.error('LLM stream error:', err?.message || err);
  } finally {
    st.replyAbort = null;
  }

  const finalOut = (fullText.trim() || 'Te escucho. ¿Te leo todo el menú o prefieres una recomendación rápida?').trim();
  st.lastReplyText = finalOut;
  st.history.push({ role: 'assistant', content: finalOut });
  if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
  await speakNow(twilioSocket, sid, st, finalOut);
  return finalOut;
}

/* =============== TURN LOGIC =============== */
async function handleCompleteUserTurn(twilioSocket, sid) {
  const st = ensureState(sid);
  if (st.handlingTurn) return;
  st.handlingTurn = true;
  try {
    const COOLDOWN_MS = 250;
    if (st.ttsActive) return;
    if (st.lastTtsStopTime && Date.now() - st.lastTtsStopTime < COOLDOWN_MS) return;

    const humanText = (st.partialBuffer || '').trim();
    if (!humanText) return;
    if (!userTurnReady(st)) return;
    if (humanText === st.lastUserTurnAnsweredText) return;

    st.history.push({ role: 'user', content: humanText });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

    logToN8n({ streamSid: sid, type: 'user_turn_finalized', role: 'user', mensaje: humanText, ts: new Date().toISOString() });

    stopTTS(twilioSocket, sid, 'before_new_reply'); // también aborta LLM previo

    const reply = await streamAnswerAndSpeak(twilioSocket, sid, st, humanText);
    console.log('🧠 Asistente (intent/LLM final_only):', reply);

    st.lastUserTurnHandled      = humanText;
    st.lastUserTurnAnsweredText = humanText;
    st.partialBuffer            = '';
  } finally {
    st.handlingTurn = false;
  }
}

/* =============== HEARTBEAT Deepgram =============== */
function startDeepgramHeartbeat(dgSocket) {
  const HEARTBEAT_MS = 1000; let alive = true;
  const loop = () => { if (!alive) return; try { dgSocket.send(SILENCE_FRAME); } catch { alive = false; return; } setTimeout(loop, HEARTBEAT_MS); };
  loop(); return () => { alive = false; };
}

/* =============== Twilio WS handler =============== */
wss.on('connection', async (twilioSocket, req) => {
  console.log('📞 Conexión de Twilio desde', req?.socket?.remoteAddress, 'url:', req?.url);
  twilioSocket.on('ping', () => { try { twilioSocket.pong(); } catch {} });
  twilioSocket.on('error', e => console.error('❌ WS client error:', e?.message || e));

  twilioSocket.on('message', async (raw) => {
    const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    let msg; try { msg = JSON.parse(str); } catch { return; }

    if (msg.event === 'start') {
      const sid = normalizeSid(msg?.start?.streamSid);
      if (!sid) { console.warn('⚠️ start sin streamSid'); return; }

      activePrimarySid = sid;
      twilioSocket.streamSid = sid;
      streams.set(sid, twilioSocket);

      const st = ensureState(sid);
      st.session.callerId = msg.start?.from || msg.start?.caller || msg.start?.callSid || 'desconocido';
      console.log('▶️ start streamSid:', sid, '| activos:', streams.size);

      if (!st.dgSocket) {
        const dgSocket = await deepgram.listen.live({
          model: 'nova-3',
          language: 'es',
          punctuate: true,
          encoding: 'mulaw',
          sample_rate: 8000,
          interim_results: true,
          vad_events: true,
          endpointing: DG_ENDPOINTING,
          smart_format: false,
        });
        st.dgSocket = dgSocket;

        dgSocket.on(LiveTranscriptionEvents.Open, async () => {
          console.log('🎤 Deepgram OPEN', sid);
          st.stopHeartbeat = startDeepgramHeartbeat(dgSocket);

          (async () => {
            const [memData, menuData] = await Promise.all([
              fetchClientMemory(st.session.callerId),
              fetchMenuAndPromos(),
            ]);
            if (memData && typeof memData === 'object') {
              if (memData.nombreCliente)       st.session.nombreCliente       = memData.nombreCliente;
              if (memData.direccionFavorita)   st.session.direccionFavorita   = memData.direccionFavorita;
              if (memData.direccionConfirmada) st.session.direccionConfirmada = !!memData.direccionConfirmada;
            }
            st.session.menu   = menuData?.menu   || [];
            st.session.promos = menuData?.promos || [];
            buildPagesInSession(st);

            console.log('🍕 Menú n8n:', st.session.menu);
            console.log('🎁 Promos n8n:', st.session.promos);

            if (!st.hasGreeted) {
              const saludo = st.session.nombreCliente
                ? `Hola ${st.session.nombreCliente}. ¿Te leo el menú completo o prefieres que te recomiende algo rapidito?`
                : 'Hola, bienvenid@ a Pizzería Don Napoli. ¿Te leo el menú completo o prefieres que te recomiende algo rapidito?';
              st.hasGreeted = true;
              st.session.step = 'en_conversacion';
              st.history.push({ role: 'assistant', content: saludo });
              if (st.history.length > 30) st.history.splice(0, st.history.length - 30);
              stopTTS(twilioSocket, sid, 'before_greeting');
              await speakNow(twilioSocket, sid, st, saludo);
            }
          })().catch(e => console.error('Precarga n8n error:', e?.message || e));
        });

        dgSocket.on(LiveTranscriptionEvents.Transcript, async (data) => {
          if (sid !== activePrimarySid) return;
          const alt = data?.channel?.alternatives?.[0];
          const rawTxt = (alt?.transcript || '').trim();
          if (!rawTxt) return;
          const isFinal = data?.is_final || data?.speech_final;
          let normalized = rawTxt.replace(/\s+/g, ' ').trim();
          normalized = normalizeUserText(normalized);

          const st = ensureState(sid);
          st.partialBuffer = normalized;

          console.log(isFinal ? '🗣️ Cliente (final):' : '🗣️ Cliente (parcial):', normalized);
          logToN8n({ streamSid: sid, type: isFinal ? 'user_final_raw' : 'user_partial', role:'user', mensaje: normalized, ts: new Date().toISOString() });

          if (!isFinal) {
            if (!st.silenceTimer) {
              st.silenceTimer = setTimeout(async () => {
                st.silenceTimer = null;
                if (userTurnReady(st)) await handleCompleteUserTurn(twilioSocket, sid);
              }, SILENCE_DECISION_MS);
            }
          } else {
            if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
            await handleCompleteUserTurn(twilioSocket, sid);
          }
        });

        dgSocket.on(LiveTranscriptionEvents.Error, (err) => console.error('❌ Deepgram error:', err));
        dgSocket.on(LiveTranscriptionEvents.Close, () => console.log('📴 Deepgram CLOSE', sid));
      } else {
        console.log('🔁 Reusando Deepgram existente para', sid);
      }
      return;
    }

    if (msg.event === 'media') {
      const sid = twilioSocket.streamSid;
      if (!sid || sid !== activePrimarySid) return;
      const st = ensureState(sid);
      if ((msg?.track || 'inbound') !== 'inbound') return;

      const payload = msg.media?.payload || '';
      const audioUlaw = Buffer.from(payload, 'base64');
      if (!audioUlaw.length) return;

      const vad = processFrame(sid, audioUlaw);
      const callerTalking = !!vad.open;
      if (st.ttsActive || st.azureSynth) {
        if (callerTalking) st.bargeStreak = (st.bargeStreak || 0) + 1;
        else {
          const frameRms = vad.rms ?? ulawRms(audioUlaw);
          if (frameRms >= RMS_BARGE_THRESHOLD) st.bargeStreak = (st.bargeStreak || 0) + 1;
          else st.bargeStreak = 0;
        }
        if (st.bargeStreak >= BARGE_STREAK_FRAMES) {
          stopTTS(twilioSocket, sid, 'barge'); // también aborta LLM
          st.bargeStreak = 0;
        }
      } else st.bargeStreak = 0;

      if (st.dgSocket) { try { st.dgSocket.send(audioUlaw); } catch (e) { console.error('❌ send DG err', e?.message || e); } }
      return;
    }

    if (msg.event === 'stop') {
      const sid = twilioSocket.streamSid;
      console.log('⏹️ Twilio stop', sid);
      if (sid !== activePrimarySid) {
        streams.delete(sid); memory.delete(sid); clearVadState(sid);
        return;
      }

      stopTTS(twilioSocket, sid, 'stop');
      const st = memory.get(sid);
      if (st) {
        await pushClientMemoryUpdate({
          callerId: st.session.callerId || null,
          nombreCliente: st.session.nombreCliente || null,
          direccionFavorita: st.session.direccionFavorita || null,
          direccionConfirmada: !!st.session.direccionConfirmada,
          ultimoMensajeAsistente: st.lastReplyText || null,
          endedAt: new Date().toISOString()
        });
        if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
        if (st.dgSocket) { try { st.dgSocket.finish(); } catch {} st.dgSocket = null; }
        if (st.stopHeartbeat) { try { st.stopHeartbeat(); } catch {} st.stopHeartbeat = null; }
        disposeAzureSynth(st);
      }
      streams.delete(sid); memory.delete(sid); clearVadState(sid);
      if (sid === activePrimarySid) activePrimarySid = null;
      return;
    }
  });

  twilioSocket.on('close', async () => {
    const sid = twilioSocket.streamSid;
    console.log('❌ Twilio close', sid);
    if (sid && sid !== activePrimarySid) {
      streams.delete(sid); memory.delete(sid); clearVadState(sid); return;
    }
    stopTTS(twilioSocket, sid, 'close');
    const st = memory.get(sid);
    if (st) {
      await pushClientMemoryUpdate({
        callerId: st.session.callerId || null,
        nombreCliente: st.session.nombreCliente || null,
        direccionFavorita: st.session.direccionFavorita || null,
        direccionConfirmada: !!st.session.direccionConfirmada,
        ultimoMensajeAsistente: st.lastReplyText || null,
        endedAt: new Date().toISOString()
      });
      if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
      if (st.dgSocket) { try { st.dgSocket.finish(); } catch {} st.dgSocket = null; }
      if (st.stopHeartbeat) { try { st.stopHeartbeat(); } catch {} st.stopHeartbeat = null; }
      disposeAzureSynth(st);
    }
    streams.delete(sid); memory.delete(sid); clearVadState(sid);
    if (sid === activePrimarySid) activePrimarySid = null;
  });
});

/* =============== KEEPALIVE =============== */
const PING_EVERY_MS = 25000;
setInterval(() => { for (const ws of wss.clients) { try { ws.ping(); } catch {} } }, PING_EVERY_MS);
wss.on('error', (err) => console.error('❌ WSS error:', err));

/* =============== HTTP + upgrade /twilio =============== */
app.server = app.listen(port, () => console.log(`🚀 Servidor escuchando en http://localhost:${port}`));
app.server.on('upgrade', (req, socket, head) => {
  if (!req.url || !req.url.startsWith('/twilio')) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

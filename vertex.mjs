// index.mjs - Asistente voice con Deepgram+Twilio+AzureTTS
// memoria/menu/promos vía n8n+Supabase

import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import { processFrame, clearVadState } from './vad.mjs';

dotenv.config();

/* =========================
   ENV HELPERS
   ========================= */
const envStr = (key, def) => {
  const v = process.env[key];
  return (v === undefined || v === null || v === '') ? def : v;
};
const envNum = (key, def) => {
  const v = process.env[key];
  if (v === undefined || v === null || v === '') return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};

/* =========================
   EXPRESS / PORT
   ========================= */
const app = express();
const port = envNum('PORT', 3000);
app.use(express.static('.'));
app.use(express.json({ limit: '10mb' }));

/* =========================
   EXTERNAL SERVICES KEYS
   ========================= */
const deepgram = createClient(envStr('DEEPGRAM_API_KEY', ''));

// Azure TTS
const AZURE_SPEECH_KEY    = envStr('AZURE_SPEECH_KEY', '');
const AZURE_SPEECH_REGION = envStr('AZURE_SPEECH_REGION', '');
const AZURE_TTS_VOICE     = envStr('AZURE_TTS_VOICE', 'es-MX-DaliaNeural');

// n8n integration
const N8N_BASE_URL       = envStr('N8N_BASE_URL', '');
const N8N_SHARED_SECRET  = envStr('N8N_SHARED_SECRET', 'pizzeriadonnapoliSUPERSECRETO');
const N8N_WEBHOOK_URL    = envStr('N8N_WEBHOOK_URL', ''); // opcional


/* =========================
   HELPERS n8n <-> Node
   ========================= */

async function callN8n(payloadObj) {
  if (!N8N_BASE_URL) {
    console.warn('⚠️ N8N_BASE_URL vacío, no se puede llamar a n8n');
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
      console.error('❌ n8n respondió error', res.status, text);
      return null;
    }

    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  } catch (e) {
    console.error('⚠️ callN8n error:', e?.message || e);
    return null;
  }
}

async function fetchClientMemory(callerId) {
  const out = await callN8n({
    action: 'get_memory',
    callerId,
  });

  if (!out || typeof out !== 'object') return {};

  const row = Array.isArray(out) ? out[0] : out;
  if (!row) return {};

  return {
    nombreCliente: row.nombre_cliente ?? null,
    direccionFavorita: row.direccion_favorita ?? null,
    direccionConfirmada: row.direccion_confirmada ?? false,
    ultimoPedido: row.ultimo_pedido ?? null,
  };
}

async function fetchMenuAndPromos() {
  const out = await callN8n({
    action: 'get_menu',
  });

  if (!out || typeof out !== 'object') {
    return { menu: [], promos: [] };
  }

  console.log('🍕 Menú recibido desde n8n:', out.menu);
  console.log('🎁 Promos recibidas desde n8n:', out.promos);

  return {
    menu: Array.isArray(out.menu) ? out.menu : [],
    promos: Array.isArray(out.promos) ? out.promos : [],
  };
}

async function pushClientMemoryUpdate(snapshot) {
  const payload = {
    action: 'update_memory',
    ...snapshot,
  };
  await callN8n(payload);
}

async function logToN8n(payload) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    fetch(N8N_WEBHOOK_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-shared-secret': N8N_SHARED_SECRET,
      },
      body: JSON.stringify(payload),
    }).catch(() => {});
  } catch {}
}


/* =========================
   WS infra
   ========================= */
const wss = new WebSocketServer({ noServer: true });

const streams = new Map(); // sid -> twilioSocket
const memory  = new Map(); // sid -> state obj

const SILENCE_FRAME = Buffer.alloc(160, 0xff); // 20ms mulaw @8khz
const RMS_BARGE_THRESHOLD = 0.05;
const BARGE_STREAK_FRAMES = 3;

// 🔐 el SID "activo" al que realmente escuchamos/respondernos
let activePrimarySid = null;


/* =========================
   STATE MGMT
   ========================= */
function ensureState(sid) {
  if (!memory.has(sid)) {
    memory.set(sid, {
      session: {
        callerId: null,
        nombreCliente: null,
        direccionFavorita: null,
        direccionConfirmada: false,
        modo: 'venta',
        step: 'saludo',
        menu: [],
        promos: [],
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
    });
  }
  return memory.get(sid);
}

function normalizeSid(v) {
  if (v == null) return '';
  return String(v).replace(/^=\s*/, '').trim();
}


/* =========================
   AUDIO UTILS
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
  for (let i = 0; i < buf.length; i++) {
    const s = ULAW_TAB[buf[i] & 0xff];
    acc += s * s;
  }
  return Math.sqrt(acc / buf.length) / 32768;
}


/* =========================
   TTS pacing & control
   ========================= */
function sendClear(ws, sid) {
  try {
    ws.send(JSON.stringify({ event: 'clear', streamSid: sid }));
  } catch {}
}

function stopTTS(ws, sid, reason='stop') {
  const st = ensureState(sid);
  if (!st.ttsActive) return;

  if (reason === 'barge') {
    logToN8n({
      streamSid: sid,
      type: 'assistant_interrupted',
      reason: 'caller_barged_in',
      ts: new Date().toISOString()
    });
  }

  if (st.ttsSender) st.ttsSender.stop();

  st.ttsActive = false;
  st.lastTtsStopTime = Date.now();

  console.log('🤫 TTS detenido (' + reason + ')', sid);

  sendClear(ws, sid);
}

function createMulawSender(
  ws,
  sid,
  frameBytes = 160,
  frameMs = 20,
  prebufferBytes = 160 * 10 // ~200ms
) {
  const st = ensureState(sid);
  let buf = Buffer.alloc(0);
  let running = false;
  let nextTick = 0;
  let timer = null;

  function tick() {
    if (!running) return;
    if (ws.readyState !== 1) { stop(); return; }

    // prebuffer
    if (
      buf.length < frameBytes ||
      (nextTick === 0 && buf.length < Math.max(prebufferBytes, frameBytes))
    ) {
      timer = setTimeout(tick, frameMs);
      return;
    }

    // backpressure Twilio
    if (ws.bufferedAmount > 64 * 1024) {
      timer = setTimeout(tick, Math.min(frameMs * 2, 50));
      return;
    }

    const slice   = buf.subarray(0, frameBytes);
    buf           = buf.subarray(frameBytes);
    const payload = slice.toString('base64');

    try {
      ws.send(JSON.stringify({
        event: 'media',
        streamSid: sid,
        media: { payload }
      }));
    } catch (e) {
      console.error('❌ WS send error (paced):', e?.message || e);
      stop();
      return;
    }

    if (nextTick === 0) nextTick = Date.now() + frameMs;
    else nextTick += frameMs;
    const wait = nextTick - Date.now();
    timer = setTimeout(tick, wait > 0 ? wait : 0);
  }

  function push(data) {
    buf = Buffer.concat([buf, data]);
    if (!running) {
      running = true;
      st.ttsActive = true;
      tick();
    }
  }

  function stop() {
    running = false;
    st.ttsActive = false;
    if (timer) { clearTimeout(timer); timer = null; }
  }

  return { push, stop };
}


/* =========================
   LLM (OpenAI) helpers
   ========================= */
function isNoiseUtterance(str) {
  const cleaned = str
    .toLowerCase()
    .replace(/[¿?¡!.,]/g,'')
    .trim();

  const tokens = cleaned.split(/\s+/).filter(Boolean);

  if (tokens.length <= 3) {
    const common = [
      'alo','aló','hola','holaa','buenas','buenas?','si','sí',
      'me','escuchas','meescuchas',
      'alo?','aló?','hola?','alooo','aloooo','dime','diga','dígame','buenas tardes','buenas?'
    ];
    if (common.includes(tokens.join(' '))) return true;
  }

  // repeticiones tipo "aló aló aló"
  const uniq = new Set(tokens);
  if (uniq.size === 1 && tokens.length <= 5) return true;

  return false;
}

function buildSystemPrompt(session) {
  const menuStr   = JSON.stringify(session.menu ?? [],   null, 2);
  const promosStr = JSON.stringify(session.promos ?? [], null, 2);

  return `
Eres el asistente telefónico de la Pizzería Don Napoli.

Datos de contexto:
- Cliente: ${session.nombreCliente ?? 'desconocido'}
- Teléfono: ${session.callerId ?? 'desconocido'}
- Dirección confirmada: ${session.direccionConfirmada ? 'sí' : 'no'}
- Dirección favorita del cliente: ${session.direccionFavorita ?? 'desconocida'}

Menú actual (pizzas, etc.):
${menuStr}

Promociones vigentes:
${promosStr}

Reglas de voz:
1. Responde en tono humano, amable y natural, máximo 2 frases cortas.
2. No repitas la misma idea si ya la dijiste en la última respuesta.
3. Solo habla cuando el cliente ya terminó de hablar (no lo interrumpas).
4. Si el cliente pregunta por precios o sabores, usa los datos de menú.
5. Si el cliente solo dice "aló", "hola", o "me escuchas", confirma que lo escuchas y pregúntale qué desea.
6. Si ya tienes una dirección guardada y confirmada, no la vuelvas a pedir.
7. Si el cliente ya está cerrando ("ya eso nomás gracias"), ofrécele despedida amable y termina.
8. Mantén todo en español latino neutro.
9. No hables en pesos habla en dolares cuando se trate de precios y vocaliza mejor.
Devuelve SOLO el texto que dirías al cliente por teléfono. Nada técnico, nada de instrucciones internas.
`.trim();
}

async function answerWithOpenAI(st, userText) {
  if (isNoiseUtterance(userText)) {
    const yaSalude = (st.session.step !== 'saludo');
    const canned = yaSalude
      ? 'Sí, te escucho claro. ¿Qué te gustaría pedir?'
      : 'Pizzería Don Napoli, hola ¿Qué te gustaría pedir hoy?';
    st.lastReplyText = canned;
    return canned;
  }

  const recentHistory = st.history.slice(-10);

  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${envStr('OPENAI_API_KEY', '')}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      temperature: 0.45,
      max_tokens: 90,
      messages: [
        { role: 'system', content: buildSystemPrompt(st.session) },
        ...recentHistory,
        { role: 'user', content: userText },
        st.lastReplyText
          ? { role: 'system', content: `No repitas literalmente tu última respuesta: "${st.lastReplyText}". Di algo nuevo si es posible.` }
          : null,
      ].filter(Boolean),
    }),
  });

  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
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
   Azure TTS
   ========================= */
function buildSSMLFromText(text) {
  const style  = envStr('AZURE_TTS_STYLE', 'customerservice');
  const rate   = envStr('AZURE_TTS_RATE', '1.3');
  const pitch  = envStr('AZURE_TTS_PITCH', '+2%');
  const volume = envStr('AZURE_TTS_VOLUME', 'default');
  const pause  = envNum('AZURE_TTS_PAUSE_MS', 250);
  const voice  = envStr('AZURE_TTS_VOICE', AZURE_TTS_VOICE);

  const esc = (s) => String(s)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;');

  const sentences = esc(text)
    .split(/(?<=[\.\!\?…])\s+/)
    .map(s => s.trim())
    .filter(Boolean);

  const body = sentences.map((s, i) => {
    const br = (i < sentences.length - 1) ? `<break time="${pause}ms"/>` : '';
    return `${s} ${br}`.trim();
  }).join(' ');

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

async function speakWithAzureTTS(ws, sid, text) {
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

    const st = ensureState(sid);

    if (!st.ttsSender) {
      st.ttsSender = createMulawSender(ws, sid, 160, 20, 160 * 10);
    }

    sendClear(ws, sid);

    const buf = Buffer.from(await tts.arrayBuffer());
    if (!buf || buf.length === 0) {
      console.warn('⚠️ Azure TTS devolvió audio vacío');
      return;
    }

    const CHUNK = 160;
    for (let i = 0; i < buf.length; i += CHUNK) {
      const slice = buf.subarray(i, Math.min(i + CHUNK, buf.length));
      st.ttsSender?.push(slice);
    }

    st.lastAssistantReplySent = text;
  } catch (err) {
    console.error('❌ speakWithAzureTTS exception', err);
  }
}


/* =========================
   TURN LOGIC
   ========================= */
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

    if (humanText === st.lastUserTurnAnsweredText) {
      return;
    }

    st.history.push({ role: 'user', content: humanText });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

    logToN8n({
      streamSid: sid,
      type: 'user_turn_finalized',
      role: 'user',
      mensaje: humanText,
      ts: new Date().toISOString()
    });

    const reply = await answerWithOpenAI(st, humanText);
    console.log('🧠 Asistente (LLM):', reply);

    if (reply === st.lastAssistantReplySent) {
      st.lastUserTurnHandled      = humanText;
      st.lastUserTurnAnsweredText = humanText;
      st.partialBuffer            = '';
      return;
    }

    st.history.push({ role: 'assistant', content: reply });
    if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

    logToN8n({
      streamSid: sid,
      type: 'assistant_reply',
      role: 'assistant',
      mensaje: reply,
      ts: new Date().toISOString()
    });

    stopTTS(twilioSocket, sid, 'before_new_reply');
    await speakWithAzureTTS(twilioSocket, sid, reply);

    st.lastUserTurnHandled      = humanText;
    st.lastUserTurnAnsweredText = humanText;
    st.partialBuffer            = '';

  } finally {
    st.handlingTurn = false;
  }
}


/* =========================
   HEARTBEAT Deepgram
   ========================= */
function startDeepgramHeartbeat(dgSocket) {
  const HEARTBEAT_MS = 1000;
  let alive = true;

  const loop = () => {
    if (!alive) return;
    try {
      dgSocket.send(SILENCE_FRAME);
    } catch (e) {
      alive = false;
      return;
    }
    setTimeout(loop, HEARTBEAT_MS);
  };

  loop();
  return () => { alive = false; };
}


/* =========================
   Twilio WS handler
   ========================= */
wss.on('connection', async (twilioSocket, req) => {
  console.log('📞 Conexión de Twilio establecida desde', req?.socket?.remoteAddress, 'url:', req?.url);

  twilioSocket.on('ping', () => { try { twilioSocket.pong(); } catch {} });
  twilioSocket.on('error', e => console.error('❌ WS client error:', e?.message || e));

  twilioSocket.on('message', async (raw) => {
    const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    let msg; try { msg = JSON.parse(str); } catch { return; }

    /* ====== START EVENT ====== */
    if (msg.event === 'start') {
      const sid = normalizeSid(msg?.start?.streamSid);
      if (!sid) {
        console.warn('⚠️ start sin streamSid válido');
        return;
      }

      // 👇 NUEVA POLÍTICA:
      // el SID más reciente SIEMPRE es el primary
      activePrimarySid = sid;
      console.log('🌟 Este SID es ahora el primary:', sid);

      twilioSocket.streamSid = sid;
      streams.set(sid, twilioSocket);

      const st = ensureState(sid);
      console.log('▶️ start streamSid:', sid, '| activos:', streams.size);

      st.session.callerId =
        msg.start?.from ||
        msg.start?.caller ||
        msg.start?.callSid ||
        'desconocido';

      // si ya había un dgSocket en el estado (de un SID anterior) reutilízalo,
      // porque Deepgram ya está escuchando la llamada real.
      if (!st.dgSocket) {
        // no tenemos deepgram en este SID todavía, así que lo creamos nuevo
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
          console.log('🎤 Conexión con Deepgram abierta para', sid);

          const stopHb = startDeepgramHeartbeat(dgSocket);
          st.stopHeartbeat = stopHb;

          // SALUDO inicial solo si no hemos saludado antes EN ESTA SESIÓN LÓGICA
          if (!st.hasGreeted) {
            const saludoInicial = st.session.nombreCliente
              ? `Hola ${st.session.nombreCliente}, bienvenido otra vez a Pizzería Don Napoli. ¿Qué deseas pedir hoy?`
              : 'Hola, somos la pizzeria don napoli. ¿Qué te gustaría pedir hoy?';

            logToN8n({
              streamSid: sid,
              type: 'assistant_auto_greeting',
              role: 'assistant',
              mensaje: saludoInicial,
              ts: new Date().toISOString()
            });

            st.history.push({ role: 'assistant', content: saludoInicial });
            if (st.history.length > 30) st.history.splice(0, st.history.length - 30);

            st.lastReplyText = saludoInicial;
            st.lastAssistantReplySent = saludoInicial;

            st.session.step = 'en_conversacion';
            st.hasGreeted = true;

            stopTTS(twilioSocket, sid, 'before_greeting');
            await speakWithAzureTTS(twilioSocket, sid, saludoInicial);
          }

          // precargar memoria y menú/promos (solo 1 vez realmente útil)
          (async () => {
            const [memData, menuData] = await Promise.all([
              fetchClientMemory(st.session.callerId),
              fetchMenuAndPromos(),
            ]);

            if (memData && typeof memData === 'object') {
              if (memData.nombreCliente)        st.session.nombreCliente        = memData.nombreCliente;
              if (memData.direccionFavorita)    st.session.direccionFavorita    = memData.direccionFavorita;
              if (memData.direccionConfirmada)  st.session.direccionConfirmada  = !!memData.direccionConfirmada;
            }

            st.session.menu   = menuData?.menu   || [];
            st.session.promos = menuData?.promos || [];

            console.log('🍕 Menú recibido desde n8n:', st.session.menu);
            console.log('🎁 Promos recibidas desde n8n:', st.session.promos);
          })().catch(err => {
            console.error('⚠️ Error precargando memoria/menú async:', err?.message || err);
          });
        });

        dgSocket.on(LiveTranscriptionEvents.Transcript, async (data) => {
          // SOLO respondemos al SID que actualmente es primary
          if (sid !== activePrimarySid) return;

          const alt = data?.channel?.alternatives?.[0];
          const rawTxt = (alt?.transcript || '').trim();
          if (!rawTxt) return;

          const isFinal = data?.is_final || data?.speech_final;
          const normalized = rawTxt.replace(/\s+/g, ' ').trim();

          st.partialBuffer = normalized;

          console.log(isFinal ? '🗣️ Cliente (final):' : '🗣️ Cliente (parcial):', normalized);

          logToN8n({
            streamSid: sid,
            type: isFinal ? 'user_final_raw' : 'user_partial',
            role: 'user',
            mensaje: normalized,
            ts: new Date().toISOString()
          });

          if (isFinal) {
            if (st.silenceTimer) {
              clearTimeout(st.silenceTimer);
              st.silenceTimer = null;
            }
            await handleCompleteUserTurn(twilioSocket, sid);
          }
        });

        dgSocket.on(LiveTranscriptionEvents.Error, (err) => {
          console.error('❌ Error Deepgram:', err);
        });

        dgSocket.on(LiveTranscriptionEvents.Close, () => {
          console.log('📴 Deepgram dijo Close (lo ignoramos hasta hangup) para', sid);
        });
      } else {
        // ya había dgSocket (por un SID anterior), o sea venimos de un swap:
        console.log('🔁 Reusando Deepgram / estado existente para SID nuevo', sid);
      }

      return;
    }

    /* ====== MEDIA EVENT ====== */
    if (msg.event === 'media') {
      const sid = twilioSocket.streamSid;
      if (!sid) return;

      // SOLO escuchamos el SID que es primary EN ESTE MOMENTO
      if (sid !== activePrimarySid) {
        return;
      }

      const st = ensureState(sid);

      const track = msg?.track || 'inbound';
      if (track !== 'inbound') return;

      const payload   = msg.media?.payload || '';
      const audioUlaw = Buffer.from(payload, 'base64');
      if (audioUlaw.length === 0) return;

      // barge-in
      const vadInfoOpenLike = processFrame(sid, audioUlaw); // { open, rms, ... }
      const callerTalking   = !!vadInfoOpenLike.open;
      if (st.ttsActive) {
        if (callerTalking) {
          st.bargeStreak = (st.bargeStreak || 0) + 1;
        } else {
          const frameRms = vadInfoOpenLike.rms ?? ulawRms(audioUlaw);
          if (frameRms >= RMS_BARGE_THRESHOLD) {
            st.bargeStreak = (st.bargeStreak || 0) + 1;
          } else {
            st.bargeStreak = 0;
          }
        }
        if (st.bargeStreak >= BARGE_STREAK_FRAMES) {
          stopTTS(twilioSocket, sid, 'barge');
          st.bargeStreak = 0;
        }
      } else {
        st.bargeStreak = 0;
      }

      // mandar audio a Deepgram
      if (st.dgSocket) {
        try {
          st.dgSocket.send(audioUlaw);
        } catch (e) {
          console.error('❌ Error enviando audio a Deepgram para', sid, e?.message || e);
        }
      }

      return;
    }

    /* ====== STOP EVENT ====== */
    if (msg.event === 'stop') {
      const sid = twilioSocket.streamSid;
      console.log('⏹️ Twilio envió stop para', sid);

      // si este SID YA NO es el primary actual,
      // es un socket viejo => limpieza ligera nomás
      if (sid !== activePrimarySid) {
        console.log('⏭️ stop de socket viejo, ignoro cleanup global');
        streams.delete(sid);
        memory.delete(sid);
        clearVadState(sid);
        return;
      }

      // este sí es el primary actual => cleanup real y colgamos sesión
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

        if (st.silenceTimer) {
          clearTimeout(st.silenceTimer);
          st.silenceTimer = null;
        }

        if (st.dgSocket) {
          try { st.dgSocket.finish(); } catch {}
          st.dgSocket = null;
        }
        if (st.stopHeartbeat) {
          try { st.stopHeartbeat(); } catch {}
          st.stopHeartbeat = null;
        }
      }

      streams.delete(sid);
      memory.delete(sid);
      clearVadState(sid);

      // ya no hay llamada activa
      if (sid === activePrimarySid) {
        activePrimarySid = null;
      }

      return;
    }
  });

  twilioSocket.on('close', async () => {
    const sid = twilioSocket.streamSid;
    console.log('❌ Twilio cerró la conexión', sid);

    // si este socket NO es el primary actual
    // => era un socket viejo, límpialo suave y ya
    if (sid && sid !== activePrimarySid) {
      console.log('⏭️ close de socket viejo, no cierro la sesión activa');
      streams.delete(sid);
      memory.delete(sid);
      clearVadState(sid);
      return;
    }

    // este sí era el primary actual => cleanup total
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

      if (st.silenceTimer) {
        clearTimeout(st.silenceTimer);
        st.silenceTimer = null;
      }

      if (st.dgSocket) {
        try { st.dgSocket.finish(); } catch {}
        st.dgSocket = null;
      }
      if (st.stopHeartbeat) {
        try { st.stopHeartbeat(); } catch {}
        st.stopHeartbeat = null;
      }
    }

    streams.delete(sid);
    memory.delete(sid);
    clearVadState(sid);

    if (sid === activePrimarySid) {
      activePrimarySid = null;
    }
  });
});


/* =========================
   KEEPALIVE WSS
   ========================= */
const PING_EVERY_MS = 25000;
setInterval(() => {
  for (const ws of wss.clients) {
    try { ws.ping(); } catch {}
  }
}, PING_EVERY_MS);

wss.on('error', (err) => console.error('❌ WSS error:', err));


/* =========================
   HTTP server + upgrade WS /twilio
   ========================= */
app.server = app.listen(port, () => {
  console.log(`🚀 Servidor escuchando en http://localhost:${port}`);
});

app.server.on('upgrade', (req, socket, head) => {
  if (!req.url || !req.url.startsWith('/twilio')) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

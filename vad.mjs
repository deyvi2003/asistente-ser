// vad.js
// VAD minimalista con histéresis por streamSid.
// No bloquea audio, no afecta Deepgram.
// Solo nos dice si el humano está Hablando (open=true) o Silencio (open=false).

const VAD_DEFAULTS = {
  MIN_OPEN_FRAMES: 10,     // ~200ms de voz consistente para "abrir"
  HANG_FRAMES: 10,         // ~200ms de cola antes de "cerrar"
  SNR_OPEN_DB: 10,         // abrir si SNR >= 10 dB
  SNR_CLOSE_DB: 7,         // cerrar si SNR < 7 dB
  ABS_RMS_OPEN: 0.025,     // energía mínima para abrir
  ABS_RMS_CLOSE: 0.015,    // energía mínima para mantener
  BOOT_GRACE_MS: 1200,     // primeros ~1.2s: somos más permisivos
};

// μ-law byte -> Int16 PCM approx
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
  return Math.sqrt(acc / buf.length) / 32768; // [0..1]
}

// Estado interno del VAD por streamSid
// st = {
//   open: bool,
//   hang: number,
//   openStreak: number,
//   noiseRms: number,
//   lastRms: number,
//   lastSnrDb: number,
//   firstAt: number,
// }
const vadMap = new Map();

function ensureVadState(sid) {
  if (!vadMap.has(sid)) {
    vadMap.set(sid, {
      open: false,
      hang: 0,
      openStreak: 0,
      noiseRms: 0.003,
      lastRms: 0,
      lastSnrDb: 0,
      firstAt: Date.now(),
    });
  }
  return vadMap.get(sid);
}

// Llamar en cada frame de audio μ-law del cliente (~20ms, 160 bytes @8k).
// Devuelve { open, rms, snrDb }.
function processFrame(sid, audioUlaw, opts = {}) {
  const cfg = { ...VAD_DEFAULTS, ...opts };
  const st = ensureVadState(sid);

  const rms = ulawRms(audioUlaw);

  // actualiza "ruido base" si estamos cerrados y el frame tiene energía baja
  if (!st.open && rms < cfg.ABS_RMS_OPEN) {
    st.noiseRms = 0.94 * st.noiseRms + 0.06 * Math.max(rms, 1e-5);
  }
  const noise = Math.max(st.noiseRms, 1e-5);
  const snrDb = 20 * Math.log10((rms + 1e-6) / noise);

  st.lastRms = rms;
  st.lastSnrDb = snrDb;

  const sinceStartMs = Date.now() - (st.firstAt || Date.now());

  const openCond = (rms >= cfg.ABS_RMS_OPEN) && (snrDb >= cfg.SNR_OPEN_DB);
  const closeCond = (rms < cfg.ABS_RMS_CLOSE) || (snrDb < cfg.SNR_CLOSE_DB);

  const bootOpenCond =
    (sinceStartMs <= cfg.BOOT_GRACE_MS) &&
    (rms >= Math.max(0.8 * cfg.ABS_RMS_OPEN, 0.02));

  if (!st.open) {
    if (openCond || bootOpenCond) {
      st.openStreak = Math.min(st.openStreak + 1, cfg.MIN_OPEN_FRAMES);
      if (st.openStreak >= cfg.MIN_OPEN_FRAMES) {
        st.open = true;
        st.hang = cfg.HANG_FRAMES;
      }
    } else {
      st.openStreak = 0;
    }
  } else {
    if (closeCond) {
      st.hang = Math.max(0, st.hang - 1);
      if (st.hang === 0) {
        st.open = false;
        st.openStreak = 0;
      }
    } else {
      st.hang = cfg.HANG_FRAMES;
    }
  }

  return {
    open: st.open,
    rms: st.lastRms,
    snrDb: st.lastSnrDb,
  };
}

// para debug o métricas
function getVadState(sid) {
  return vadMap.get(sid) || null;
}

// limpiar cuando cuelga la llamada
function clearVadState(sid) {
  vadMap.delete(sid);
}

export {
  processFrame,
  getVadState,
  clearVadState,
};

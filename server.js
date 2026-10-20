'use strict';
/**
 * Video Downloader backend (Node.js, no npm packages needed - Node 18+).
 * Talks to the "Social Media Video Downloader" API on RapidAPI.
 * The RapidAPI key lives ONLY on the server (environment variable RAPIDAPI_KEY).
 *
 *   GET /health
 *   GET /info?url=...          -> { title, thumbnail, duration, formats[], default }
 *   GET /start?url=...&res=... -> { id }            (starts a download job)
 *   GET /status/<id>           -> { state, percent } (queued | downloading | processing | done | error)
 *   GET /file/<id>             -> the finished file
 *   GET /                      -> public/index.html (the website)
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { Readable } = require('stream');
const { once } = require('events');

// ---------------------------------------------------------------- settings
try { // tiny .env loader (optional, for running on your own computer)
  for (const line of fs.readFileSync(path.join(__dirname, '.env'), 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
} catch (e) { /* no .env file - fine */ }

const num = (n, d) => { const v = parseInt(process.env[n], 10); return Number.isFinite(v) ? v : d; };
const ENV = process.env;
const PORT = num('PORT', 8000);
const API_KEY = (ENV.RAPIDAPI_KEY || '').trim();
const API_HOST = (ENV.RAPIDAPI_HOST || 'social-media-video-downloader.p.rapidapi.com').trim();
const API_BASE = (ENV.API_BASE_URL || ('https://' + API_HOST)).replace(/\/+$/, '');
const ALLOWED_ORIGINS = (ENV.ALLOWED_ORIGINS || '*').split(',').map(s => s.trim()).filter(Boolean);
const MAX_FILESIZE = num('MAX_FILESIZE_MB', 500) * 1048576;
const MAX_CONCURRENT = num('MAX_CONCURRENT', 3);
const MAX_ACTIVE_IP = num('MAX_ACTIVE_PER_IP', 2);
const JOB_TTL = num('JOB_TTL_SEC', 900) * 1000;
const INFO_PER_MIN = num('INFO_PER_MIN', 10);
const START_PER_MIN = num('START_PER_MIN', 4);
const DAILY_API_LIMIT = num('DAILY_API_LIMIT', 400);   // protects your RapidAPI quota
const INFO_CACHE_MS = num('INFO_CACHE_SEC', 600) * 1000;
const ADMIN_KEY = (ENV.ADMIN_KEY || '').trim();
const TRUST_PROXY = ENV.TRUST_PROXY !== '0';
const WORK_DIR = ENV.WORK_DIR || path.join(os.tmpdir(), 'vd-jobs');
const PUBLIC_DIR = path.join(__dirname, 'public');

fs.mkdirSync(WORK_DIR, { recursive: true });
for (const d of fs.readdirSync(WORK_DIR)) fs.rmSync(path.join(WORK_DIR, d), { recursive: true, force: true });

class UserError extends Error { constructor(msg, code) { super(msg); this.code = code || 400; } }
const log = (...a) => console.log(new Date().toISOString(), ...a);

// ---------------------------------------------------------------- small helpers
const hits = new Map();
function limited(kind, ip, perMin) {
  const k = kind + '|' + ip, now = Date.now();
  const arr = (hits.get(k) || []).filter(t => now - t < 60000);
  if (arr.length >= perMin) { hits.set(k, arr); return true; }
  arr.push(now); hits.set(k, arr); return false;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.some(t => now - t < 60000)) hits.delete(k); }, 60000).unref();

let budget = { day: new Date().toISOString().slice(0, 10), used: 0 };
function spend(cost) {
  const today = new Date().toISOString().slice(0, 10);
  if (budget.day !== today) budget = { day: today, used: 0 };
  if (budget.used + cost > DAILY_API_LIMIT) throw new UserError('Daily limit reached. Please try again tomorrow.', 429);
  budget.used += cost;
}

const clientIp = req => (TRUST_PROXY && String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || 'unknown';

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}
const fail = (res, code, msg) => sendJson(res, code, { error: msg });

function applyCors(req, res) {
  const origin = req.headers.origin || '';
  if (ALLOWED_ORIGINS.includes('*')) res.setHeader('Access-Control-Allow-Origin', '*');
  else if (origin && ALLOWED_ORIGINS.includes(origin)) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
  res.setHeader('Access-Control-Expose-Headers', 'Content-Disposition');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store');
}

// ---------------------------------------------------------------- link parsing
function parseLink(raw) {
  let u; try { u = new URL(String(raw || '').trim()); } catch (e) { return { error: 'Enter a valid link (starting with https://).' }; }
  if (!/^https?:$/.test(u.protocol) || String(raw).length > 2000) return { error: 'Enter a valid link (starting with https://).' };
  const h = u.hostname.toLowerCase().replace(/^(www|m|mobile|music|web)\./, '');
  if (h === 'youtu.be' || h === 'youtube.com' || h === 'youtube-nocookie.com') {
    let id = null;
    if (h === 'youtu.be') id = u.pathname.split('/')[1];
    else if (u.pathname === '/watch') id = u.searchParams.get('v');
    else { const m = u.pathname.match(/^\/(?:shorts|embed|live|v)\/([^/?#]+)/); if (m) id = m[1]; }
    if (!id || !/^[\w-]{11}$/.test(id)) return { error: 'This YouTube link does not point to a single video.' };
    return { platform: 'youtube', id, key: 'yt:' + id };
  }
  if (h === 'facebook.com' || h === 'fb.watch' || h === 'fb.com' || h.endsWith('.facebook.com')) return { platform: 'facebook', url: u.href, key: 'fb:' + u.href };
  if (h === 'instagram.com' || h.endsWith('.instagram.com')) {
    const m = u.pathname.match(/\/(?:p|reel|reels|tv)\/([\w-]+)/);
    if (!m) return { error: 'This Instagram link does not point to a post or reel.' };
    return { platform: 'instagram', id: m[1], key: 'ig:' + m[1] };
  }
  if (h === 'tiktok.com' || h.endsWith('.tiktok.com')) return { platform: 'tiktok', url: u.href, key: 'tt:' + u.href };
  if (h === 'twitter.com' || h === 'x.com' || h === 't.co') return { error: 'X (Twitter) links are not supported by this service yet.' };
  return { error: 'This website is not supported. Supported: YouTube, Facebook, Instagram, TikTok.' };
}

// ---------------------------------------------------------------- RapidAPI calls
function endpoint(link, o) {
  o = o || {};
  switch (link.platform) {
    case 'youtube': {
      const p = { videoId: link.id, urlAccess: o.proxied ? 'proxied' : 'normal', getTranscript: 'false' };
      if (o.render) p.renderableFormats = o.render;
      return ['/youtube/v3/video/details', p];
    }
    case 'facebook': { const p = { url: link.url }; if (o.render) p.renderableFormats = o.render; return ['/facebook/v3/post/details', p]; }
    case 'instagram': { const p = { shortcode: link.id }; if (o.render) p.renderableFormats = o.render; return ['/instagram/v3/media/post/details', p]; }
    case 'tiktok': return ['/tiktok/v3/post/details', { url: link.url }];
  }
  throw new UserError('Unsupported link.');
}

async function callApi(link, o) {
  if (!API_KEY) throw new UserError('The server is not set up yet (API key missing).', 503);
  const [p, params] = endpoint(link, o);
  spend((o && (o.proxied || o.render)) ? 2 : 1);
  const url = new URL(API_BASE + p);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  let r, text;
  try {
    r = await fetch(url, { headers: { 'x-rapidapi-key': API_KEY, 'x-rapidapi-host': API_HOST, Accept: 'application/json' }, signal: AbortSignal.timeout(70000) });
    text = await r.text();
  } catch (e) {
    throw new UserError('The download service did not respond. Please try again.', 504);
  }
  let json = null; try { json = JSON.parse(text); } catch (e) { /* not json */ }
  if (!r.ok) {
    log('API error', r.status, String(text).slice(0, 300));
    if (r.status === 401 || r.status === 403) throw new UserError('The server API key is not working. Please contact the site owner.', 502);
    if (r.status === 429) throw new UserError('The service is busy right now. Please try again in a minute.', 429);
    if (r.status === 404 || r.status === 400 || r.status === 422) throw new UserError('This video could not be found. It may be private, removed or not supported.', 422);
    throw new UserError('The download service had a problem. Please try again.', 502);
  }
  if (!json || typeof json !== 'object') throw new UserError('The download service returned an unexpected answer.', 502);
  return json;
}

const infoCache = new Map();
async function getInfo(link) {
  const c = infoCache.get(link.key);
  if (c && Date.now() - c.t < INFO_CACHE_MS) return c.data;
  const data = await callApi(link, { proxied: false });
  infoCache.set(link.key, { t: Date.now(), data });
  if (infoCache.size > 300) infoCache.delete(infoCache.keys().next().value);
  return data;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of infoCache) if (now - v.t > INFO_CACHE_MS) infoCache.delete(k); }, 120000).unref();

// ---------------------------------------------------------------- reading the API answer (tolerant of different layouts)
const str = v => (typeof v === 'string' ? v : '');
const numOf = v => { const n = typeof v === 'string' ? parseFloat(v) : v; return typeof n === 'number' && isFinite(n) ? n : 0; };
const isHttp = s => /^https?:\/\//i.test(s);

function kindOf(key) {
  const k = String(key).toLowerCase();
  if (k.includes('renderablevideo')) return 'rvideos';
  if (k.includes('renderableaudio')) return 'raudios';
  if (k === 'videos' || k === 'video') return 'videos';
  if (k === 'audios' || k === 'audio') return 'audios';
  return null;
}

function extFromMime(m) {
  m = m.toLowerCase();
  if (m.includes('mp4a') && m.startsWith('audio')) return 'm4a';
  if (m.startsWith('audio/mp4') || m.startsWith('audio/m4a') || m.startsWith('audio/x-m4a')) return 'm4a';
  if (m.startsWith('audio/webm')) return 'webm';
  if (m.startsWith('audio/mpeg') || m.startsWith('audio/mp3')) return 'mp3';
  if (m.startsWith('video/mp4')) return 'mp4';
  if (m.startsWith('video/webm')) return 'webm';
  if (m.startsWith('video/quicktime')) return 'mov';
  return '';
}
function extFromUrl(u) { try { const m = new URL(u).pathname.match(/\.([a-z0-9]{2,4})$/i); return m ? m[1].toLowerCase() : ''; } catch (e) { return ''; } }

function normItem(it, kind, platform) {
  const m = Object.assign({}, it.metadata && typeof it.metadata === 'object' ? it.metadata : {}, it);
  let url = '';
  for (const k of ['url', 'src', 'link', 'downloadUrl', 'download_url', 'href']) if (isHttp(str(m[k]))) { url = m[k]; break; }
  if (!url) return null;
  const mime = str(m.mimeType || m.mime || m.mime_type || m.contentType || m.type);
  const qlabel = str(m.qualityLabel || m.quality || m.label || m.resolution || m.format);
  let w = numOf(m.width), h = numOf(m.height);
  let q = 0;
  if (w && h) q = Math.min(w, h);
  else if (h) q = h;
  else if (w) q = w;
  if (!q) { const mm = qlabel.match(/(\d{3,4})\s*p/i) || qlabel.match(/\d{3,4}x(\d{3,4})/i); if (mm) q = parseInt(mm[1], 10); }
  if (!q && /^2160|4k/i.test(qlabel)) q = 2160;
  if (!q) { const l = qlabel.toLowerCase(); if (/\bhd\b|^hd/.test(l) && !/sd/.test(l)) q = 720; else if (/\bsd\b|^sd/.test(l)) q = 360; }
  const ext = (str(m.extension || m.ext || m.container) || extFromMime(mime) || extFromUrl(url)).toLowerCase().replace(/^\./, '');
  let hasAudio = null;
  for (const k of ['hasAudio', 'has_audio', 'audio']) if (typeof m[k] === 'boolean') { hasAudio = m[k]; break; }
  if (hasAudio === null) {
    const codecs = (mime + ' ' + str(m.codecs) + ' ' + str(m.acodec) + ' ' + str(m.audioCodec)).toLowerCase();
    if (/mp4a|opus|aac|vorbis|ac-3|ec-3/.test(codecs)) hasAudio = true;
    else if (str(m.audioQuality) || str(m.audioSampleRate) || numOf(m.audioChannels)) hasAudio = true;
    else if (/codecs/.test(codecs) && /avc1|vp0?9|av01|hev1|hvc1/.test(codecs)) hasAudio = false;
  }
  if (kind === 'rvideos' || kind === 'raudios') hasAudio = true;
  if (kind === 'videos' && hasAudio === null && platform === 'youtube' && q > 360) hasAudio = false;
  const size = numOf(m.size || m.contentLength || m.content_length || m.filesize || m.bytes || m.approxSize);
  const bitrate = numOf(m.bitrate || m.averageBitrate || m.audioBitrate || m.bitRate);
  return { url, ext, q, qlabel, hasAudio, size, bitrate, mime, kind };
}

function extractMedia(json, platform) {
  const out = { videos: [], audios: [], rvideos: [], raudios: [] };
  const seen = new Set();
  const restrict = json && typeof json === 'object' && 'contents' in json;
  (function walk(node, trail, depth) {
    if (depth > 9 || node == null || typeof node !== 'object') return;
    if (Array.isArray(node)) {
      const kind = kindOf(trail[trail.length - 1] || '');
      const okPath = restrict ? trail.includes('contents') : !trail.some(t => /metadata|related|comment|playlist/i.test(t));
      if (kind && okPath) for (const it of node) {
        if (it && typeof it === 'object') { const n = normItem(it, kind, platform); if (n && !seen.has(kind + n.url)) { seen.add(kind + n.url); out[kind].push(n); } }
      }
      node.forEach((v, i) => walk(v, trail.concat(String(i)), depth + 1));
      return;
    }
    for (const [k, v] of Object.entries(node)) walk(v, trail.concat(k), depth + 1);
  })(json, [], 0);
  if (!out.videos.length && !out.audios.length && !out.rvideos.length && !out.raudios.length) { // last resort: any object that looks like a media link
    (function scan(node, depth) {
      if (depth > 9 || node == null || typeof node !== 'object') return;
      if (!Array.isArray(node) && isHttp(str(node.url)) && (node.mimeType || node.quality || node.height || node.qualityLabel || node.extension)) {
        const isAudio = /^audio/i.test(str(node.mimeType || node.type)) || /audio/i.test(str(node.quality));
        const n = normItem(node, isAudio ? 'audios' : 'videos', platform);
        if (n && !seen.has(n.kind + n.url)) { seen.add(n.kind + n.url); out[n.kind].push(n); }
      }
      for (const v of Object.values(node)) scan(v, depth + 1);
    })(json, 0);
  }
  return out;
}

function deepFirst(json, keys, test) { // breadth-first, so top-level / metadata values win
  const queue = [json]; let n = 0;
  while (queue.length && n++ < 4000) {
    const node = queue.shift();
    if (node == null || typeof node !== 'object') continue;
    if (!Array.isArray(node)) for (const k of keys) if (k in node) { const v = node[k]; if (test(v)) return v; }
    for (const v of (Array.isArray(node) ? node : Object.values(node))) if (v && typeof v === 'object') queue.push(v);
  }
  return null;
}
function readMeta(json, platform) {
  const title = deepFirst(json, ['title', 'name', 'caption', 'description'], v => typeof v === 'string' && v.trim()) || '';
  let thumb = deepFirst(json, ['thumbnails', 'thumbnail', 'thumbnailUrl', 'cover', 'image', 'poster', 'images'], v =>
    (typeof v === 'string' && isHttp(v)) || (Array.isArray(v) && v.some(x => x && isHttp(str(x.url || x)))) || (v && typeof v === 'object' && !Array.isArray(v) && isHttp(str(v.url))));
  if (Array.isArray(thumb)) {
    const arr = thumb.map(x => (typeof x === 'string' ? { url: x } : x)).filter(x => x && isHttp(str(x.url)));
    arr.sort((a, b) => numOf(a.width) - numOf(b.width));
    thumb = (arr.find(x => numOf(x.width) >= 320) || arr[arr.length - 1] || {}).url || '';
  } else if (thumb && typeof thumb === 'object') thumb = thumb.url;
  let dur = deepFirst(json, ['duration', 'durationSeconds', 'lengthSeconds', 'length', 'durationMs', 'duration_ms'], v => v !== null && v !== '' && v !== undefined && typeof v !== 'object');
  let seconds = 0;
  if (typeof dur === 'number' || /^\d+(\.\d+)?$/.test(String(dur))) { seconds = parseFloat(dur); if (seconds > ((platform === 'tiktok' || platform === 'instagram') ? 4000 : 100000)) seconds /= 1000; }
  else if (typeof dur === 'string') {
    const iso = dur.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/i);
    if (iso) seconds = (+iso[1] || 0) * 3600 + (+iso[2] || 0) * 60 + (+iso[3] || 0);
    else if (/^\d+(:\d+){1,2}$/.test(dur)) seconds = dur.split(':').reduce((a, b) => a * 60 + (+b), 0);
  }
  return { title: String(title).replace(/\s+/g, ' ').trim().slice(0, 150), thumbnail: isHttp(str(thumb)) ? thumb : '', duration: seconds ? Math.round(seconds) : null };
}

const labelOf = q => (q >= 2000 ? '4K' : q + 'p');
function buildLabels(media) {
  const heights = new Set();
  for (const v of [...media.videos, ...media.rvideos]) if (v.q) heights.add(v.q);
  const list = [...heights].sort((a, b) => a - b).map(labelOf);
  const hasVideo = media.videos.length + media.rvideos.length > 0;
  if (!list.length && hasVideo) list.push('Best quality');
  const hasAudio = media.audios.length + media.raudios.length > 0;
  return { video: list, audio: hasAudio };
}
function parseRes(res) {
  if (/^audio/i.test(res)) return { audio: true };
  if (/^best/i.test(res)) return { height: 99999 };
  if (/^4k$/i.test(res)) return { height: 2160 };
  const m = res.match(/(\d{3,4})p/i); if (m) return { height: parseInt(m[1], 10) };
  return { height: 99999 };
}
// best option at or below the wanted height (if none, the smallest above it)
function pickByHeight(list, h) {
  if (!list.length) return null;
  const score = x => (x.ext === 'mp4' ? 2 : 0) + (x.size ? 1 : 0);
  const below = list.filter(x => !x.q || x.q <= h);
  const pool = below.length ? below : [list.slice().sort((a, b) => a.q - b.q)[0]];
  return pool.slice().sort((a, b) => (b.q - a.q) || (score(b) - score(a)) || (b.size - a.size))[0];
}
function pickAudio(list) {
  return list.slice().sort((a, b) => (b.bitrate - a.bitrate) || ((b.ext === 'm4a') - (a.ext === 'm4a')) || (b.size - a.size))[0] || null;
}

// ---------------------------------------------------------------- find the file to download
async function resolveMedia(link, res) {
  const want = parseRes(res);
  const info = await getInfo(link);
  let media = extractMedia(info, link.platform);
  const canRender = link.platform === 'youtube' || link.platform === 'facebook' || link.platform === 'instagram';

  if (want.audio) {
    if (link.platform === 'youtube') media = extractMedia(await callApi(link, { proxied: true }), link.platform);
    const a = pickAudio([...media.raudios, ...media.audios]);
    if (!a) throw new UserError('No audio-only file is available for this video.', 422);
    return a;
  }

  const withAudio = [...media.rvideos, ...media.videos.filter(v => v.hasAudio !== false)];
  const bestProg = pickByHeight(withAudio, want.height);
  const videoOnly = media.videos.filter(v => v.hasAudio === false && v.q && v.q <= want.height);
  const bestAdaptive = videoOnly.sort((a, b) => b.q - a.q)[0];
  const needRender = canRender && bestAdaptive && (!bestProg || bestAdaptive.q > (bestProg.q || 0));

  if (link.platform === 'youtube' || needRender) {
    const opts = { proxied: link.platform === 'youtube' };
    if (needRender) opts.render = labelOf(bestAdaptive.q).replace('4K', '2160p');
    try {
      const m2 = extractMedia(await callApi(link, opts), link.platform);
      const pool2 = [...m2.rvideos, ...m2.videos.filter(v => v.hasAudio !== false)];
      const pick2 = pickByHeight(pool2, want.height);
      if (pick2) return pick2;
    } catch (e) {
      if (!(e instanceof UserError) || e.code === 429 || !bestProg) throw e;
      log('second call failed, using the ready-made file instead:', e.message);
    }
  }
  if (bestProg) return bestProg;
  if (bestAdaptive) throw new UserError('This quality has no sound available. Please choose a lower quality.', 422);
  throw new UserError('No downloadable video was found for this link.', 422);
}

// ---------------------------------------------------------------- download jobs
const JOBS = new Map();
let running = 0; const waiters = [];
async function acquire() { if (running < MAX_CONCURRENT) { running++; return; } await new Promise(r => waiters.push(r)); running++; }
function release() { running--; const n = waiters.shift(); if (n) n(); }

const safeName = s => (String(s || 'video').replace(/[\\/:*?"<>|\x00-\x1f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 90) || 'video');
function isPublicHost(u) { // refuse links that point at the server itself or private networks
  if (ENV.ALLOW_LOCAL_MEDIA === '1') return true; // for testing only
  try {
    const h = new URL(u).hostname.toLowerCase();
    if (h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal')) return false;
    if (/^\d+\.\d+\.\d+\.\d+$/.test(h)) { const [a, b] = h.split('.').map(Number); if (a === 10 || a === 127 || a === 0 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168)) return false; }
    if (h.includes(':')) return false;
    return true;
  } catch (e) { return false; }
}

async function downloadTo(url, dest, job) {
  if (!isPublicHost(url)) throw new UserError('The file link is not allowed.', 422);
  const ac = new AbortController();
  let timer = setTimeout(() => ac.abort(), 120000); // time allowed to START (rendered files can take a while)
  let r;
  try { r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36', Accept: '*/*' }, redirect: 'follow', signal: ac.signal }); }
  catch (e) { clearTimeout(timer); throw new UserError('Could not start the download. Please try again.', 502); }
  if (!r.ok || !r.body) { clearTimeout(timer); throw new UserError('The video file could not be opened (' + r.status + '). Try another quality.', 502); }
  const ct = (r.headers.get('content-type') || '').toLowerCase();
  if (/text\/|json|html/.test(ct)) { clearTimeout(timer); ac.abort(); throw new UserError('The source did not return a video file. Try another quality.', 502); }
  const total = parseInt(r.headers.get('content-length') || '0', 10) || 0;
  if (total > MAX_FILESIZE) { clearTimeout(timer); ac.abort(); throw new UserError('This file is too large (limit ' + Math.round(MAX_FILESIZE / 1048576) + ' MB). Choose a lower quality.', 413); }
  job.state = 'downloading';
  const ws = fs.createWriteStream(dest);
  let got = 0;
  try {
    for await (const chunk of Readable.fromWeb(r.body)) {
      clearTimeout(timer); timer = setTimeout(() => ac.abort(), 45000); // stalled for 45 s -> give up
      got += chunk.length;
      if (got > MAX_FILESIZE) throw new UserError('This file is too large (limit ' + Math.round(MAX_FILESIZE / 1048576) + ' MB). Choose a lower quality.', 413);
      job.percent = total ? Math.min(97, 12 + (got / total) * 85) : Math.min(95, job.percent + 0.6);
      if (!ws.write(chunk)) await once(ws, 'drain');
    }
  } catch (e) {
    ws.destroy();
    if (e instanceof UserError) throw e;
    throw new UserError('The download was interrupted. Please try again.', 502);
  } finally { clearTimeout(timer); }
  job.state = 'processing';
  await new Promise((ok, bad) => { ws.on('error', bad); ws.end(ok); });
  if (!got) throw new UserError('The downloaded file was empty. Please try again.', 502);
  return { ct, got };
}

async function runJob(job) {
  await acquire();
  try {
    job.state = 'downloading'; job.percent = 4;
    const info = await getInfo(job.link);
    const meta = readMeta(info, job.link.platform);
    job.percent = 8;
    const item = await resolveMedia(job.link, job.res);
    job.percent = 12;
    const tmpFile = path.join(job.dir, 'file.part');
    const { ct } = await downloadTo(item.url, tmpFile, job);
    let ext = item.ext;
    if (!/^[a-z0-9]{2,4}$/.test(ext || '')) ext = extFromMime(ct) || (/^audio/i.test(job.res) ? 'm4a' : 'mp4');
    const isAudio = /^audio/i.test(job.res);
    const name = safeName(meta.title || job.link.id || 'video') + (!isAudio && item.q ? ' ' + labelOf(item.q) : '') + '.' + ext;
    const finalPath = path.join(job.dir, 'file.' + ext);
    fs.renameSync(tmpFile, finalPath);
    job.file = finalPath; job.name = name;
    job.percent = 100; job.state = 'done';
  } catch (e) {
    job.error = e instanceof UserError ? e.message : 'Something went wrong. Please try again.';
    if (!(e instanceof UserError)) log('job failed', e);
    job.state = 'error';
    fs.rmSync(job.dir, { recursive: true, force: true });
  } finally { job.finishedAt = Date.now(); release(); }
}
setInterval(() => {
  const now = Date.now();
  for (const [id, j] of JOBS) {
    if ((j.finishedAt && now - j.finishedAt > JOB_TTL) || (!j.finishedAt && now - j.created > 3 * 3600 * 1000)) { fs.rmSync(j.dir, { recursive: true, force: true }); JOBS.delete(id); }
  }
}, 60000).unref();

// ---------------------------------------------------------------- routes
const STATIC = { 'index.html': 'text/html; charset=utf-8', 'manifest.webmanifest': 'application/manifest+json', 'sw.js': 'application/javascript', 'icon-192.png': 'image/png', 'icon-512.png': 'image/png' };

async function handle(req, res) {
  applyCors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const u = new URL(req.url, 'http://localhost');
  const p = u.pathname.replace(/\/+$/, '') || '/';
  const ip = clientIp(req);

  if (p === '/health') return sendJson(res, 200, { ok: true });

  if (p === '/info') {
    const link = parseLink(u.searchParams.get('url'));
    if (link.error) return fail(res, 400, link.error);
    if (limited('info', ip, INFO_PER_MIN)) return fail(res, 429, 'Too many requests. Please wait a minute.');
    const data = await getInfo(link);
    const media = extractMedia(data, link.platform);
    const labels = buildLabels(media);
    if (!labels.video.length && !labels.audio) {
      log('no media found in API answer for', link.platform, JSON.stringify(data).slice(0, 600));
      return fail(res, 422, 'No downloadable video was found for this link.');
    }
    const meta = readMeta(data, link.platform);
    const formats = labels.video.slice(); if (labels.audio) formats.push('Audio only');
    const def = formats.includes('720p') ? '720p' : (labels.video.filter(l => parseInt(l, 10) < 720).pop() || labels.video[0] || formats[0]);
    return sendJson(res, 200, { title: meta.title || 'Video found', thumbnail: meta.thumbnail, duration: meta.duration, formats, default: def });
  }

  if (p === '/start') {
    const link = parseLink(u.searchParams.get('url'));
    if (link.error) return fail(res, 400, link.error);
    const resq = String(u.searchParams.get('res') || '').trim();
    if (!/^[A-Za-z0-9 ()._-]{1,30}$/.test(resq)) return fail(res, 400, 'Unknown quality.');
    if (limited('start', ip, START_PER_MIN)) return fail(res, 429, 'Too many downloads. Please wait a minute.');
    let active = 0; for (const j of JOBS.values()) if (j.ip === ip && !j.finishedAt) active++;
    if (active >= MAX_ACTIVE_IP) return fail(res, 429, 'You already have downloads in progress. Please wait for them to finish.');
    if (!API_KEY) return fail(res, 503, 'The server is not set up yet (API key missing).');
    const id = crypto.randomBytes(16).toString('hex');
    const dir = path.join(WORK_DIR, id); fs.mkdirSync(dir, { recursive: true });
    const job = { id, ip, link, res: resq, dir, state: 'queued', percent: 0, created: Date.now() };
    JOBS.set(id, job); runJob(job);
    return sendJson(res, 200, { id });
  }

  let m;
  if ((m = p.match(/^\/status\/([a-f0-9]{32})$/))) {
    const j = JOBS.get(m[1]);
    if (!j) return fail(res, 404, 'This download has expired. Please start again.');
    if (j.state === 'error') return sendJson(res, 200, { state: 'error', error: j.error });
    return sendJson(res, 200, { state: j.state, percent: Math.round(j.percent * 10) / 10 });
  }

  if ((m = p.match(/^\/file\/([a-f0-9]{32})$/))) {
    const j = JOBS.get(m[1]);
    if (!j || j.state !== 'done' || !fs.existsSync(j.file)) return fail(res, 404, 'File not ready or expired. Please start again.');
    const size = fs.statSync(j.file).size;
    const ascii = j.name.replace(/[^\x20-\x7e]/g, '_').replace(/"/g, "'");
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream', 'Content-Length': size,
      'Content-Disposition': `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(j.name)}`,
    });
    const rs = fs.createReadStream(j.file); rs.pipe(res); req.on('close', () => rs.destroy());
    return;
  }

  if (p === '/selftest') { // owner-only check: shows how the API answer is understood
    if (!ADMIN_KEY || u.searchParams.get('key') !== ADMIN_KEY) return fail(res, 404, 'Not found.');
    const link = parseLink(u.searchParams.get('url') || 'https://www.youtube.com/watch?v=dQw4w9WgXcQ');
    if (link.error) return fail(res, 400, link.error);
    const data = await callApi(link, { proxied: false });
    const media = extractMedia(data, link.platform);
    const cut = o => ({ q: o.q, ext: o.ext, hasAudio: o.hasAudio, size: o.size, url: o.url.slice(0, 70) + '...' });
    return sendJson(res, 200, {
      platform: link.platform, topLevelKeys: Object.keys(data), meta: readMeta(data), labels: buildLabels(media),
      counts: { videos: media.videos.length, audios: media.audios.length, renderableVideos: media.rvideos.length, renderableAudios: media.raudios.length },
      sample: { videos: media.videos.slice(0, 4).map(cut), audios: media.audios.slice(0, 2).map(cut) },
      rawStart: JSON.stringify(data).slice(0, 3000), apiCallsToday: budget.used,
    });
  }

  // website files
  const name = p === '/' ? 'index.html' : p.slice(1);
  if (STATIC[name] && fs.existsSync(path.join(PUBLIC_DIR, name))) {
    const buf = fs.readFileSync(path.join(PUBLIC_DIR, name));
    res.writeHead(200, { 'Content-Type': STATIC[name], 'Content-Length': buf.length, 'Cache-Control': 'no-cache' });
    return res.end(buf);
  }
  return fail(res, 404, 'Not found.');
}

const server = http.createServer((req, res) => {
  handle(req, res).catch(e => {
    if (e instanceof UserError) return res.headersSent ? res.end() : fail(res, e.code, e.message);
    log('error', e);
    if (!res.headersSent) fail(res, 500, 'Something went wrong. Please try again.'); else res.end();
  });
});
server.requestTimeout = 0;
server.listen(PORT, () => {
  log(`Video Downloader running on port ${PORT}`);
  if (!API_KEY) log('WARNING: RAPIDAPI_KEY is not set - downloads will not work until you add it.');
});

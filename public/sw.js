// Tiny service worker: lets the site be installed as an app and open offline (shell only).
// Network first, so a new version of the site always shows up. Downloads/API are never cached.
const CACHE = "vd-shell-v1";
const API = /^\/(info|start|status|file|health)(\/|$)/;

self.addEventListener("install", e => {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(["/", "/manifest.webmanifest", "/icon-192.png"])).catch(() => {}));
});
self.addEventListener("activate", e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener("fetch", e => {
  const r = e.request, u = new URL(r.url);
  if (r.method !== "GET" || u.origin !== location.origin || API.test(u.pathname) || r.headers.has("range")) return;
  e.respondWith(
    fetch(r).then(res => { if (res.ok) { const cp = res.clone(); caches.open(CACHE).then(c => c.put(r, cp)); } return res; })
      .catch(() => caches.match(r).then(m => m || caches.match("/")))
  );
});

import asyncio, hashlib, json, os, re, sqlite3, time, ipaddress, socket
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv("DATA_DIR", APP_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
DB = os.path.join(DATA_DIR, "cybersearch.db")
USER_AGENT = "CyberSearchBot/1.0 (+authorized site search; respects robots.txt)"
MAX_PAGES_PER_SITE = int(os.getenv("MAX_PAGES_PER_SITE", "80"))
REQUEST_TIMEOUT = 15
scheduler = AsyncIOScheduler()
app = FastAPI(title="CyberSearch", version="0.1.0")


def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS searches(
            id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT NOT NULL,
            sites TEXT NOT NULL, interval_hours INTEGER DEFAULT 0,
            created_at TEXT NOT NULL, last_run TEXT, active INTEGER DEFAULT 1)""")
        c.execute("""CREATE TABLE IF NOT EXISTS results(
            id INTEGER PRIMARY KEY AUTOINCREMENT, search_id INTEGER NOT NULL,
            url TEXT NOT NULL, title TEXT, snippet TEXT, matched_text TEXT,
            content_hash TEXT, found_at TEXT NOT NULL,
            UNIQUE(search_id,url,content_hash))""")
        c.execute("""CREATE TABLE IF NOT EXISTS runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, search_id INTEGER NOT NULL,
            started_at TEXT NOT NULL, finished_at TEXT, pages_seen INTEGER DEFAULT 0,
            matches INTEGER DEFAULT 0, status TEXT DEFAULT 'running', message TEXT)""")

def normalize_url(url):
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    p = urlparse(url)
    if not p.hostname or p.scheme not in ("http", "https"):
        raise ValueError("رابط غير صالح")
    return url

def is_public_host(hostname):
    # Prevent this crawler from being used to reach localhost/private infrastructure.
    try:
        addresses = {ipaddress.ip_address(hostname)}
    except ValueError:
        try:
            addresses = {ipaddress.ip_address(item[4][0]) for item in
                         socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)}
        except Exception:
            return False
    return bool(addresses) and all(
        ip.is_global and not ip.is_multicast and not ip.is_unspecified
        for ip in addresses
    )

async def safe_get(client, url, headers=None, timeout=REQUEST_TIMEOUT):
    current = url
    for _ in range(5):
        p = urlparse(current)
        if p.scheme not in ("http", "https") or not p.hostname or not is_public_host(p.hostname):
            raise ValueError("تم منع عنوان غير عام أو إعادة توجيه غير آمنة")
        response = await client.get(current, timeout=timeout, follow_redirects=False,
                                    headers=headers or {})
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("location")
            if not location:
                return response
            current = urljoin(current, location)
            continue
        return response
    raise ValueError("عدد التحويلات أكبر من الحد المسموح")

async def robots_allowed(client, url):
    p = urlparse(url)
    robots_url = f"{p.scheme}://{p.netloc}/robots.txt"
    try:
        r = await safe_get(client, robots_url, timeout=8)
        if r.status_code in (404, 410):
            return True
        if r.status_code >= 400:
            return False
        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(r.text.splitlines())
        return parser.can_fetch(USER_AGENT, url)
    except Exception:
        return False  # fail closed if robots policy cannot be checked

def extract_text(content, content_type):
    if "html" not in content_type.lower():
        return "", ""
    soup = BeautifulSoup(content, "html.parser")
    for x in soup(["script", "style", "noscript", "svg", "nav", "footer"]):
        x.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    return title[:300], text[:1_000_000]

def make_snippet(text, query, radius=180):
    low, qlow = text.casefold(), query.casefold()
    i = low.find(qlow)
    if i < 0:
        return text[:radius * 2]
    return ("…" if i > radius else "") + text[max(0, i-radius):i+len(query)+radius] + ("…" if i+len(query)+radius < len(text) else "")

async def crawl_site(client, site, query, search_id):
    start = normalize_url(site)
    origin = urlparse(start)
    allowed_host = origin.hostname
    queue, visited = [start], set()
    pages, matches = 0, 0
    while queue and pages < MAX_PAGES_PER_SITE:
        url = urldefrag(queue.pop(0))[0]
        p = urlparse(url)
        if p.hostname != allowed_host or url in visited:
            continue
        visited.add(url)
        if not await robots_allowed(client, url):
            continue
        try:
            response = await safe_get(client, url, timeout=REQUEST_TIMEOUT,
                                      headers={"User-Agent": USER_AGENT})
            if response.status_code != 200:
                continue
            ctype = response.headers.get("content-type", "")
            if "text/html" not in ctype.lower():
                continue
            title, text = extract_text(response.text, ctype)
            pages += 1
            if query.casefold() in text.casefold():
                snippet = make_snippet(text, query)
                digest = hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()
                with db() as c:
                    c.execute("""INSERT OR IGNORE INTO results
                    (search_id,url,title,snippet,matched_text,content_hash,found_at)
                    VALUES(?,?,?,?,?,?,?)""",
                    (search_id, str(response.url), title, snippet, query, digest,
                     datetime.now(timezone.utc).isoformat()))
                matches += 1
            soup = BeautifulSoup(response.text, "html.parser")
            for a in soup.find_all("a", href=True):
                nxt = urldefrag(urljoin(str(response.url), a["href"]))[0]
                np = urlparse(nxt)
                if np.scheme in ("http", "https") and np.hostname == allowed_host and nxt not in visited:
                    if not re.search(r"\.(?:jpg|jpeg|png|gif|webp|zip|mp4|mp3|css|js|xml)(?:$|\?)", np.path, re.I):
                        queue.append(nxt)
            await asyncio.sleep(0.5)  # polite crawl rate
        except Exception:
            continue
    return pages, matches

async def run_search(search_id):
    with db() as c:
        row = c.execute("SELECT * FROM searches WHERE id=? AND active=1", (search_id,)).fetchone()
        if not row:
            return
        c.execute("INSERT INTO runs(search_id,started_at) VALUES(?,?)",
                  (search_id, datetime.now(timezone.utc).isoformat()))
        run_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    total_pages = total_matches = 0
    try:
        sites = json.loads(row["sites"])
        async with httpx.AsyncClient() as client:
            for site in sites:
                p, m = await crawl_site(client, site, row["query"], search_id)
                total_pages += p
                total_matches += m
        status, message = "completed", "اكتمل البحث"
    except Exception as e:
        status, message = "error", str(e)[:300]
    now = datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.execute("UPDATE runs SET finished_at=?,pages_seen=?,matches=?,status=?,message=? WHERE id=?",
                  (now,total_pages,total_matches,status,message,run_id))
        c.execute("UPDATE searches SET last_run=? WHERE id=?", (now,search_id))

class SearchIn(BaseModel):
    query: str = Field(min_length=1, max_length=200)
    sites: list[str] = Field(min_length=1, max_length=20)
    interval_hours: int = Field(default=0, ge=0, le=168)

@app.on_event("startup")
async def startup():
    init_db()
    scheduler.start()
    with db() as c:
        rows = c.execute("SELECT id,interval_hours FROM searches WHERE active=1 AND interval_hours>0").fetchall()
    for r in rows:
        scheduler.add_job(run_search, "interval", hours=r["interval_hours"],
                          args=[r["id"]], id=f"search-{r['id']}", replace_existing=True)

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown(wait=False)

INDEX_HTML = '<!doctype html>\n<html lang="ar" dir="rtl">\n<head>\n<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">\n<title>AMR Search | البحث الذكي داخل المواقع</title>\n<meta name="theme-color" content="#0b1220">\n<meta name="description" content="AMR Search - ابحث وتابع الكلمات داخل المواقع التي تحددها">\n<link rel="manifest" href="/manifest.webmanifest">\n<link rel="icon" href="/icon.svg">\n<link rel="apple-touch-icon" href="/icon.svg">\n<style>\n:root{--bg:#0b1220;--panel:#121d30;--line:#26364f;--txt:#e8eef8;--muted:#9aabc2;--blue:#4388ff;--green:#37c994}\n*{box-sizing:border-box}body{margin:0;background:linear-gradient(140deg,#0b1220,#101c31);color:var(--txt);font-family:Tahoma,Arial,sans-serif}\nmain{max-width:1050px;margin:auto;padding:28px 18px}.top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:24px}\n.brand{font-size:26px;font-weight:800;letter-spacing:.3px}.brand span{color:#63a0ff}.tag{color:#9aabc2;font-size:13px}\n.panel{background:#121d30;border:1px solid var(--line);border-radius:18px;padding:22px;margin-bottom:18px;box-shadow:0 12px 35px #0003}\nh1{font-size:24px;margin:0 0 8px}p{color:var(--muted);line-height:1.7;margin-top:5px}\nlabel{display:block;font-size:14px;margin:16px 0 8px;color:#cbd7e8}\ninput,textarea,select{width:100%;background:#0b1526;border:1px solid #30415c;border-radius:10px;padding:13px;color:white;font:inherit;outline:none}\ntextarea{min-height:90px;resize:vertical}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}\nbutton{border:0;border-radius:10px;padding:13px 18px;background:var(--blue);color:white;font-weight:bold;font-size:15px;cursor:pointer}\nbutton.secondary{background:#263750}button:disabled{opacity:.55;cursor:wait}.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}\n.note{font-size:12px;color:#9aabc2;margin-top:12px}.results{display:grid;gap:10px}.result{padding:15px;border:1px solid var(--line);border-radius:12px;background:#0d182a}\n.result a{color:#7eb0ff;word-break:break-all}.result h3{font-size:15px;margin:0 0 8px}.snippet{color:#c4d0e1;line-height:1.7;font-size:13px}\n.pill{display:inline-block;background:#173c36;color:#6ce0b4;padding:5px 9px;border-radius:20px;font-size:11px}\n.searchrow{display:flex;justify-content:space-between;align-items:center;gap:10px}.empty{color:var(--muted);text-align:center;padding:25px}\n@media(max-width:650px){.grid{grid-template-columns:1fr}.top{align-items:flex-start;flex-direction:column}.panel{padding:17px}}\n</style>\n</head>\n<body><main>\n<header class="top"><div><div class="brand">AMR<span> Search</span></div><div class="tag">محرك البحث والمتابعة داخل المواقع التي تحددها</div></div><span class="pill">AMR Search</span></header>\n<section class="panel">\n<h1>ابحث داخل موقع أو عدة مواقع</h1>\n<p>اكتب كلمة أو جملة، ثم أضف عناوين المواقع. سيزحف المحرك إلى الصفحات العامة المسموح بها ويعرض الصفحات التي تحتوي على العبارة.</p>\n<label>الكلمة أو الجملة</label><input id="query" placeholder="مثال: سياسة الخصوصية أو اسم منتج">\n<label>المواقع (كل موقع في سطر منفصل)</label><textarea id="sites" placeholder="https://example.com&#10;https://www.example.org"></textarea>\n<div class="grid">\n<div><label>المتابعة الدورية</label><select id="interval"><option value="0">بحث مرة واحدة فقط</option><option value="6">كل 6 ساعات</option><option value="12">كل 12 ساعة</option><option value="24">كل 24 ساعة</option><option value="168">مرة أسبوعيًا</option></select></div>\n<div><label>حدود النسخة الأولية</label><input value="حتى 80 صفحة لكل موقع في الجولة" disabled></div>\n</div>\n<div class="actions"><button id="start" onclick="startSearch()">ابدأ البحث</button><button class="secondary" onclick="loadSearches()">تحديث سجل البحث</button><button class="secondary" id="installBtn" hidden>تثبيت AMR على الموبايل</button></div>\n<div class="note">استخدم الأداة فقط مع المواقع التي تملكها أو لديك إذن بالزحف إليها. يحترم البرنامج robots.txt، ولا يتجاوز تسجيل الدخول أو الحماية أو قيود الوصول.</div>\n</section>\n<section class="panel"><div class="searchrow"><h2 style="font-size:19px;margin:0">سجل عمليات البحث</h2><span class="tag" id="status"></span></div><div id="history" class="results" style="margin-top:14px"><div class="empty">لا توجد عمليات بحث بعد.</div></div></section>\n<section class="panel"><div class="searchrow"><h2 style="font-size:19px;margin:0">النتائج</h2><span class="tag" id="resultTitle"></span></div><div id="results" class="results" style="margin-top:14px"><div class="empty">ابدأ عملية بحث لعرض النتائج.</div></div></section>\n</main>\n<script>\nconst $=id=>document.getElementById(id);\nasync function startSearch(){\n const query=$(\'query\').value.trim(), sites=$(\'sites\').value.split(/\\n|,/).map(x=>x.trim()).filter(Boolean);\n if(!query||!sites.length){alert(\'اكتب عبارة البحث وموقعًا واحدًا على الأقل\');return}\n $(\'start\').disabled=true;$(\'status\').textContent=\'جارٍ البحث...\';\n try{\n  const r=await fetch(\'/api/search\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({query,sites,interval_hours:+$(\'interval\').value})});\n  const d=await r.json();if(!r.ok)throw Error(d.detail||\'حدث خطأ\');\n  await loadSearches();await showResults(d.id);\n }catch(e){alert(e.message)}finally{$(\'start\').disabled=false;$(\'status\').textContent=\'\';}\n}\nasync function loadSearches(){\n const r=await fetch(\'/api/searches\'),data=await r.json();\n $(\'history\').innerHTML=data.length?data.map(s=>`<div class="result"><div class="searchrow"><div><b>${esc(s.query)}</b><div class="tag">${JSON.parse(s.sites).map(esc).join(\' • \')}</div><div class="tag">نتائج: ${s.result_count} | متابعة: ${s.interval_hours?s.interval_hours+\' ساعة\':\'متوقفة\'}</div></div><button class="secondary" onclick="showResults(${s.id})">عرض النتائج</button></div></div>`).join(\'\'):\'<div class="empty">لا توجد عمليات بحث بعد.</div>\';\n}\nasync function showResults(id){\n const r=await fetch(`/api/searches/${id}/results`),data=await r.json();\n $(\'resultTitle\').innerHTML=`${data.length} نتيجة <a style="color:#7eb0ff;margin-right:10px" href="/api/searches/${id}/export.csv">تصدير CSV</a>`;\n $(\'results\').innerHTML=data.length?data.map(x=>`<article class="result"><h3>${esc(x.title||\'صفحة بدون عنوان\')}</h3><a href="${esc(x.url)}" target="_blank" rel="noopener">${esc(x.url)}</a><div class="snippet">${esc(x.snippet||\'\')}</div><div class="tag">${esc(x.found_at)}</div></article>`).join(\'\'):\'<div class="empty">لم يتم العثور على تطابقات في الصفحات التي أمكن فحصها.</div>\';\n}\nfunction esc(s){return String(s??\'\').replace(/[&<>"\']/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]))}\nloadSearches();\nif (\'serviceWorker\' in navigator) { window.addEventListener(\'load\', () => navigator.serviceWorker.register(\'/sw.js\').catch(()=>{})); }\nlet installPrompt;\nwindow.addEventListener(\'beforeinstallprompt\', (event) => { event.preventDefault(); installPrompt = event; $(\'installBtn\').hidden = false; });\n$(\'installBtn\').addEventListener(\'click\', async () => { if (!installPrompt) return; installPrompt.prompt(); await installPrompt.userChoice; installPrompt = null; $(\'installBtn\').hidden = true; });\n</script></body></html>\n'
MANIFEST_TEXT = '{\n  "id": "/",\n  "name": "AMR Search",\n  "short_name": "AMR",\n  "description": "ابحث وتابع الكلمات داخل المواقع التي تحددها",\n  "lang": "ar",\n  "dir": "rtl",\n  "start_url": "/",\n  "scope": "/",\n  "display": "standalone",\n  "background_color": "#0b1220",\n  "theme_color": "#0b1220",\n  "icons": [\n    {"src": "/static/icons/amr-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},\n    {"src": "/static/icons/amr-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"}\n  ]\n}\n'
SERVICE_WORKER_TEXT = "const CACHE = 'amr-search-shell-v1';\nconst APP_SHELL = ['/', '/manifest.webmanifest', '/static/icons/amr-192.png', '/static/icons/amr-512.png'];\nself.addEventListener('install', event => { event.waitUntil(caches.open(CACHE).then(cache => cache.addAll(APP_SHELL))); self.skipWaiting(); });\nself.addEventListener('activate', event => { event.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))); self.clients.claim(); });\nself.addEventListener('fetch', event => {\n  const url = new URL(event.request.url);\n  if (event.request.method !== 'GET' || url.pathname.startsWith('/api/')) return;\n  event.respondWith(fetch(event.request).then(response => {\n    if (response.ok && url.origin === self.location.origin) { const copy = response.clone(); caches.open(CACHE).then(cache => cache.put(event.request, copy)); }\n    return response;\n  }).catch(() => caches.match(event.request).then(cached => cached || caches.match('/'))));\n});\n"

@app.get("/")
def home():
    return HTMLResponse(INDEX_HTML)

@app.get("/manifest.webmanifest")
def pwa_manifest():
    return Response(MANIFEST_TEXT, media_type="application/manifest+json")

@app.get("/sw.js")
def service_worker():
    return Response(SERVICE_WORKER_TEXT, media_type="application/javascript",
                    headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"})

@app.get("/icon.svg")
def app_icon():
    return Response("""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" rx="100" fill="#101c31"/><text x="256" y="300" font-size="130" font-family="Arial" font-weight="bold" text-anchor="middle" fill="#63a0ff">AMR</text></svg>""", media_type="image/svg+xml")

@app.post("/api/search")
async def create_search(data: SearchIn):
    if not data.query.strip():
        raise HTTPException(400, "اكتب كلمة أو جملة للبحث")
    try:
        sites = list(dict.fromkeys(normalize_url(s) for s in data.sites))
    except ValueError as e:
        raise HTTPException(400, str(e))
    with db() as c:
        c.execute("INSERT INTO searches(query,sites,interval_hours,created_at) VALUES(?,?,?,?)",
                  (data.query.strip(), json.dumps(sites), data.interval_hours,
                   datetime.now(timezone.utc).isoformat()))
        sid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    if data.interval_hours:
        scheduler.add_job(run_search, "interval", hours=data.interval_hours,
                          args=[sid], id=f"search-{sid}", replace_existing=True)
    await run_search(sid)
    return {"id": sid}

@app.get("/api/searches")
def searches():
    with db() as c:
        rows = c.execute("""SELECT s.*,
          (SELECT COUNT(*) FROM results r WHERE r.search_id=s.id) result_count
          FROM searches s ORDER BY s.id DESC""").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/searches/{sid}/results")
def get_results(sid: int):
    with db() as c:
        rows = c.execute("SELECT * FROM results WHERE search_id=? ORDER BY id DESC", (sid,)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/searches/{sid}/run")
async def rerun(sid: int):
    await run_search(sid)
    return {"ok": True}

@app.post("/api/searches/{sid}/pause")
def pause(sid: int):
    with db() as c:
        c.execute("UPDATE searches SET active=0 WHERE id=?", (sid,))
    job = scheduler.get_job(f"search-{sid}")
    if job: scheduler.remove_job(f"search-{sid}")
    return {"ok": True}

@app.get("/api/searches/{sid}/export.csv")
def export_csv(sid: int):
    import csv, io
    with db() as c:
        rows = c.execute("SELECT url,title,snippet,found_at FROM results WHERE search_id=?", (sid,)).fetchall()
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["URL","Title","Snippet","Found at"])
    for r in rows: w.writerow(list(r))
    from fastapi.responses import Response
    return Response("\ufeff"+output.getvalue(), media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="cybersearch-{sid}.csv"'})

from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import sqlite3, os, re, secrets, hashlib, json, smtplib
from email.mime.text import MIMEText
try:
    import anthropic
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False
from datetime import datetime, date, timedelta

app = FastAPI(title="Platestory AIR 6")
security = HTTPBearer(auto_error=False)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENT_SECRET   = os.getenv("AGENT_SECRET", "platestory-2025-xK9mP2qR7nL4")
DB_PATH        = os.getenv("DB_PATH", "platestory.db")
ADMIN_EMAIL    = "vamsi.bhogi@platestory.in"
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
SMTP_HOST      = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER      = os.getenv("SMTP_USER", "")
SMTP_PASS      = os.getenv("SMTP_PASS", "")
ALERT_EMAIL    = os.getenv("ALERT_EMAIL", "vamsi.bhogi@platestory.in")

CITIES = {
    "chennai": {"name": "Chennai", "slots": 300},
    "hyderabad": {"name": "Hyderabad", "slots": 300}
}

MONTH_MAP = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,
    "apr":4,"april":4,"may":5,"jun":6,"june":6,"jul":7,"july":7,
    "aug":8,"august":8,"sep":9,"sept":9,"september":9,
    "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}

CAKE_KEYWORDS = {
    "wedding":    ["wedding","shaadi","vivah","marriage","bride","groom"],
    "birthday":   ["birthday","bday","b-day","janmdin","kids birthday","wife birthday",
                   "husband birthday","baby birthday","daughter birthday","son birthday",
                   "mom birthday","dad birthday","sister birthday","brother birthday"],
    "anniversary":["anniversary","anniv"],
    "baby_shower":["baby shower","babyshower","godh bharai"],
    "engagement": ["engagement","roka","ring ceremony","sagai"],
    "custom":     ["custom cake","theme cake","designer cake","corporate","office","launch","farewell","retirement"]
}

def hash_password(p): return hashlib.sha256(p.encode()).hexdigest()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'salesperson', name TEXT,
        city TEXT DEFAULT 'chennai', created_at TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY, email TEXT NOT NULL,
        role TEXT NOT NULL, created_at TEXT NOT NULL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS extractions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        salesperson_email TEXT, contact_name TEXT, message TEXT,
        event_date TEXT, cake_type TEXT, lead_score TEXT,
        funnel_stage TEXT DEFAULT 'enquiry', assigned_to TEXT,
        follow_up_done INTEGER DEFAULT 0, follow_up_at TEXT,
        notes TEXT, next_action TEXT, conversion_probability TEXT,
        business_vertical TEXT, estimated_order_value TEXT,
        suggested_reply TEXT, has_image INTEGER DEFAULT 0,
        city TEXT DEFAULT 'chennai', captured_at TEXT,
        conversion_status TEXT DEFAULT 'open',
        last_updated TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS follow_ups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        extraction_id INTEGER, done_by TEXT, done_at TEXT, notes TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS conversion_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern_type TEXT, pattern_data TEXT, outcome TEXT,
        created_at TEXT)""")
    conn.commit()
    # Add missing columns if upgrading from older schema
    for col, defn in [
        ("has_image", "INTEGER DEFAULT 0"),
        ("city", "TEXT DEFAULT 'chennai'"),
        ("conversion_status", "TEXT DEFAULT 'open'"),
        ("last_updated", "TEXT"),
        ("suggested_reply", "TEXT")
    ]:
        try:
            conn.execute(f"ALTER TABLE extractions ADD COLUMN {col} {defn}")
            conn.commit()
        except: pass
    try:
        conn.execute("ALTER TABLE users ADD COLUMN city TEXT DEFAULT 'chennai'")
        conn.commit()
    except: pass
    existing = conn.execute("SELECT * FROM users WHERE email=?", (ADMIN_EMAIL,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO users (email,password_hash,role,name,city,created_at) VALUES (?,?,?,?,?,?)",
            (ADMIN_EMAIL, hash_password("platestory@2025"), "admin", "Vamsi", "chennai", datetime.utcnow().isoformat()))
        conn.commit()
    conn.close()

init_db()

# ── HELPERS ────────────────────────────────────────────────────────────────────

def detect_city(message: str, contact: str) -> str:
    msg = (message + " " + contact).lower()
    hyd_kw = ["hyderabad","hyd","jubilee hills","banjara hills","gachibowli","kondapur","hitech city","madhapur","secunderabad","ameerpet"]
    if any(k in msg for k in hyd_kw): return "hyderabad"
    return "chennai"

def parse_date_from_message(message: str) -> Optional[str]:
    msg = message.lower()
    msg = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', msg)
    p1 = re.compile(r'(\d{1,2})\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)')
    p2 = re.compile(r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})')
    p3 = re.compile(r'(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?')
    day, month, year = None, None, None
    today = datetime.today()
    m = p1.search(msg)
    if m: day, month = int(m.group(1)), MONTH_MAP.get(m.group(2)[:3])
    else:
        m = p2.search(msg)
        if m: month, day = MONTH_MAP.get(m.group(1)[:3]), int(m.group(2))
        else:
            m = p3.search(msg)
            if m:
                day, month = int(m.group(1)), int(m.group(2))
                year = int(m.group(3)) if m.group(3) else None
    if day and month:
        if not year:
            year = today.year
            try:
                if date(year, month, day) < today.date(): year += 1
            except: return None
        try: return date(year, month, day).isoformat()
        except: return None
    return None

def parse_cake_type(message: str) -> Optional[str]:
    msg = message.lower()
    for ct, kws in CAKE_KEYWORDS.items():
        if any(k in msg for k in kws): return ct
    return None

def parse_lead_score(message: str) -> str:
    msg = message.lower()
    if any(w in msg for w in ["urgent","asap","today","tomorrow","this week","confirmed","book","advance","payment","upi","gpay"]): return "HOT"
    if any(w in msg for w in ["maybe","will think","let me check","not sure","budget issue","too expensive","costly"]): return "COLD"
    return "WARM"

def parse_funnel_stage(message: str) -> str:
    msg = message.lower()
    if any(w in msg for w in ["paid","advance","payment done","transferred","upi","gpay","confirmed"]): return "confirmed"
    if any(w in msg for w in ["how much","price","cost","rate","charges","quote","budget"]): return "quoted"
    if any(w in msg for w in ["reference","ref image","like this","similar","design","theme","colour","color","flavor","flavour"]): return "ref_shared"
    return "enquiry"

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials: raise HTTPException(status_code=401)
    conn = get_db()
    session = conn.execute("SELECT * FROM sessions WHERE token=?", (credentials.credentials,)).fetchone()
    conn.close()
    if not session: raise HTTPException(status_code=401, detail="Invalid session")
    return dict(session)

def send_email_alert(subject: str, body: str):
    if not SMTP_USER or not SMTP_PASS: return
    try:
        msg = MIMEText(body)
        msg["Subject"] = f"[Platestory AIR 6] {subject}"
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
    except Exception as e:
        print(f"Email alert failed: {e}")

# ── CLAUDE ─────────────────────────────────────────────────────────────────────

async def claude_extract(message: str, contact_name: str,
                          conversation_context: str = "",
                          has_image: bool = False,
                          existing_lead: dict = None) -> dict:
    if not CLAUDE_AVAILABLE or not ANTHROPIC_KEY:
        return {}
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

        context_block = ""
        if conversation_context:
            try:
                ctx = json.loads(conversation_context)
                if ctx:
                    context_block = "\n\nConversation history (oldest first):\n"
                    for m in ctx[-10:]:
                        context_block += f"- {m.get('text','')}\n"
            except: pass

        existing_block = ""
        if existing_lead:
            existing_block = f"""
Previous analysis of this lead:
- Lead score: {existing_lead.get('lead_score', 'unknown')}
- Funnel stage: {existing_lead.get('funnel_stage', 'unknown')}
- Notes: {existing_lead.get('notes', '')}
- Last message: {existing_lead.get('message', '')}
Update your analysis based on the new message. If the stage has changed, reflect that."""

        image_block = "\nNOTE: Customer has sent an image/photo — likely a reference image for the cake design. Treat as ref_shared stage signal." if has_image else ""

        prompt = f"""You are the AI sales brain for Platestory — premium custom cakes in Chennai & Hyderabad.

Platestory pricing (per kg):
- Classic: Vanilla ₹1,107 | Butterscotch ₹1,307 | Chocolate Truffle ₹1,507 | Rainbow ₹1,957
- Specialities: Red Velvet ₹1,707 | Belgian Choc Ganache ₹2,207 | Biscoff ₹2,357 | Berry ₹2,507
- Exotics: Rasmalai ₹2,207 | Tender Coconut ₹1,957 | Mango ₹1,657
- Customisation: 1-tier +₹500 | 2-tier +₹1,500 | 3-tier +₹2,500
- Little Bites: Cupcakes ₹127/pc | Macarons ₹107/pc | Cakesicles ₹157/pc | Cakepops ₹87/pc
- Bento: ₹600-₹900

Conversion signals (strongest → weakest):
1. Advance paid / "confirmed" / sharing payment → confirmed, HOT
2. Asking for payment link / UPI / account number → HOT
3. Event within 7 days + ref image sent → HOT
4. Price accepted ("okay","fine","sounds good","proceed") → HOT
5. Asking price + specific date mentioned → WARM, quoted
6. Sent ref image / design preference → WARM, ref_shared
7. Asking about flavours / delivery area / general → COLD, enquiry
8. Gone silent after price → COLD, needs nudge

Drop signals (likely won't convert):
- "too expensive", "out of budget", "will manage", "nevermind", "cancel", "not required"

Conversion probability rules:
- high: HOT score OR confirmed stage
- medium: WARM with specific date OR ref image sent
- low: COLD or vague enquiry or drop signal detected{existing_block}{context_block}{image_block}

Contact: {contact_name}
Latest message: {message}

Return ONLY valid JSON:
{{
  "cake_type": "birthday|wedding|anniversary|engagement|baby_shower|corporate|bento|little_bites|custom|unknown",
  "event_date": "YYYY-MM-DD or null",
  "lead_score": "HOT|WARM|COLD",
  "funnel_stage": "enquiry|ref_shared|quoted|confirmed",
  "conversion_status": "open|converted|dropped",
  "business_vertical": "little_cakes|large_cakes|corporate|unknown",
  "estimated_order_value": "low(<2500)|mid(2500-8000)|high(8000-20000)|premium(20000+)|unknown",
  "conversion_probability": "high|medium|low",
  "next_action": "one line — what salesperson must do right now",
  "notes": "one line summary of intent and conversation context",
  "suggested_reply": "natural WhatsApp reply to send (2-3 sentences max, conversational, not salesy)",
  "drop_detected": false
}}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"Claude error: {e}")
        return {}

# ── AUTH ───────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    city: str = "chennai"

@app.post("/api/v1/auth/login")
def login(req: LoginRequest):
    if not req.email.endswith("@platestory.in"):
        raise HTTPException(status_code=403, detail="Only @platestory.in emails")
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email,)).fetchone()
    if not user or user["password_hash"] != hash_password(req.password):
        conn.close()
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = secrets.token_hex(32)
    conn.execute("INSERT INTO sessions (token,email,role,created_at) VALUES (?,?,?,?)",
        (token, req.email, user["role"], datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return {"token": token, "role": user["role"], "name": user["name"],
            "city": dict(user).get("city", "chennai")}

@app.post("/api/v1/auth/register")
def register(req: RegisterRequest, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    if not req.email.endswith("@platestory.in"):
        raise HTTPException(status_code=400, detail="Only @platestory.in emails")
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (email,password_hash,role,name,city,created_at) VALUES (?,?,?,?,?,?)",
            (req.email, hash_password(req.password), "salesperson", req.name, req.city, datetime.utcnow().isoformat()))
        conn.commit()
    except: raise HTTPException(status_code=400, detail="Email already exists")
    finally: conn.close()
    return {"status": "ok"}

@app.get("/api/v1/auth/users")
def list_users(user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    users = conn.execute("SELECT id, email, name, role, city, created_at FROM users").fetchall()
    conn.close()
    return [dict(u) for u in users]

@app.delete("/api/v1/auth/users/{user_id}")
def delete_user(user_id: int, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    conn.execute("DELETE FROM users WHERE id=? AND email != ?", (user_id, ADMIN_EMAIL))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.get("/api/v1/auth/me")
def me(user=Depends(get_current_user)):
    return user

# ── EXTRACTIONS ────────────────────────────────────────────────────────────────

@app.post("/api/v1/extractions")
async def extract(req: Request, background_tasks: BackgroundTasks):
    auth = req.headers.get("Authorization", "")
    body = await req.json()
    is_agent = auth == f"Bearer {AGENT_SECRET}"

    if is_agent:
        salesperson = body.get("salesperson_id", ADMIN_EMAIL)
        items = body.get("extractions") or [body]
    else:
        token = auth.replace("Bearer ", "")
        conn = get_db()
        session = conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone()
        conn.close()
        if not session: raise HTTPException(status_code=401)
        salesperson = body.get("salesperson_email", session["email"])
        items = [body]

    conn = get_db()
    results = []
    alerts = []

    for item in items:
        msg   = item.get("message", "").strip()
        contact = item.get("contact_name", "").strip()
        conv_ctx = item.get("conversation_context", "")
        has_image = bool(item.get("has_image", False))

        if not msg or not contact or len(msg) < 3: continue
        if contact in ["Add status","Calls","Chats","Status","WhatsApp","Camera"]: continue

        # Check if lead exists — pass existing data to Claude for continuity
        existing = conn.execute(
            "SELECT * FROM extractions WHERE contact_name=? ORDER BY captured_at DESC LIMIT 1",
            (contact,)
        ).fetchone()
        existing_dict = dict(existing) if existing else None

        city = detect_city(msg, contact)

        ai = await claude_extract(msg, contact, conv_ctx, has_image, existing_dict)

        event_date        = ai.get("event_date")             or parse_date_from_message(msg)
        cake_type         = ai.get("cake_type")              or parse_cake_type(msg)
        lead_score        = ai.get("lead_score")             or parse_lead_score(msg)
        funnel_stage      = ai.get("funnel_stage")           or parse_funnel_stage(msg)
        notes             = ai.get("notes", "")
        next_action       = ai.get("next_action", "")
        conv_prob         = ai.get("conversion_probability", "")
        biz_vertical      = ai.get("business_vertical", "")
        order_value       = ai.get("estimated_order_value", "")
        suggested_reply   = ai.get("suggested_reply", "")
        conv_status       = ai.get("conversion_status", "open")
        drop_detected     = ai.get("drop_detected", False)

        # If image received, ensure funnel stage reflects that
        if has_image and funnel_stage == "enquiry":
            funnel_stage = "ref_shared"

        now_iso = datetime.utcnow().isoformat()

        conn.execute("""INSERT INTO extractions
            (salesperson_email, contact_name, message, event_date, cake_type, lead_score,
             funnel_stage, notes, next_action, conversion_probability, business_vertical,
             estimated_order_value, suggested_reply, has_image, city,
             conversion_status, captured_at, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (salesperson, contact, msg, event_date, cake_type, lead_score,
             funnel_stage, notes, next_action, conv_prob, biz_vertical,
             order_value, suggested_reply, 1 if has_image else 0, city,
             conv_status, now_iso, now_iso))

        # Store pattern for learning
        if existing_dict and existing_dict.get("lead_score") != lead_score:
            conn.execute("""INSERT INTO conversion_patterns (pattern_type, pattern_data, outcome, created_at)
                VALUES (?,?,?,?)""", (
                "score_change",
                json.dumps({"from": existing_dict.get("lead_score"), "to": lead_score, "message": msg[:100]}),
                conv_status, now_iso))

        results.append({"contact": contact, "score": lead_score, "city": city})

        # Email alerts
        if lead_score == "HOT" and (not existing_dict or existing_dict.get("lead_score") != "HOT"):
            alerts.append(f"HOT LEAD: {contact} ({city})\nMessage: {msg[:150]}\nNext: {next_action}")
        if drop_detected:
            alerts.append(f"DROPPED LEAD: {contact} ({city})\nMessage: {msg[:150]}")
        if funnel_stage == "confirmed":
            alerts.append(f"CONFIRMED ORDER: {contact} ({city})\nMessage: {msg[:150]}")

    conn.commit()
    conn.close()

    if alerts:
        body_text = "\n\n---\n\n".join(alerts)
        background_tasks.add_task(send_email_alert, f"{len(alerts)} alert(s)", body_text)

    return {"status": "ok", "processed": len(results), "results": results}

@app.get("/api/v1/extractions/recent")
def recent_extractions(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials if credentials else ""
    conn = get_db()
    if token != AGENT_SECRET:
        session = conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone()
        if not session:
            conn.close()
            raise HTTPException(status_code=401)
    rows = conn.execute("""
        SELECT contact_name, message, event_date, cake_type, lead_score,
               funnel_stage, salesperson_email, captured_at, suggested_reply, city, has_image
        FROM extractions ORDER BY captured_at DESC LIMIT 20
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── DASHBOARD ──────────────────────────────────────────────────────────────────

@app.get("/api/v1/dashboard")
def dashboard(user=Depends(get_current_user), city: str = None):
    conn = get_db()
    today = datetime.today().date()
    next30 = today + timedelta(days=30)
    is_admin = user["role"] == "admin"
    email = user["email"]

    # City filter — admin can see all or filter by city; salesperson sees their city
    city_filter = ""
    if city and city in CITIES:
        city_filter = f"AND city='{city}'"
    elif not is_admin:
        user_city = conn.execute("SELECT city FROM users WHERE email=?", (email,)).fetchone()
        if user_city:
            city_filter = f"AND city='{user_city[0]}'"

    where = ("" if is_admin else f"AND salesperson_email='{email}'") + " " + city_filter

    total     = conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE 1=1 {where}").fetchone()["c"]
    confirmed = conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE funnel_stage='confirmed' {where}").fetchone()["c"]
    dropped   = conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE conversion_status='dropped' {where}").fetchone()["c"]
    hot_count = conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE lead_score='HOT' {where}").fetchone()["c"]
    warm_count= conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE lead_score='WARM' {where}").fetchone()["c"]
    image_leads= conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE has_image=1 {where}").fetchone()["c"]

    forecast_low  = int((hot_count * 5500) + (warm_count * 5500 * 0.3))
    forecast_high = int((hot_count * 8000) + (warm_count * 8000 * 0.5))

    # City slot usage
    city_slots = {}
    for city_key, city_info in CITIES.items():
        used = conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE funnel_stage='confirmed' AND city='{city_key}'").fetchone()["c"]
        city_slots[city_key] = {"name": city_info["name"], "total": city_info["slots"], "used": used, "remaining": city_info["slots"] - used}

    upcoming = conn.execute(f"""
        SELECT contact_name, event_date, cake_type, lead_score, message,
               salesperson_email, funnel_stage, suggested_reply, city, has_image
        FROM extractions WHERE event_date BETWEEN ? AND ? {where}
        ORDER BY event_date ASC
    """, (today.isoformat(), next30.isoformat())).fetchall()

    hot = conn.execute(f"""
        SELECT contact_name, message, cake_type, lead_score, captured_at,
               salesperson_email, funnel_stage, next_action, suggested_reply, city, has_image, id
        FROM extractions WHERE lead_score='HOT' {where}
        ORDER BY captured_at DESC LIMIT 30
    """).fetchall()

    recent = conn.execute(f"""
        SELECT id, contact_name, message, cake_type, lead_score, event_date,
               captured_at, salesperson_email, funnel_stage, follow_up_done, assigned_to,
               notes, next_action, conversion_probability, business_vertical,
               estimated_order_value, suggested_reply, has_image, city, conversion_status
        FROM extractions WHERE 1=1 {where}
        ORDER BY captured_at DESC LIMIT 100
    """).fetchall()

    four_hours_ago = (datetime.utcnow() - timedelta(hours=4)).isoformat()
    unattended = conn.execute(f"""
        SELECT id, contact_name, message, salesperson_email, captured_at,
               cake_type, lead_score, next_action, suggested_reply, city, has_image
        FROM extractions
        WHERE follow_up_done=0 AND captured_at < ? AND conversion_status='open' {where}
        ORDER BY captured_at ASC LIMIT 20
    """, (four_hours_ago,)).fetchall()

    funnel = conn.execute(f"""
        SELECT funnel_stage, COUNT(*) as count FROM extractions WHERE 1=1 {where} GROUP BY funnel_stage
    """).fetchall()

    score_breakdown = conn.execute(f"""
        SELECT lead_score, COUNT(*) as count FROM extractions WHERE 1=1 {where} GROUP BY lead_score
    """).fetchall()

    sp_stats = []
    if is_admin:
        sp_stats = conn.execute("""
            SELECT salesperson_email, COUNT(*) as total,
                   SUM(CASE WHEN lead_score='HOT' THEN 1 ELSE 0 END) as hot,
                   SUM(CASE WHEN lead_score='WARM' THEN 1 ELSE 0 END) as warm,
                   SUM(CASE WHEN follow_up_done=1 THEN 1 ELSE 0 END) as followed_up,
                   SUM(CASE WHEN funnel_stage='confirmed' THEN 1 ELSE 0 END) as confirmed,
                   SUM(CASE WHEN conversion_status='dropped' THEN 1 ELSE 0 END) as dropped,
                   city
            FROM extractions GROUP BY salesperson_email
        """).fetchall()

    users = []
    if is_admin:
        users = conn.execute("SELECT id, email, name, role, city FROM users").fetchall()

    conn.close()
    return {
        "role": user["role"], "email": email,
        "total_customers": total, "confirmed_orders": confirmed,
        "dropped_leads": dropped, "image_leads": image_leads,
        "hot_count": hot_count, "warm_count": warm_count,
        "forecast_low": forecast_low, "forecast_high": forecast_high,
        "city_slots": city_slots,
        "upcoming_events":   [dict(r) for r in upcoming],
        "hot_leads":         [dict(r) for r in hot],
        "recent_leads":      [dict(r) for r in recent],
        "unattended_leads":  [dict(r) for r in unattended],
        "funnel":            [dict(r) for r in funnel],
        "score_breakdown":   [dict(r) for r in score_breakdown],
        "salesperson_stats": [dict(r) for r in sp_stats],
        "users":             [dict(r) for r in users]
    }

# ── LEAD ACTIONS ───────────────────────────────────────────────────────────────

@app.post("/api/v1/leads/{lead_id}/followup")
def mark_followup(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE extractions SET follow_up_done=1, follow_up_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.execute("INSERT INTO follow_ups (extraction_id,done_by,done_at) VALUES (?,?,?)",
        (lead_id, user["email"], datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/stage")
def update_stage(lead_id: int, body: dict, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE extractions SET funnel_stage=?, last_updated=? WHERE id=?",
        (body.get("stage"), datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/convert")
def mark_converted(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE extractions SET conversion_status='converted', funnel_stage='confirmed', last_updated=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/drop")
def mark_dropped(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE extractions SET conversion_status='dropped', last_updated=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/assign")
def assign_lead(lead_id: int, body: dict, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    conn.execute("UPDATE extractions SET assigned_to=? WHERE id=?", (body.get("email"), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.delete("/api/v1/leads/{lead_id}")
def delete_lead(lead_id: int, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    conn.execute("DELETE FROM extractions WHERE id=?", (lead_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

# ── INTELLIGENCE ───────────────────────────────────────────────────────────────

@app.get("/api/v1/intelligence")
def intelligence(user=Depends(get_current_user)):
    conn = get_db()
    # Conversion rate
    total = conn.execute("SELECT COUNT(*) as c FROM extractions").fetchone()["c"]
    converted = conn.execute("SELECT COUNT(*) as c FROM extractions WHERE conversion_status='converted'").fetchone()["c"]
    dropped = conn.execute("SELECT COUNT(*) as c FROM extractions WHERE conversion_status='dropped'").fetchone()["c"]
    conv_rate = round((converted / total * 100), 1) if total > 0 else 0

    # Best performing cake types
    top_cakes = conn.execute("""
        SELECT cake_type, COUNT(*) as total,
               SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END) as converted
        FROM extractions WHERE cake_type IS NOT NULL
        GROUP BY cake_type ORDER BY converted DESC LIMIT 5
    """).fetchall()

    # Average time to convert (days)
    patterns = conn.execute("SELECT pattern_data, outcome FROM conversion_patterns LIMIT 100").fetchall()

    # Hot leads at risk (HOT but no followup in 2 hours)
    two_hours_ago = (datetime.utcnow() - timedelta(hours=2)).isoformat()
    at_risk = conn.execute("""
        SELECT contact_name, city, captured_at, next_action, suggested_reply
        FROM extractions
        WHERE lead_score='HOT' AND follow_up_done=0 AND captured_at < ?
        AND conversion_status='open'
        ORDER BY captured_at ASC LIMIT 10
    """, (two_hours_ago,)).fetchall()

    # City performance
    city_perf = conn.execute("""
        SELECT city, COUNT(*) as total,
               SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END) as converted,
               SUM(CASE WHEN lead_score='HOT' THEN 1 ELSE 0 END) as hot
        FROM extractions GROUP BY city
    """).fetchall()

    conn.close()
    return {
        "total_leads": total,
        "conversion_rate": conv_rate,
        "converted": converted,
        "dropped": dropped,
        "top_cake_types": [dict(r) for r in top_cakes],
        "hot_leads_at_risk": [dict(r) for r in at_risk],
        "city_performance": [dict(r) for r in city_perf]
    }

@app.get("/health")
def health():
    conn = get_db()
    leads = conn.execute("SELECT COUNT(*) as c FROM extractions").fetchone()["c"]
    conn.close()
    return {"status": "ok", "time": datetime.utcnow().isoformat(), "leads": leads}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

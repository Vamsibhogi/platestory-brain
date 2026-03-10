from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import sqlite3, os, re, secrets, hashlib, json
try:
    import anthropic
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False
from datetime import datetime, date, timedelta

app = FastAPI(title="Platestory AIR 6")
security = HTTPBearer(auto_error=False)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENT_SECRET = os.getenv("AGENT_SECRET", "platestory-2025-xK9mP2qR7nL4")
DB_PATH = os.getenv("DB_PATH", "platestory.db")
ADMIN_EMAIL = "vamsi.bhogi@platestory.in"

MONTH_MAP = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,
    "apr":4,"april":4,"may":5,"jun":6,"june":6,"jul":7,"july":7,
    "aug":8,"august":8,"sep":9,"sept":9,"september":9,
    "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}

CAKE_KEYWORDS = {
    "wedding": ["wedding","shaadi","vivah","marriage","bride","groom"],
    "birthday": ["birthday","bday","b-day","janmdin","kids birthday","friends birthday",
                 "wife birthday","husband birthday","baby birthday","daughter birthday",
                 "son birthday","mom birthday","dad birthday","sister birthday","brother birthday"],
    "anniversary": ["anniversary","anniv"],
    "baby_shower": ["baby shower","babyshower","godh bharai"],
    "engagement": ["engagement","roka","ring ceremony","sagai"],
    "custom": ["custom cake","theme cake","designer cake","profit","celebration",
               "corporate","office","launch","milestone","farewell","retirement"]
}

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'salesperson',
        name TEXT,
        created_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS extractions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        salesperson_email TEXT,
        contact_name TEXT,
        message TEXT,
        event_date TEXT,
        cake_type TEXT,
        lead_score TEXT,
        funnel_stage TEXT DEFAULT 'enquiry',
        assigned_to TEXT,
        follow_up_done INTEGER DEFAULT 0,
        follow_up_at TEXT,
        notes TEXT,
        next_action TEXT,
        conversion_probability TEXT,
        business_vertical TEXT,
        estimated_order_value TEXT,
        captured_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS follow_ups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        extraction_id INTEGER,
        done_by TEXT,
        done_at TEXT,
        notes TEXT
    )""")
    conn.commit()

    # Create admin account if not exists
    existing = conn.execute("SELECT * FROM users WHERE email=?", (ADMIN_EMAIL,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO users (email, password_hash, role, name, created_at) VALUES (?,?,?,?,?)",
            (ADMIN_EMAIL, hash_password("platestory@2025"), "admin", "Vamsi", datetime.utcnow().isoformat()))
        conn.commit()
    conn.close()

init_db()

def parse_date_from_message(message: str) -> Optional[str]:
    msg = message.lower()
    msg = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', msg)
    p1 = re.compile(r'(\d{1,2})\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)')
    p2 = re.compile(r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})')
    p3 = re.compile(r'(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?')
    day, month, year = None, None, None
    today = datetime.today()
    m = p1.search(msg)
    if m:
        day, month = int(m.group(1)), MONTH_MAP.get(m.group(2)[:3])
    else:
        m = p2.search(msg)
        if m:
            month, day = MONTH_MAP.get(m.group(1)[:3]), int(m.group(2))
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
    for cake_type, keywords in CAKE_KEYWORDS.items():
        if any(kw in msg for kw in keywords): return cake_type
    return None

def parse_lead_score(message: str) -> str:
    msg = message.lower()
    if any(w in msg for w in ["urgent","asap","today","tomorrow","this week","confirmed","book","advance"]): return "HOT"
    if any(w in msg for w in ["maybe","will think","let me check","not sure","budget issue","too expensive","costly"]): return "COLD"
    return "WARM"

def parse_funnel_stage(message: str) -> str:
    msg = message.lower()
    if any(w in msg for w in ["paid","advance","payment done","transferred","upi","gpay","confirmed"]): return "confirmed"
    if any(w in msg for w in ["how much","price","cost","rate","charges","quote","budget"]): return "quoted"
    if any(w in msg for w in ["reference","ref image","like this","similar to","design","theme","colour","color","flavor","flavour"]): return "ref_shared"
    return "enquiry"

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials: raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    session = conn.execute("SELECT * FROM sessions WHERE token=?", (credentials.credentials,)).fetchone()
    conn.close()
    if not session: raise HTTPException(status_code=401, detail="Invalid or expired session")
    return dict(session)

# ── AUTH ENDPOINTS ────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str

@app.post("/api/v1/auth/login")
def login(req: LoginRequest):
    if not req.email.endswith("@platestory.in"):
        raise HTTPException(status_code=403, detail="Only @platestory.in emails allowed")
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email,)).fetchone()
    if not user or user["password_hash"] != hash_password(req.password):
        conn.close()
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = secrets.token_hex(32)
    conn.execute("INSERT INTO sessions (token, email, role, created_at) VALUES (?,?,?,?)",
        (token, req.email, user["role"], datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return {"token": token, "role": user["role"], "name": user["name"]}

@app.post("/api/v1/auth/register")
def register(req: RegisterRequest, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403, detail="Admin only")
    if not req.email.endswith("@platestory.in"):
        raise HTTPException(status_code=400, detail="Only @platestory.in emails allowed")
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (email, password_hash, role, name, created_at) VALUES (?,?,?,?,?)",
            (req.email, hash_password(req.password), "salesperson", req.name, datetime.utcnow().isoformat()))
        conn.commit()
    except: raise HTTPException(status_code=400, detail="Email already exists")
    finally: conn.close()
    return {"status": "ok"}

@app.get("/api/v1/auth/me")
def me(user=Depends(get_current_user)):
    return user

# ── EXTRACTIONS ───────────────────────────────────────────────────────────────


ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

async def claude_extract(message: str, contact_name: str) -> dict:
    """Use Claude to extract lead info. Falls back to regex if unavailable."""
    if not CLAUDE_AVAILABLE or not ANTHROPIC_API_KEY:
        return {}
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = f"""You are a sales intelligence assistant for Platestory — a premium custom cake brand in Chennai, India.

Platestory's pricing (per kg, excl. customisation & delivery):
- Classic: Vanilla ₹1,107 | Butterscotch ₹1,307 | Chocolate Truffle ₹1,507 | Rainbow ₹1,957
- Specialities: Red Velvet ₹1,707 | Belgian Choc Ganache ₹2,207 | Biscoff ₹2,357 | Berry ₹2,507
- Exotics: Rasmalai ₹2,207 | Tender Coconut ₹1,957 | Mango ₹1,657
- Customisation tiers: 1-tier +₹500 | 2-tier +₹1,500 | 3-tier +₹2,500
- Little Bites: Cupcakes ₹127/pc | Macarons ₹107/pc | Cakesicles ₹157/pc | Cakepops ₹87/pc
- Bento cakes: small format, typically ₹600-₹900 range

Business verticals:
- B2C Little Cakes: Bento, cupcakes, small birthday cakes. Fast cycle, low ticket (₹500-₹2,500). Converts quickly.
- B2C Large Cakes: Wedding, engagement, anniversary, tiered cakes. High ticket (₹3,000-₹30,000+). Longer cycle.
- Corporate: Bulk orders, repeat business. High value.

Conversion signals (from strongest to weakest):
1. Advance paid / says "confirmed" / "I'll pay now" = CONFIRMED
2. Asking for account details / payment link = HOT
3. Event within 7 days + reference image shared = HOT
4. Price accepted, says "okay" or "fine" = HOT
5. Asking for price/quote + specific date mentioned = WARM
6. Sent reference image without asking price yet = WARM
7. Just asking "what flavours do you have" / "do you deliver to X" = COLD
8. No date, no reference, vague = COLD

Negotiation context:
- If customer pushes back on price (e.g. quoted ₹10,300, asks for ₹10,000) = still HOT, close to converting
- If customer says "let me check / will confirm later" = WARM, needs follow-up within 2 hours
- If customer goes silent after price = needs nudge, still WARM for 24 hours

Now analyze this WhatsApp message:
Contact: {contact_name}
Message: {message}

Return ONLY a JSON object, no explanation, no markdown:
{{
  "cake_type": "birthday|wedding|anniversary|engagement|baby_shower|corporate|bento|little_bites|custom|unknown",
  "event_date": "YYYY-MM-DD or null",
  "lead_score": "HOT|WARM|COLD",
  "funnel_stage": "enquiry|ref_shared|quoted|confirmed",
  "business_vertical": "little_cakes|large_cakes|corporate|unknown",
  "estimated_order_value": "low(<2500)|mid(2500-8000)|high(8000-20000)|premium(20000+)|unknown",
  "urgency_days": null or integer,
  "conversion_probability": "high|medium|low",
  "next_action": "one line — what salesperson should do next",
  "notes": "one line summary of customer intent"
}}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"Claude extraction error: {e}")
        return {}

class ExtractionRequest(BaseModel):
    salesperson_email: str
    contact_name: str
    message: str

class LegacyBatch(BaseModel):
    salesperson_id: str
    device_id: str
    batch_size: int = 1
    extractions: List[dict]

@app.post("/api/v1/extractions")
async def extract(req: Request):
    auth = req.headers.get("Authorization", "")
    body = await req.json()

    # Android legacy format
    if auth == f"Bearer {AGENT_SECRET}":
        extractions = body.get("extractions", [])
        salesperson = body.get("salesperson_id", "unknown")
        conn = get_db()
        for item in extractions:
            msg = item.get("message", "")
            contact = item.get("contact_name", "")
            if not msg or not contact or contact in ["Add status","Calls","Chats"]: continue
            if len(msg) < 5: continue
            conn.execute("""INSERT INTO extractions 
                (salesperson_email, contact_name, message, event_date, cake_type, lead_score, funnel_stage, captured_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (salesperson, contact, msg,
                 parse_date_from_message(msg), parse_cake_type(msg),
                 parse_lead_score(msg), parse_funnel_stage(msg),
                 datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()
        return {"status": "ok"}

    # Chrome extension format (session token)
    token = auth.replace("Bearer ", "")
    conn = get_db()
    session = conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone()
    if not session:
        # Try X-Session-Token header
        token = req.headers.get("X-Session-Token", "")
        session = conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone()
    if not session:
        conn.close()
        raise HTTPException(status_code=401, detail="Not authenticated")

    contact = body.get("contact_name", "")
    msg = body.get("message", "")
    salesperson = body.get("salesperson_email", session["email"])

    if not contact or not msg or len(msg) < 5: 
        conn.close()
        return {"status": "skipped"}
    if contact in ["Add status","Calls","Chats","Status"]:
        conn.close()
        return {"status": "skipped"}

    # Try Claude first, fall back to regex
    ai = await claude_extract(msg, contact)
    event_date           = ai.get("event_date")            or parse_date_from_message(msg)
    cake_type            = ai.get("cake_type")             or parse_cake_type(msg)
    lead_score           = ai.get("lead_score")            or parse_lead_score(msg)
    funnel_stage         = ai.get("funnel_stage")          or parse_funnel_stage(msg)
    notes                = ai.get("notes", "")
    next_action          = ai.get("next_action", "")
    conversion_prob      = ai.get("conversion_probability", "")
    business_vertical    = ai.get("business_vertical", "")
    estimated_order_value= ai.get("estimated_order_value", "")

    conn.execute("""INSERT INTO extractions
        (salesperson_email, contact_name, message, event_date, cake_type, lead_score,
         funnel_stage, notes, next_action, conversion_probability, business_vertical,
         estimated_order_value, captured_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (salesperson, contact, msg,
         event_date, cake_type, lead_score, funnel_stage,
         notes, next_action, conversion_prob, business_vertical,
         estimated_order_value, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return {"status": "ok", "ai_extraction": bool(ai), "score": lead_score, "next_action": next_action, "notes": notes}

@app.get("/api/v1/extractions/recent")
def recent_extractions(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials if credentials else ""
    conn = get_db()
    # Allow both agent secret and session token
    if token != AGENT_SECRET:
        session = conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone()
        if not session:
            conn.close()
            raise HTTPException(status_code=401)
    rows = conn.execute("""
        SELECT contact_name, message, event_date, cake_type, lead_score, funnel_stage,
               salesperson_email, captured_at
        FROM extractions ORDER BY captured_at DESC LIMIT 20
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── DASHBOARD DATA ────────────────────────────────────────────────────────────

@app.get("/api/v1/dashboard")
def dashboard(user=Depends(get_current_user)):
    conn = get_db()
    today = datetime.today().date()
    next30 = today + timedelta(days=30)
    is_admin = user["role"] == "admin"
    email = user["email"]

    where = "" if is_admin else f"AND salesperson_email='{email}'"

    upcoming = conn.execute(f"""
        SELECT contact_name, event_date, cake_type, lead_score, message, salesperson_email, funnel_stage
        FROM extractions WHERE event_date BETWEEN ? AND ? {where}
        ORDER BY event_date ASC
    """, (today.isoformat(), next30.isoformat())).fetchall()

    hot = conn.execute(f"""
        SELECT contact_name, message, cake_type, lead_score, captured_at, salesperson_email, funnel_stage
        FROM extractions WHERE lead_score='HOT' {where}
        ORDER BY captured_at DESC LIMIT 20
    """).fetchall()

    recent = conn.execute(f"""
        SELECT id, contact_name, message, cake_type, lead_score, event_date,
               captured_at, salesperson_email, funnel_stage, follow_up_done, assigned_to,
               notes, next_action, conversion_probability, business_vertical, estimated_order_value
        FROM extractions WHERE 1=1 {where}
        ORDER BY captured_at DESC LIMIT 50
    """).fetchall()

    total = conn.execute(f"SELECT COUNT(*) as c FROM extractions WHERE 1=1 {where}").fetchone()["c"]

    # Unattended — no follow up in 4 hours
    four_hours_ago = (datetime.utcnow() - timedelta(hours=4)).isoformat()
    unattended = conn.execute(f"""
        SELECT id, contact_name, message, salesperson_email, captured_at, cake_type
        FROM extractions 
        WHERE follow_up_done=0 AND captured_at < ? {where}
        ORDER BY captured_at ASC LIMIT 20
    """, (four_hours_ago,)).fetchall()

    # Funnel breakdown
    funnel = conn.execute(f"""
        SELECT funnel_stage, COUNT(*) as count FROM extractions WHERE 1=1 {where}
        GROUP BY funnel_stage
    """).fetchall()

    # Salesperson stats (admin only)
    sp_stats = []
    if is_admin:
        sp_stats = conn.execute("""
            SELECT salesperson_email, COUNT(*) as total,
                   SUM(CASE WHEN lead_score='HOT' THEN 1 ELSE 0 END) as hot,
                   SUM(CASE WHEN follow_up_done=1 THEN 1 ELSE 0 END) as followed_up
            FROM extractions GROUP BY salesperson_email
        """).fetchall()

    # Users list (admin only)
    users = []
    if is_admin:
        users = conn.execute("SELECT email, name, role FROM users").fetchall()

    conn.close()
    return {
        "role": user["role"],
        "email": email,
        "total_customers": total,
        "confirmed_orders": confirmed,
        "slots_used": min(confirmed, 300),
        "slots_total": 300,
        "upcoming_events": [dict(r) for r in upcoming],
        "hot_leads": [dict(r) for r in hot],
        "recent_leads": [dict(r) for r in recent],
        "unattended_leads": [dict(r) for r in unattended],
        "funnel": [dict(r) for r in funnel],
        "salesperson_stats": [dict(r) for r in sp_stats],
        "users": [dict(r) for r in users]
    }

@app.post("/api/v1/leads/{lead_id}/followup")
def mark_followup(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE extractions SET follow_up_done=1, follow_up_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.execute("INSERT INTO follow_ups (extraction_id, done_by, done_at) VALUES (?,?,?)",
        (lead_id, user["email"], datetime.utcnow().isoformat()))
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

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

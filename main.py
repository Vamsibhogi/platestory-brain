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

# --- CONFIGURATION & ENVIRONMENT ---
# Uses the Agent Secret for Android App authentication
AGENT_SECRET   = os.getenv("AGENT_SECRET", "platestory-2025-xK9mP2qR7nL4")
# Path configured for Railway Volume persistence
DB_PATH        = os.getenv("DB_PATH", "/data/platestory.db") 
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

# --- DATABASE ENGINE ---
def hash_password(p): return hashlib.sha256(p.encode()).hexdigest()

def get_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    # Core system tables
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
    # Schema migration for existing DBs
    for col, defn in [("has_image", "INTEGER DEFAULT 0"), ("city", "TEXT DEFAULT 'chennai'"), 
                     ("conversion_status", "TEXT DEFAULT 'open'"), ("last_updated", "TEXT"), ("suggested_reply", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE extractions ADD COLUMN {col} {defn}")
            conn.commit()
        except: pass
    # Default Admin setup
    existing = conn.execute("SELECT * FROM users WHERE email=?", (ADMIN_EMAIL,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO users (email,password_hash,role,name,city,created_at) VALUES (?,?,?,?,?,?)",
            (ADMIN_EMAIL, hash_password("platestory@2025"), "admin", "Vamsi", "chennai", datetime.utcnow().isoformat()))
        conn.commit()
    conn.close()

init_db()

# --- UTILITY HELPERS ---
def detect_city_from_message(message: str, contact: str) -> str:
    msg = (message + " " + contact).lower()
    hyd_kw = ["hyderabad","hyd","jubilee hills","banjara hills","gachibowli","kondapur","hitech city"]
    chn_kw = ["chennai","madras","anna nagar","velachery","adyar","t nagar","tnagar","omr","ecr"]
    if any(k in msg for k in hyd_kw): return "hyderabad"
    if any(k in msg for k in chn_kw): return "chennai"
    return "unknown"

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials: raise HTTPException(status_code=401)
    conn = get_db()
    session = conn.execute("SELECT * FROM sessions WHERE token=?", (credentials.credentials,)).fetchone()
    conn.close()
    if not session: raise HTTPException(status_code=401)
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

# --- CLAUDE AI INTELLIGENCE ---
async def claude_extract(message: str, contact_name: str, conv_ctx: str = "", has_image: bool = False, existing_lead: dict = None) -> dict:
    if not CLAUDE_AVAILABLE or not ANTHROPIC_KEY: return {}
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        # Detailed prompt including Platestory menu and pricing
        prompt = f"""You are the AI sales brain for Platestory Cakes. 
        Pricing: Classic 1107/kg, Special 2207/kg, Exotic 2207/kg. Bento 600-900.
        Message: {message} from {contact_name}. Image attached: {has_image}.
        Return ONLY valid JSON with: cake_type, event_date, lead_score (HOT/WARM/COLD), 
        funnel_stage, city (chennai/hyderabad/unknown), suggested_reply, conversion_status, next_action, drop_detected."""
        
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        return json.loads(re.sub(r"```json|```", "", text).strip())
    except Exception as e:
        print(f"Claude error: {e}")
        return {}

# --- API ROUTES ---

@app.post("/api/v1/auth/login")
def login(req: dict):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.get("email"),)).fetchone()
    if not user or user["password_hash"] != hash_password(req.get("password")):
        conn.close(); raise HTTPException(status_code=401)
    token = secrets.token_hex(32)
    conn.execute("INSERT INTO sessions (token,email,role,created_at) VALUES (?,?,?,?)",
        (token, user["email"], user["role"], datetime.utcnow().isoformat()))
    conn.commit(); conn.close()
    return {"token": token, "role": user["role"], "city": user["city"]}

@app.post("/api/v1/extractions")
async def extract(req: Request, background_tasks: BackgroundTasks):
    auth = req.headers.get("Authorization", "")
    body = await req.json()
    is_agent = auth == f"Bearer {AGENT_SECRET}"

    # Determine salesperson context
    if is_agent:
        salesperson = body.get("salesperson_id", ADMIN_EMAIL)
        items = body.get("extractions") or [body]
    else:
        token = auth.replace("Bearer ", "")
        conn = get_db(); session = conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone(); conn.close()
        if not session: raise HTTPException(status_code=401)
        salesperson = session["email"]
        items = [body]

    conn = get_db()
    results = []
    alerts = []

    for item in items:
        msg = item.get("message", "").strip()
        contact = item.get("contact_name", "").strip()
        has_image = bool(item.get("has_image", False))

        if not msg or not contact: continue

        # --- FIX: CALL CLAUDE FIRST TO DEFINE 'AI' VARIABLE ---
        ai = await claude_extract(msg, contact, item.get("conversation_context", ""), has_image)
        if not ai: ai = {}

        # --- NOW ACCESS 'AI' FOR CITY LOGIC ---
        ai_city = ai.get("city", "unknown")
        city = ai_city if ai_city in ["chennai", "hyderabad"] else detect_city_from_message(msg, contact)

        now_iso = datetime.utcnow().isoformat()
        conn.execute("""INSERT INTO extractions
            (salesperson_email, contact_name, message, event_date, cake_type, lead_score,
             funnel_stage, city, suggested_reply, has_image, captured_at, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (salesperson, contact, msg, ai.get("event_date"), ai.get("cake_type"),
             ai.get("lead_score", "WARM"), ai.get("funnel_stage", "enquiry"),
             city, ai.get("suggested_reply"), 1 if has_image else 0, now_iso, now_iso))
        
        # Capture for email alerts if score is HOT
        if ai.get("lead_score") == "HOT":
            alerts.append(f"HOT LEAD: {contact} ({city})\nMessage: {msg[:100]}")

        results.append({"contact": contact, "score": ai.get("lead_score", "WARM"), "city": city})

    conn.commit(); conn.close()
    
    if alerts:
        background_tasks.add_task(send_email_alert, "New Alerts", "\n\n".join(alerts))

    return {"status": "ok", "processed": len(results), "results": results}

@app.get("/api/v1/dashboard")
def dashboard(user=Depends(get_current_user)):
    conn = get_db()
    # Fetch recent leads and city slot utilization
    recent = conn.execute("SELECT * FROM extractions ORDER BY captured_at DESC LIMIT 100").fetchall()
    stats = conn.execute("SELECT city, COUNT(*) as count FROM extractions WHERE funnel_stage='confirmed' GROUP BY city").fetchall()
    conn.close()
    return {"leads": [dict(r) for r in recent], "city_usage": [dict(s) for s in stats]}

@app.get("/health")
def health():
    return {"status": "online", "db_connected": True, "path": DB_PATH}

if __name__ == "__main__":
    import uvicorn
    # Use environment port for Railway deployment
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

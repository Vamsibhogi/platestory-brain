from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import sqlite3, os, re, secrets, hashlib, json, smtplib
from email.mime.text import MIMEText
try:
    from openai import OpenAI as _OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
try:
    import anthropic as _anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
from datetime import datetime, date, timedelta

app = FastAPI(title="Platestory AIR 6")
security = HTTPBearer(auto_error=False)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def serve_dashboard():
    """Serve the Platestory dashboard at the root URL."""
    dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    if os.path.exists(dashboard_path):
        with open(dashboard_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard file not found</h1><p>dashboard.html is missing from the deployment.</p>", status_code=404)

AGENT_SECRET   = os.getenv("AGENT_SECRET", "platestory-2025-xK9mP2qR7nL4")
DB_PATH        = os.getenv("DB_PATH", "/data/platestory.db")
ADMIN_EMAIL    = "vamsi.bhogi@platestory.in"
OPENAI_KEY     = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")  # default to real OpenAI API
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_KEY     = os.getenv("GEMINI_API_KEY", "")
SMTP_HOST      = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER      = os.getenv("SMTP_USER", "")
SMTP_PASS      = os.getenv("SMTP_PASS", "")
ALERT_EMAIL    = os.getenv("ALERT_EMAIL", "vamsi.bhogi@platestory.in")

CITIES = {
    "chennai":   {"name": "Chennai",   "slots": 300},
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

# ── NOISE FILTER ───────────────────────────────────────────────────────────────
# Contacts and message patterns that are NOT real customers

NOISE_CONTACTS = {
    "WA Business", "WhatsApp Business", "WhatsApp", "Chats", "Calls",
    "Status", "Add status", "Camera", "WhatsApp Web", "Backup",
    "Missed video call", "Missed voice call"
}

NOISE_MESSAGE_PATTERNS = [
    r"^checking for new messages$",
    r"^messages and calls are end-to-end encrypted",
    r"^tap to learn more$",
    r"^you're now connected",
    r"^\+?\d{10,15}$",  # pure phone number
    r"^(hi|hello|hey|ok|okay|yes|no|thanks|thank you|noted|sure)\.?$",  # single word greetings (too vague alone)
]

def is_noise(contact: str, message: str) -> bool:
    if contact in NOISE_CONTACTS:
        return True
    # Contact name is just a phone number
    if re.match(r"^\+?\d[\d\s\-]{8,}$", contact.strip()):
        return False  # phone-number contacts ARE real customers, don't filter
    msg_lower = message.lower().strip()
    for pattern in NOISE_MESSAGE_PATTERNS:
        if re.match(pattern, msg_lower, re.IGNORECASE):
            return True
    return False

# ── PHONE NUMBER EXTRACTION ────────────────────────────────────────────────────

def extract_phone_number(contact_name: str, message: str) -> Optional[str]:
    """Extract phone number from contact name or message text."""
    # Check if contact name IS a phone number
    phone_in_name = re.search(r'\+?\d[\d\s\-]{9,14}', contact_name)
    if phone_in_name:
        digits = re.sub(r'[\s\-]', '', phone_in_name.group())
        if len(digits) >= 10:
            return digits

    # Check message for phone number
    phone_in_msg = re.search(r'(?:my number|call me|whatsapp me|contact|phone|mobile)[:\s]*(\+?\d[\d\s\-]{9,14})', message, re.IGNORECASE)
    if phone_in_msg:
        digits = re.sub(r'[\s\-]', '', phone_in_msg.group(1))
        if len(digits) >= 10:
            return digits

    # Bare phone number in message
    bare_phone = re.search(r'\b(\+91[\s\-]?\d{10}|\d{10})\b', message)
    if bare_phone:
        return re.sub(r'[\s\-]', '', bare_phone.group())

    return None

# ── REPEAT ORDER DETECTION ─────────────────────────────────────────────────────

def is_new_order_context(existing_lead: dict, new_message: str, ai_result: dict) -> bool:
    """
    Returns True if this message represents a NEW order from an existing customer,
    rather than a continuation of the current conversation.
    """
    if not existing_lead:
        return False

    existing_stage = existing_lead.get("funnel_stage", "enquiry")
    existing_status = existing_lead.get("conversion_status", "open")

    # If previous order was confirmed/converted, any new enquiry = new order
    if existing_status in ("converted", "dropped") and existing_stage in ("confirmed",):
        new_stage = ai_result.get("funnel_stage", "enquiry")
        if new_stage in ("enquiry", "ref_shared"):
            return True

    # If previous event date has passed and customer is asking about a new date
    prev_event = existing_lead.get("event_date")
    if prev_event:
        try:
            prev_date = date.fromisoformat(prev_event)
            if prev_date < date.today():
                new_event = ai_result.get("event_date")
                if new_event and new_event != prev_event:
                    return True
        except:
            pass

    return False

def get_order_number(conn, contact_name: str) -> int:
    """Count how many orders this customer has had."""
    count = conn.execute(
        "SELECT COUNT(*) as c FROM customers WHERE contact_name=?", (contact_name,)
    ).fetchone()
    return (count["c"] if count else 0) + 1

# ── DATABASE ───────────────────────────────────────────────────────────────────

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

    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'salesperson', name TEXT,
        city TEXT DEFAULT 'chennai', created_at TEXT)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY, email TEXT NOT NULL,
        role TEXT NOT NULL, created_at TEXT NOT NULL)""")

    # NEW: customers table — one row per customer per order
    conn.execute("""CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_name TEXT NOT NULL,
        phone_number TEXT,
        order_number INTEGER DEFAULT 1,
        salesperson_email TEXT,

        -- Conversation
        messages TEXT DEFAULT '[]',
        last_message TEXT,
        message_count INTEGER DEFAULT 0,

        -- AI-extracted intelligence (with confidence)
        cake_type TEXT, cake_type_confidence TEXT DEFAULT 'uncertain',
        event_date TEXT, event_date_confidence TEXT DEFAULT 'uncertain',
        budget_range TEXT, budget_confidence TEXT DEFAULT 'uncertain',
        city TEXT DEFAULT 'unknown', city_confidence TEXT DEFAULT 'uncertain',
        weight_kg TEXT, weight_confidence TEXT DEFAULT 'uncertain',
        flavour TEXT, flavour_confidence TEXT DEFAULT 'uncertain',
        occasion_detail TEXT,

        -- Lead intelligence
        lead_score TEXT DEFAULT 'WARM',
        funnel_stage TEXT DEFAULT 'enquiry',
        conversion_probability TEXT DEFAULT 'low',
        conversion_status TEXT DEFAULT 'open',
        business_vertical TEXT,
        estimated_order_value TEXT,
        drop_detected INTEGER DEFAULT 0,

        -- Co-pilot
        next_action TEXT,
        copilot_recommendation TEXT,
        suggested_reply TEXT,
        urgency_flag INTEGER DEFAULT 0,

        -- Metadata
        has_image INTEGER DEFAULT 0,
        follow_up_done INTEGER DEFAULT 0,
        follow_up_at TEXT,
        assigned_to TEXT,
        notes TEXT,
        captured_at TEXT,
        last_updated TEXT)""")

    # Legacy extractions table — keep for backward compat
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
        customer_id INTEGER, done_by TEXT, done_at TEXT, notes TEXT)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS conversion_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern_type TEXT, pattern_data TEXT, outcome TEXT, created_at TEXT)""")

    # B2B outbound tracker
    conn.execute("""CREATE TABLE IF NOT EXISTS b2b_prospects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        contact_person TEXT,
        phone TEXT,
        email TEXT,
        industry TEXT,
        city TEXT DEFAULT 'chennai',
        status TEXT DEFAULT 'not_contacted',
        notes TEXT,
        last_contact_at TEXT,
        next_followup_at TEXT,
        potential_value TEXT,
        assigned_to TEXT,
        created_by TEXT,
        created_at TEXT,
        updated_at TEXT)""")

    conn.commit()

    # Schema migrations for existing deployments
    for col, defn in [
        ("phone_number", "TEXT"),
        ("order_number", "INTEGER DEFAULT 1"),
        ("copilot_recommendation", "TEXT"),
        ("urgency_flag", "INTEGER DEFAULT 0"),
        ("cake_type_confidence", "TEXT DEFAULT 'uncertain'"),
        ("event_date_confidence", "TEXT DEFAULT 'uncertain'"),
        ("budget_confidence", "TEXT DEFAULT 'uncertain'"),
        ("city_confidence", "TEXT DEFAULT 'uncertain'"),
        ("weight_kg", "TEXT"),
        ("weight_confidence", "TEXT DEFAULT 'uncertain'"),
        ("flavour", "TEXT"),
        ("flavour_confidence", "TEXT DEFAULT 'uncertain'"),
        ("occasion_detail", "TEXT"),
        ("budget_range", "TEXT"),
        ("messages", "TEXT DEFAULT '[]'"),
        ("message_count", "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE customers ADD COLUMN {col} {defn}")
            conn.commit()
        except: pass

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

    try:
        conn.execute("ALTER TABLE users ADD COLUMN assigned_cities TEXT DEFAULT '[]'")
        conn.commit()
    except: pass

    # Back-fill assigned_cities from city column for existing users
    try:
        users_to_fix = conn.execute("SELECT email, city, assigned_cities FROM users").fetchall()
        for u in users_to_fix:
            existing = json.loads(u['assigned_cities'] or '[]')
            if not existing and u['city']:
                conn.execute("UPDATE users SET assigned_cities=? WHERE email=?",
                    (json.dumps([u['city']]), u['email']))
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

def detect_city_from_message(message: str, contact: str) -> str:
    msg = (message + " " + contact).lower()
    hyd_kw = ["hyderabad","hyd","jubilee hills","banjara hills","gachibowli",
              "kondapur","hitech city","madhapur","secunderabad","ameerpet",
              "kukatpally","miyapur","begumpet","dilsukhnagar","lb nagar"]
    chn_kw = ["chennai","madras","anna nagar","velachery","adyar","t nagar",
              "tnagar","tambaram","porur","omr","ecr","nungambakkam",
              "mylapore","guindy","chromepet","perambur","sholinganallur"]
    if any(k in msg for k in hyd_kw): return "hyderabad"
    if any(k in msg for k in chn_kw): return "chennai"
    return "unknown"

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

def days_until(event_date_str: str) -> Optional[int]:
    try:
        d = date.fromisoformat(event_date_str)
        return (d - date.today()).days
    except:
        return None

# ── CLAUDE AI ──────────────────────────────────────────────────────────────────

async def claude_extract(message: str, contact_name: str,
                          conversation_history: list = None,
                          has_image: bool = False,
                          existing_customer: dict = None) -> dict:
    # Try Anthropic first (real key on Railway), fall back to OpenAI proxy
    if not (ANTHROPIC_AVAILABLE and ANTHROPIC_KEY) and not (OPENAI_AVAILABLE and OPENAI_KEY):
        return {}
    try:
        # Build conversation context block
        context_block = ""
        if conversation_history:
            context_block = "\n\nFull conversation so far (oldest first):\n"
            for m in conversation_history[-15:]:
                context_block += f"- {m}\n"

        # Build existing customer profile block
        existing_block = ""
        if existing_customer:
            days = days_until(existing_customer.get("event_date", "")) if existing_customer.get("event_date") else None
            days_str = f"{days} days away" if days is not None else "unknown"
            existing_block = f"""
Current customer profile:
- Lead score: {existing_customer.get('lead_score', 'unknown')}
- Stage: {existing_customer.get('funnel_stage', 'unknown')}
- Cake type: {existing_customer.get('cake_type', 'unknown')} (confidence: {existing_customer.get('cake_type_confidence', 'uncertain')})
- Event date: {existing_customer.get('event_date', 'unknown')} ({days_str})
- Budget: {existing_customer.get('budget_range', 'unknown')} (confidence: {existing_customer.get('budget_confidence', 'uncertain')})
- City: {existing_customer.get('city', 'unknown')} (confidence: {existing_customer.get('city_confidence', 'uncertain')})
- Notes: {existing_customer.get('notes', '')}
Update all fields based on new message. Promote confidence from uncertain→likely→confirmed as more evidence arrives."""

        image_block = "\nNOTE: Customer sent an image/photo — likely a reference design. Treat as ref_shared signal." if has_image else ""

        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        prompt = f"""You are the AI intelligence core of Platestory — a premium custom cake brand operating in Chennai and Hyderabad. You think like a Palantir analyst: you stitch together every fragment of conversation, infer unstated intent, resolve ambiguity using context, and build a continuously-evolving customer intelligence profile. You never leave a field blank if evidence exists anywhere in the conversation.

Today's date: {today_str}

━━━ PLATESTORY PRICING ━━━
Cakes (per kg):
• Vanilla ₹1,107 | Butterscotch ₹1,307 | Chocolate Truffle ₹1,507 | Rainbow ₹1,957
• Red Velvet ₹1,707 | Belgian Choc Ganache ₹2,207 | Biscoff ₹2,357 | Berry ₹2,507
• Rasmalai ₹2,207 | Tender Coconut ₹1,957 | Mango ₹1,657
Tiers: 1-tier +₹500 | 2-tier +₹1,500 | 3-tier +₹2,500
Little Bites: Cupcakes ₹127/pc | Macarons ₹107/pc | Cakesicles ₹157/pc | Cakepops ₹87/pc
Bento: ₹600–₹900

━━━ INTELLIGENCE RULES ━━━
1. NEVER return null/unknown if ANY message in the conversation contains the answer — scan the full history.
2. Infer from context: "for my daughter" → birthday cake; "office party" → corporate; "15 kg" → large_cakes; "freeze at 3kg" → weight_kg=3kg confirmed.
3. Budget inference: if customer says "1500 only" → budget_range=₹1,500, budget_confidence=confirmed. If they say "around 2500" → ₹2,000–₹3,000, likely.
4. Date inference: "28th March" → 2026-03-28. "next week" → approximate. "April 5th" → 2026-04-05. Always use current year {today_str[:4]} unless stated otherwise.
5. Weight inference: "3kg", "freeze at 3 kg", "for 15 kg" → extract the number. "small cake" → likely 0.5–1kg.
6. City inference: area names like "Anna Nagar", "Velachery", "T Nagar", "Adyar", "OMR" → Chennai. "Banjara Hills", "Jubilee Hills", "Gachibowli", "Hitech City", "Kondapur" → Hyderabad.
7. Flavour inference: "chocolate", "choco truffle", "red velvet", "biscoff", "vanilla", "mango" → extract directly.
8. Occasion inference: "daughter's birthday", "wife's anniversary", "office farewell", "baby shower" → set occasion_detail precisely.
9. Progressive confidence: if a field was previously "uncertain" and new message confirms it → upgrade to "confirmed".
10. Urgency: event within 5 days → urgency_flag=true, lead_score=HOT regardless of other signals.
11. Order value calculation: use weight × flavour price + tier cost. If weight unknown, estimate from occasion (birthday 1-2kg, wedding 5kg+, corporate 3kg+).

━━━ CONVERSION SIGNALS ━━━
HOT: payment sent | advance paid | "confirmed" | asking UPI/payment link | event ≤5 days
WARM: price asked + date given | ref image sent | specific requirements stated | "how much for X"
COLD: gone silent | "will think" | "too expensive" | "out of budget" | no response after quote
DROPPED: "cancel" | "not required" | "nevermind" | "will manage elsewhere"
{existing_block}{context_block}{image_block}

Contact: {contact_name}
Latest message: {message}

━━━ TASK ━━━
Analyze ALL messages above (not just the latest). Build the most complete profile possible. Every field you leave null is a missed sales opportunity.

Return ONLY valid JSON (no markdown, no explanation):
{{
  "cake_type": "birthday|wedding|anniversary|engagement|baby_shower|corporate|bento|little_bites|custom|unknown",
  "cake_type_confidence": "confirmed|likely|uncertain",
  "event_date": "YYYY-MM-DD or null",
  "event_date_confidence": "confirmed|likely|uncertain",
  "budget_range": "e.g. ₹1,500 or ₹2,000–₹3,000 or null",
  "budget_confidence": "confirmed|likely|uncertain",
  "weight_kg": "e.g. 2kg or 3kg or null",
  "weight_confidence": "confirmed|likely|uncertain",
  "flavour": "e.g. chocolate truffle or null",
  "flavour_confidence": "confirmed|likely|uncertain",
  "city": "chennai|hyderabad|unknown",
  "city_confidence": "confirmed|likely|uncertain",
  "city_clarification": "natural question to ask if city unknown, else null",
  "occasion_detail": "e.g. daughter's 5th birthday | wife's anniversary | office farewell or null",
  "lead_score": "HOT|WARM|COLD",
  "funnel_stage": "enquiry|ref_shared|quoted|confirmed",
  "conversion_status": "open|converted|dropped",
  "conversion_probability": "high|medium|low",
  "business_vertical": "little_cakes|large_cakes|corporate|unknown",
  "estimated_order_value": "low(<2500)|mid(2500-8000)|high(8000-20000)|premium(20000+)|unknown",
  "urgency_flag": false,
  "drop_detected": false,
  "next_action": "one precise action the exec must take RIGHT NOW — specific, actionable, time-aware",
  "copilot_recommendation": "3-4 sentence briefing: who this customer is, what they want, where they are in the funnel, what to say next and why. Be specific — use names, dates, amounts, flavours where known.",
  "notes": "one line complete summary of this customer",
  "suggested_reply": "natural WhatsApp reply (2-3 sentences, warm, personal, not salesy). Reference what they asked. If city unknown, ask naturally."
}}"""

        text = None
        last_error = None

        # Layer 1: Anthropic Claude 3.5 Haiku (primary — best structured extraction)
        if ANTHROPIC_AVAILABLE and ANTHROPIC_KEY:
            try:
                ant_client = _anthropic.Anthropic(api_key=ANTHROPIC_KEY)
                ant_response = ant_client.messages.create(
                    model="claude-3-5-haiku-20241022",
                    max_tokens=1200,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = ant_response.content[0].text.strip()
                print("AI: Claude 3.5 Haiku succeeded")
            except Exception as e:
                last_error = e
                print(f"AI Layer 1 (Claude) failed: {e}")

        # Layer 2: OpenAI GPT-4.1 Mini (first fallback)
        if text is None and OPENAI_AVAILABLE and OPENAI_KEY:
            try:
                client_kwargs = {"api_key": OPENAI_KEY}
                if OPENAI_BASE_URL:
                    client_kwargs["base_url"] = OPENAI_BASE_URL
                oai_client = _OpenAI(**client_kwargs)
                oai_response = oai_client.chat.completions.create(
                    model="gpt-4.1-mini",
                    max_tokens=1200,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = oai_response.choices[0].message.content.strip()
                print("AI: GPT-4.1 Mini fallback succeeded")
            except Exception as e:
                last_error = e
                print(f"AI Layer 2 (GPT-4.1 Mini) failed: {e}")

        # Layer 3: Gemini Flash (second fallback via OpenAI-compatible API)
        if text is None and GEMINI_KEY:
            try:
                gem_client = _OpenAI(
                    api_key=GEMINI_KEY,
                    base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
                )
                gem_response = gem_client.chat.completions.create(
                    model="gemini-2.0-flash",
                    max_tokens=1200,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = gem_response.choices[0].message.content.strip()
                print("AI: Gemini Flash fallback succeeded")
            except Exception as e:
                last_error = e
                print(f"AI Layer 3 (Gemini Flash) failed: {e}")

        if text is None:
            print(f"All AI layers failed. Last error: {last_error}")
            return {}

        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"AI extraction error: {e}")
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
    assigned_cities: List[str] = []

class UpdateUserRequest(BaseModel):
    assigned_cities: Optional[List[str]] = None
    name: Optional[str] = None
    city: Optional[str] = None

@app.post("/api/v1/auth/login")
def login(req: LoginRequest):
    # Always lowercase email for case-insensitive login (e.g. Kenneth vs kenneth)
    email_lower = req.email.strip().lower()
    if not email_lower.endswith("@platestory.in"):
        raise HTTPException(status_code=403, detail="Only @platestory.in emails")
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE LOWER(email)=?", (email_lower,)).fetchone()
    if not user or user["password_hash"] != hash_password(req.password):
        conn.close()
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = secrets.token_hex(32)
    conn.execute("INSERT INTO sessions (token,email,role,created_at) VALUES (?,?,?,?)",
        (token, user["email"], user["role"], datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    user_dict = dict(user)
    assigned = json.loads(user_dict.get("assigned_cities") or "[]")
    if not assigned:
        assigned = [user_dict.get("city", "chennai")]
    return {"token": token, "role": user["role"], "name": user["name"],
            "city": user_dict.get("city", "chennai"),
            "assigned_cities": assigned}

@app.post("/api/v1/auth/register")
def register(req: RegisterRequest, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    # Always store emails in lowercase to prevent case mismatch issues
    email_lower = req.email.strip().lower()
    if not email_lower.endswith("@platestory.in"):
        raise HTTPException(status_code=400, detail="Only @platestory.in emails")
    conn = get_db()
    assigned = req.assigned_cities if req.assigned_cities else [req.city]
    try:
        conn.execute("INSERT INTO users (email,password_hash,role,name,city,assigned_cities,created_at) VALUES (?,?,?,?,?,?,?)",
            (email_lower, hash_password(req.password), "salesperson", req.name, req.city, json.dumps(assigned), datetime.utcnow().isoformat()))
        conn.commit()
    except: raise HTTPException(status_code=400, detail="Email already exists")
    finally: conn.close()
    return {"status": "ok"}

@app.get("/api/v1/auth/users")
def list_users(user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    users = conn.execute("SELECT id, email, name, role, city, assigned_cities, created_at FROM users").fetchall()
    conn.close()
    result = []
    for u in users:
        ud = dict(u)
        ud['assigned_cities'] = json.loads(ud.get('assigned_cities') or '[]') or [ud.get('city','chennai')]
        result.append(ud)
    return result

@app.put("/api/v1/auth/users/{user_id}")
def update_user(user_id: int, req: UpdateUserRequest, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    if req.assigned_cities is not None:
        primary_city = req.assigned_cities[0] if req.assigned_cities else "chennai"
        conn.execute("UPDATE users SET assigned_cities=?, city=? WHERE id=?",
            (json.dumps(req.assigned_cities), primary_city, user_id))
    if req.name is not None:
        conn.execute("UPDATE users SET name=? WHERE id=?", (req.name, user_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

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

@app.post("/api/v1/auth/users/{user_id}/reset-password")
def reset_password(user_id: int, req: dict, user=Depends(get_current_user)):
    """Admin-only: reset any user's password."""
    if user["role"] != "admin": raise HTTPException(status_code=403)
    new_password = req.get("password", "").strip()
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    conn = get_db()
    conn.execute("UPDATE users SET password_hash=? WHERE id=?",
        (hash_password(new_password), user_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ── EXTRACTIONS (SMART UPSERT) ─────────────────────────────────────────────────

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
        msg      = item.get("message", "").strip()
        contact  = item.get("contact_name", "").strip()
        conv_ctx = item.get("conversation_context", [])
        has_image = bool(item.get("has_image", False))

        if not msg or not contact or len(msg) < 2: continue
        if is_noise(contact, msg): continue

        # Extract phone number
        phone = extract_phone_number(contact, msg)

        # Get existing customer profile (most recent open order)
        existing = conn.execute(
            """SELECT * FROM customers WHERE contact_name=?
               ORDER BY captured_at DESC LIMIT 1""",
            (contact,)
        ).fetchone()
        existing_dict = dict(existing) if existing else None

        # Build conversation history
        if existing_dict and existing_dict.get("messages"):
            try:
                history = json.loads(existing_dict["messages"])
            except:
                history = []
        else:
            history = []

        if isinstance(conv_ctx, list):
            history.extend(conv_ctx)
        history.append(msg)
        history = history[-20:]  # keep last 20 messages

        # Run AI extraction
        ai = await claude_extract(msg, contact, history, has_image, existing_dict)
        if not isinstance(ai, dict) or not ai:
            ai = {}

        # Resolve fields with confidence
        ai_city = ai.get("city", "unknown")
        city = ai_city if ai_city in ["chennai","hyderabad"] else detect_city_from_message(msg, contact)
        city_conf = ai.get("city_confidence", "uncertain") if city != "unknown" else "uncertain"

        event_date = ai.get("event_date") or parse_date_from_message(msg)
        event_date_conf = ai.get("event_date_confidence", "uncertain") if event_date else "uncertain"

        cake_type = ai.get("cake_type") or parse_cake_type(msg)
        cake_type_conf = ai.get("cake_type_confidence", "uncertain") if cake_type else "uncertain"

        lead_score     = ai.get("lead_score")     or parse_lead_score(msg)
        funnel_stage   = ai.get("funnel_stage")   or parse_funnel_stage(msg)
        conv_prob      = ai.get("conversion_probability", "low")
        conv_status    = ai.get("conversion_status", "open")
        biz_vertical   = ai.get("business_vertical", "")
        order_value    = ai.get("estimated_order_value", "")
        notes          = ai.get("notes", "")
        next_action    = ai.get("next_action", "")
        copilot        = ai.get("copilot_recommendation", "")
        suggested_reply = ai.get("suggested_reply", "")
        drop_detected  = bool(ai.get("drop_detected", False))
        urgency_flag   = bool(ai.get("urgency_flag", False))
        budget_range   = ai.get("budget_range", "")
        budget_conf    = ai.get("budget_confidence", "uncertain")
        weight_kg      = ai.get("weight_kg", "")
        weight_conf    = ai.get("weight_confidence", "uncertain")
        flavour        = ai.get("flavour", "")
        flavour_conf   = ai.get("flavour_confidence", "uncertain")
        occasion_detail = ai.get("occasion_detail", "")

        if has_image and funnel_stage == "enquiry":
            funnel_stage = "ref_shared"

        # Event within 3 days = urgent
        if event_date:
            d = days_until(event_date)
            if d is not None and 0 <= d <= 3:
                urgency_flag = True
                lead_score = "HOT"

        now_iso = datetime.utcnow().isoformat()
        messages_json = json.dumps(history)

        # Detect if this is a new order from a returning customer
        new_order = is_new_order_context(existing_dict, msg, ai)

        if existing_dict and not new_order:
            # UPDATE existing customer profile
            conn.execute("""UPDATE customers SET
                last_message=?, messages=?, message_count=message_count+1,
                cake_type=COALESCE(NULLIF(?,cake_type), cake_type, ?),
                cake_type_confidence=?,
                event_date=COALESCE(NULLIF(?,event_date), event_date, ?),
                event_date_confidence=?,
                budget_range=COALESCE(NULLIF(?,budget_range), budget_range, ?),
                budget_confidence=?,
                weight_kg=COALESCE(NULLIF(?,weight_kg), weight_kg, ?),
                weight_confidence=?,
                flavour=COALESCE(NULLIF(?,flavour), flavour, ?),
                flavour_confidence=?,
                city=CASE WHEN ?='unknown' THEN city ELSE ? END,
                city_confidence=?,
                occasion_detail=COALESCE(NULLIF(?,occasion_detail), occasion_detail, ?),
                lead_score=?, funnel_stage=?, conversion_probability=?,
                conversion_status=CASE WHEN ?='open' THEN conversion_status ELSE ? END,
                business_vertical=COALESCE(NULLIF(?,business_vertical), business_vertical, ?),
                estimated_order_value=COALESCE(NULLIF(?,estimated_order_value), estimated_order_value, ?),
                notes=?, next_action=?, copilot_recommendation=?,
                suggested_reply=?, drop_detected=?, urgency_flag=?,
                has_image=CASE WHEN ?=1 THEN 1 ELSE has_image END,
                phone_number=COALESCE(NULLIF(?,phone_number), phone_number, ?),
                salesperson_email=?, last_updated=?
                WHERE id=?""",
                (msg, messages_json,
                 cake_type, cake_type, cake_type_conf,
                 event_date, event_date, event_date_conf,
                 budget_range, budget_range, budget_conf,
                 weight_kg, weight_kg, weight_conf,
                 flavour, flavour, flavour_conf,
                 city, city, city_conf,
                 occasion_detail, occasion_detail,
                 lead_score, funnel_stage, conv_prob,
                 conv_status, conv_status,
                 biz_vertical, biz_vertical,
                 order_value, order_value,
                 notes, next_action, copilot,
                 suggested_reply, 1 if drop_detected else 0, 1 if urgency_flag else 0,
                 1 if has_image else 0,
                 phone, phone,
                 salesperson, now_iso,
                 existing_dict["id"]))
            customer_id = existing_dict["id"]
        else:
            # INSERT new customer profile (new customer or new order)
            order_num = get_order_number(conn, contact)
            conn.execute("""INSERT INTO customers
                (contact_name, phone_number, order_number, salesperson_email,
                 messages, last_message, message_count,
                 cake_type, cake_type_confidence,
                 event_date, event_date_confidence,
                 budget_range, budget_confidence,
                 weight_kg, weight_confidence,
                 flavour, flavour_confidence,
                 city, city_confidence, occasion_detail,
                 lead_score, funnel_stage, conversion_probability, conversion_status,
                 business_vertical, estimated_order_value,
                 next_action, copilot_recommendation, suggested_reply, notes,
                 drop_detected, urgency_flag, has_image,
                 captured_at, last_updated)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (contact, phone, order_num, salesperson,
                 messages_json, msg, 1,
                 cake_type, cake_type_conf,
                 event_date, event_date_conf,
                 budget_range, budget_conf,
                 weight_kg, weight_conf,
                 flavour, flavour_conf,
                 city, city_conf, occasion_detail,
                 lead_score, funnel_stage, conv_prob, conv_status,
                 biz_vertical, order_value,
                 next_action, copilot, suggested_reply, notes,
                 1 if drop_detected else 0, 1 if urgency_flag else 0, 1 if has_image else 0,
                 now_iso, now_iso))
            customer_id = conn.execute("SELECT last_insert_rowid() as id").fetchone()["id"]

        results.append({"contact": contact, "score": lead_score, "city": city})

        # Alerts
        if lead_score == "HOT" and (not existing_dict or existing_dict.get("lead_score") != "HOT"):
            alerts.append(f"HOT LEAD: {contact} ({city})\nMessage: {msg[:150]}\nNext: {next_action}")
        if drop_detected:
            alerts.append(f"DROPPED LEAD: {contact} ({city})\nMessage: {msg[:150]}")
        if funnel_stage == "confirmed":
            alerts.append(f"CONFIRMED ORDER: {contact} ({city})\nMessage: {msg[:150]}")
        if urgency_flag:
            alerts.append(f"URGENT — EVENT SOON: {contact} ({city})\nEvent: {event_date}\nNext: {next_action}")

    conn.commit()
    conn.close()

    if alerts:
        body_text = "\n\n---\n\n".join(alerts)
        background_tasks.add_task(send_email_alert, f"{len(alerts)} alert(s)", body_text)

    return {"status": "ok", "processed": len(results), "results": results}

# ── DASHBOARD API ──────────────────────────────────────────────────────────────

@app.get("/api/v1/dashboard")
def dashboard(user=Depends(get_current_user), city: str = None):
    conn = get_db()
    today = datetime.today().date()
    next30 = today + timedelta(days=30)
    is_admin = user["role"] == "admin"
    email = user["email"]

    # Resolve assigned cities for this user
    user_row = conn.execute("SELECT city, assigned_cities, role FROM users WHERE email=?", (email,)).fetchone()
    user_assigned_cities = []
    if user_row:
        raw = json.loads(user_row['assigned_cities'] or '[]')
        user_assigned_cities = raw if raw else [user_row['city'] or 'chennai']

    city_filter = ""
    if city and city in CITIES:
        # Explicit city filter from dropdown — respect it for admin; for salesperson only if assigned
        if is_admin or city in user_assigned_cities:
            city_filter = f"AND city='{city}'"
    elif not is_admin and user_assigned_cities:
        # Non-admin: restrict to their assigned cities
        if len(user_assigned_cities) == 1:
            city_filter = f"AND city='{user_assigned_cities[0]}'"
        else:
            cities_in = ','.join(f"'{c}'" for c in user_assigned_cities)
            city_filter = f"AND city IN ({cities_in})"

    where = "" + " " + city_filter

    total     = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE 1=1 {where}").fetchone()["c"]
    confirmed = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE funnel_stage='confirmed' {where}").fetchone()["c"]
    dropped   = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE conversion_status='dropped' {where}").fetchone()["c"]
    hot_count = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE lead_score='HOT' {where}").fetchone()["c"]
    warm_count= conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE lead_score='WARM' {where}").fetchone()["c"]
    urgent_count = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE urgency_flag=1 AND conversion_status='open' {where}").fetchone()["c"]
    unknown_city = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE city='unknown' {where}").fetchone()["c"]

    forecast_low  = int((hot_count * 5500) + (warm_count * 5500 * 0.3))
    forecast_high = int((hot_count * 8000) + (warm_count * 8000 * 0.5))

    city_slots = {}
    for city_key, city_info in CITIES.items():
        used = conn.execute(f"SELECT COUNT(*) as c FROM customers WHERE funnel_stage='confirmed' AND city='{city_key}'").fetchone()["c"]
        city_slots[city_key] = {"name": city_info["name"], "total": city_info["slots"], "used": used, "remaining": city_info["slots"] - used}

    upcoming = conn.execute(f"""
        SELECT id, contact_name, phone_number, event_date, cake_type, lead_score,
               last_message, salesperson_email, funnel_stage, city, occasion_detail,
               budget_range, copilot_recommendation, order_number
        FROM customers WHERE event_date BETWEEN ? AND ? {where}
        ORDER BY event_date ASC
    """, (today.isoformat(), next30.isoformat())).fetchall()

    urgent = conn.execute(f"""
        SELECT id, contact_name, phone_number, last_message, cake_type, lead_score,
               captured_at, salesperson_email, funnel_stage, next_action, city,
               event_date, copilot_recommendation, budget_range, order_number,
               occasion_detail, urgency_flag
        FROM customers WHERE urgency_flag=1 AND conversion_status='open' {where}
        ORDER BY event_date ASC LIMIT 20
    """).fetchall()

    hot = conn.execute(f"""
        SELECT id, contact_name, phone_number, last_message, cake_type, lead_score,
               captured_at, salesperson_email, funnel_stage, next_action, city,
               event_date, copilot_recommendation, suggested_reply, budget_range,
               order_number, occasion_detail, messages, message_count,
               cake_type_confidence, event_date_confidence, budget_confidence, city_confidence
        FROM customers WHERE lead_score='HOT' AND conversion_status='open' {where}
        ORDER BY captured_at DESC LIMIT 30
    """).fetchall()

    recent = conn.execute(f"""
        SELECT id, contact_name, phone_number, last_message, cake_type, lead_score,
               event_date, captured_at, salesperson_email, funnel_stage, follow_up_done,
               assigned_to, notes, next_action, conversion_probability, business_vertical,
               estimated_order_value, suggested_reply, has_image, city, conversion_status,
               copilot_recommendation, budget_range, weight_kg, flavour, occasion_detail,
               order_number, message_count, urgency_flag, drop_detected,
               cake_type_confidence, event_date_confidence, budget_confidence, city_confidence,
               messages, last_updated
        FROM customers WHERE 1=1 {where}
        ORDER BY last_updated DESC LIMIT 100
    """).fetchall()

    four_hours_ago = (datetime.utcnow() - timedelta(hours=4)).isoformat()
    unattended = conn.execute(f"""
        SELECT id, contact_name, phone_number, last_message, salesperson_email,
               captured_at, cake_type, lead_score, next_action, city, event_date,
               copilot_recommendation
        FROM customers
        WHERE follow_up_done=0 AND captured_at < ? AND conversion_status='open' {where}
        ORDER BY captured_at ASC LIMIT 20
    """, (four_hours_ago,)).fetchall()

    funnel = conn.execute(f"""
        SELECT funnel_stage, COUNT(*) as count FROM customers WHERE 1=1 {where} GROUP BY funnel_stage
    """).fetchall()

    score_breakdown = conn.execute(f"""
        SELECT lead_score, COUNT(*) as count FROM customers WHERE 1=1 {where} GROUP BY lead_score
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
            FROM customers GROUP BY salesperson_email
        """).fetchall()

    users = []
    if is_admin:
        users = conn.execute("SELECT id, email, name, role, city FROM users").fetchall()

    conn.close()
    return {
        "role": user["role"], "email": email,
        "total_customers": total, "confirmed_orders": confirmed,
        "dropped_leads": dropped, "hot_count": hot_count,
        "warm_count": warm_count, "urgent_count": urgent_count,
        "unknown_city": unknown_city,
        "forecast_low": forecast_low, "forecast_high": forecast_high,
        "city_slots": city_slots,
        "upcoming_events":   [dict(r) for r in upcoming],
        "urgent_leads":      [dict(r) for r in urgent],
        "hot_leads":         [dict(r) for r in hot],
        "recent_leads":      [dict(r) for r in recent],
        "unattended_leads":  [dict(r) for r in unattended],
        "funnel":            [dict(r) for r in funnel],
        "score_breakdown":   [dict(r) for r in score_breakdown],
        "salesperson_stats": [dict(r) for r in sp_stats],
        "users":             [dict(r) for r in users]
    }

# ── CUSTOMER ACTIONS ───────────────────────────────────────────────────────────

@app.get("/api/v1/customers/{customer_id}")
def get_customer(customer_id: int, user=Depends(get_current_user)):
    conn = get_db()
    c = conn.execute("SELECT * FROM customers WHERE id=?", (customer_id,)).fetchone()
    conn.close()
    if not c: raise HTTPException(status_code=404)
    return dict(c)

@app.post("/api/v1/leads/{lead_id}/followup")
def mark_followup(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE customers SET follow_up_done=1, follow_up_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.execute("INSERT INTO follow_ups (customer_id,done_by,done_at) VALUES (?,?,?)",
        (lead_id, user["email"], datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/stage")
def update_stage(lead_id: int, body: dict, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE customers SET funnel_stage=?, last_updated=? WHERE id=?",
        (body.get("stage"), datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/city")
def update_city(lead_id: int, body: dict, user=Depends(get_current_user)):
    city = body.get("city", "")
    if city not in ["chennai", "hyderabad"]:
        raise HTTPException(status_code=400, detail="Invalid city")
    conn = get_db()
    conn.execute("UPDATE customers SET city=?, city_confidence='confirmed', last_updated=? WHERE id=?",
        (city, datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/convert")
def mark_converted(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE customers SET conversion_status='converted', funnel_stage='confirmed', last_updated=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/drop")
def mark_dropped(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE customers SET conversion_status='dropped', last_updated=? WHERE id=?",
        (datetime.utcnow().isoformat(), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/v1/leads/{lead_id}/assign")
def assign_lead(lead_id: int, body: dict, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    conn.execute("UPDATE customers SET assigned_to=? WHERE id=?", (body.get("email"), lead_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.delete("/api/v1/leads/{lead_id}")
def delete_lead(lead_id: int, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    conn.execute("DELETE FROM customers WHERE id=?", (lead_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

# ── B2B OUTBOUND TRACKER ───────────────────────────────────────────────────────

class B2BProspect(BaseModel):
    company_name: str
    contact_person: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    industry: Optional[str] = None
    city: str = "chennai"
    potential_value: Optional[str] = None
    notes: Optional[str] = None
    assigned_to: Optional[str] = None

@app.get("/api/v1/b2b")
def list_b2b(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM b2b_prospects ORDER BY updated_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/v1/b2b")
def create_b2b(prospect: B2BProspect, user=Depends(get_current_user)):
    conn = get_db()
    now = datetime.utcnow().isoformat()
    conn.execute("""INSERT INTO b2b_prospects
        (company_name, contact_person, phone, email, industry, city,
         potential_value, notes, assigned_to, status, created_by, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (prospect.company_name, prospect.contact_person, prospect.phone,
         prospect.email, prospect.industry, prospect.city,
         prospect.potential_value, prospect.notes,
         prospect.assigned_to or user["email"],
         "not_contacted", user["email"], now, now))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.put("/api/v1/b2b/{prospect_id}")
def update_b2b(prospect_id: int, body: dict, user=Depends(get_current_user)):
    conn = get_db()
    now = datetime.utcnow().isoformat()
    allowed = ["status","notes","next_followup_at","contact_person","phone","email",
               "potential_value","assigned_to","last_contact_at"]
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        conn.close()
        return {"status": "no changes"}
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [now, prospect_id]
    conn.execute(f"UPDATE b2b_prospects SET {set_clause}, updated_at=? WHERE id=?", values)
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.delete("/api/v1/b2b/{prospect_id}")
def delete_b2b(prospect_id: int, user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(status_code=403)
    conn = get_db()
    conn.execute("DELETE FROM b2b_prospects WHERE id=?", (prospect_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

# ── INTELLIGENCE ───────────────────────────────────────────────────────────────

@app.get("/api/v1/intelligence")
def intelligence(user=Depends(get_current_user)):
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM customers").fetchone()["c"]
    converted = conn.execute("SELECT COUNT(*) as c FROM customers WHERE conversion_status='converted'").fetchone()["c"]
    dropped = conn.execute("SELECT COUNT(*) as c FROM customers WHERE conversion_status='dropped'").fetchone()["c"]
    conv_rate = round((converted / total * 100), 1) if total > 0 else 0

    top_cakes = conn.execute("""
        SELECT cake_type, COUNT(*) as total,
               SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END) as converted
        FROM customers WHERE cake_type IS NOT NULL
        GROUP BY cake_type ORDER BY converted DESC LIMIT 5
    """).fetchall()

    two_hours_ago = (datetime.utcnow() - timedelta(hours=2)).isoformat()
    at_risk = conn.execute("""
        SELECT contact_name, phone_number, city, captured_at, next_action, copilot_recommendation, event_date
        FROM customers
        WHERE lead_score='HOT' AND follow_up_done=0 AND captured_at < ?
        AND conversion_status='open'
        ORDER BY captured_at ASC LIMIT 10
    """, (two_hours_ago,)).fetchall()

    city_perf = conn.execute("""
        SELECT city, COUNT(*) as total,
               SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END) as converted,
               SUM(CASE WHEN lead_score='HOT' THEN 1 ELSE 0 END) as hot
        FROM customers GROUP BY city
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
        SELECT id, contact_name, phone_number, last_message, lead_score,
               funnel_stage, salesperson_email, last_updated, suggested_reply, city, has_image
        FROM customers ORDER BY last_updated DESC LIMIT 20
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/health")
def health():
    conn = get_db()
    leads = conn.execute("SELECT COUNT(*) as c FROM customers").fetchone()["c"]
    conn.close()
    return {
        "status": "ok",
        "version": "3-layer-ai-v1",
        "time": datetime.utcnow().isoformat(),
        "leads": leads,
        "ai_layers": {
            "layer1_claude": bool(ANTHROPIC_KEY and ANTHROPIC_AVAILABLE),
            "layer2_openai": bool(OPENAI_KEY),
            "layer3_gemini": bool(GEMINI_KEY)
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

@app.get("/debug/ai-test")
async def debug_ai_test():
    """Debug endpoint to test AI extraction directly on Railway"""
    result = await claude_extract(
        message="I need a 2kg chocolate truffle birthday cake for 28th March, budget 2500, Chennai Anna Nagar",
        contact_name="Debug Test",
        conversation_history=[],
        has_image=False,
        existing_customer=None
    )
    return {
        "ai_result": result,
        "layer1_claude": bool(ANTHROPIC_KEY and ANTHROPIC_AVAILABLE),
        "layer2_openai": bool(OPENAI_KEY),
        "layer3_gemini": bool(GEMINI_KEY),
        "openai_base_url": OPENAI_BASE_URL or "default"
    }

@app.get("/debug/ai-error")
async def debug_ai_error():
    """Debug endpoint to capture the actual AI error"""
    import traceback
    try:
        from openai import OpenAI as _OpenAI
        client_kwargs = {"api_key": OPENAI_KEY}
        if OPENAI_BASE_URL:
            client_kwargs["base_url"] = OPENAI_BASE_URL
        client = _OpenAI(**client_kwargs)
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            max_tokens=100,
            messages=[{"role": "user", "content": "Reply with JSON: {\"test\": \"ok\"}"}]
        )
        return {"success": True, "response": response.choices[0].message.content}
    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}

@app.post("/admin/reprocess-all")
async def reprocess_all_leads(background_tasks: BackgroundTasks, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Re-run AI extraction on ALL existing leads to fill in empty fields. Admin only."""
    if not credentials:
        raise HTTPException(status_code=401)
    conn = get_db()
    session = conn.execute("SELECT * FROM sessions WHERE token=?", (credentials.credentials,)).fetchone()
    conn.close()
    if not session or session["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    background_tasks.add_task(_reprocess_all_background)
    return {"status": "started", "message": "Reprocessing all leads in background. Check /admin/reprocess-status for progress."}

_reprocess_status = {"total": 0, "done": 0, "errors": 0, "running": False, "started_at": None, "finished_at": None}

async def _reprocess_all_background():
    global _reprocess_status
    conn = get_db()
    leads = conn.execute("SELECT * FROM customers ORDER BY id ASC").fetchall()
    conn.close()

    _reprocess_status = {
        "total": len(leads), "done": 0, "errors": 0,
        "running": True, "started_at": datetime.utcnow().isoformat(), "finished_at": None
    }

    for lead in leads:
        try:
            lead = dict(lead)
            messages_raw = lead.get("messages", "[]") or "[]"
            try:
                msgs = json.loads(messages_raw)
            except:
                msgs = []

            # Build combined message text from all stored messages
            all_text = lead.get("last_message", "") or ""
            if msgs:
                all_text = " | ".join(str(m) for m in msgs[-20:])

            if not all_text.strip():
                _reprocess_status["done"] += 1
                continue

            ai = await claude_extract(
                message=all_text,
                contact_name=lead.get("contact_name", ""),
                conversation_history=msgs,
                has_image=bool(lead.get("has_image")),
                existing_customer=lead
            )

            if not ai:
                _reprocess_status["errors"] += 1
                _reprocess_status["done"] += 1
                continue

            # Update all AI fields — only overwrite if currently empty/unknown
            conn2 = get_db()
            conn2.execute("""UPDATE customers SET
                cake_type = COALESCE(NULLIF(cake_type,'unknown'), ?),
                cake_type_confidence = COALESCE(NULLIF(cake_type_confidence,'uncertain'), ?),
                event_date = COALESCE(NULLIF(event_date,''), ?),
                event_date_confidence = COALESCE(NULLIF(event_date_confidence,'uncertain'), ?),
                budget_range = COALESCE(NULLIF(budget_range,''), ?),
                budget_confidence = COALESCE(NULLIF(budget_confidence,'uncertain'), ?),
                weight_kg = COALESCE(NULLIF(weight_kg,''), ?),
                weight_confidence = COALESCE(NULLIF(weight_confidence,'uncertain'), ?),
                flavour = COALESCE(NULLIF(flavour,''), ?),
                flavour_confidence = COALESCE(NULLIF(flavour_confidence,'uncertain'), ?),
                city = COALESCE(NULLIF(city,'unknown'), ?),
                city_confidence = COALESCE(NULLIF(city_confidence,'uncertain'), ?),
                occasion_detail = COALESCE(NULLIF(occasion_detail,''), ?),
                lead_score = COALESCE(NULLIF(lead_score,'WARM'), ?),
                funnel_stage = COALESCE(NULLIF(funnel_stage,'enquiry'), ?),
                conversion_status = COALESCE(NULLIF(conversion_status,'open'), ?),
                conversion_probability = COALESCE(NULLIF(conversion_probability,'low'), ?),
                business_vertical = COALESCE(NULLIF(business_vertical,'unknown'), ?),
                estimated_order_value = COALESCE(NULLIF(estimated_order_value,'unknown'), ?),
                urgency_flag = CASE WHEN urgency_flag=0 THEN ? ELSE urgency_flag END,
                drop_detected = CASE WHEN drop_detected=0 THEN ? ELSE drop_detected END,
                next_action = COALESCE(NULLIF(next_action,''), ?),
                copilot_recommendation = COALESCE(NULLIF(copilot_recommendation,''), ?),
                notes = COALESCE(NULLIF(notes,''), ?),
                suggested_reply = COALESCE(NULLIF(suggested_reply,''), ?),
                last_updated = ?
                WHERE id = ?""",
                (
                    ai.get("cake_type"), ai.get("cake_type_confidence"),
                    ai.get("event_date"), ai.get("event_date_confidence"),
                    ai.get("budget_range"), ai.get("budget_confidence"),
                    ai.get("weight_kg"), ai.get("weight_confidence"),
                    ai.get("flavour"), ai.get("flavour_confidence"),
                    ai.get("city"), ai.get("city_confidence"),
                    ai.get("occasion_detail"),
                    ai.get("lead_score"),
                    ai.get("funnel_stage"),
                    ai.get("conversion_status"),
                    ai.get("conversion_probability"),
                    ai.get("business_vertical"),
                    ai.get("estimated_order_value"),
                    1 if ai.get("urgency_flag") else 0,
                    1 if ai.get("drop_detected") else 0,
                    ai.get("next_action"),
                    ai.get("copilot_recommendation"),
                    ai.get("notes"),
                    ai.get("suggested_reply"),
                    datetime.utcnow().isoformat(),
                    lead["id"]
                )
            )
            conn2.commit()
            conn2.close()
            _reprocess_status["done"] += 1
            print(f"Reprocessed lead {lead['id']} ({lead.get('contact_name','?')})")

        except Exception as e:
            print(f"Error reprocessing lead {lead.get('id')}: {e}")
            _reprocess_status["errors"] += 1
            _reprocess_status["done"] += 1

    _reprocess_status["running"] = False
    _reprocess_status["finished_at"] = datetime.utcnow().isoformat()
    print(f"Reprocess complete: {_reprocess_status['done']} done, {_reprocess_status['errors']} errors")

@app.get("/admin/reprocess-status")
async def reprocess_status():
    """Check the status of the background reprocessing job."""
    return _reprocess_status

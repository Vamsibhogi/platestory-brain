"""
Platestory Central Brain — FastAPI Backend

Receives extracted WhatsApp data from all device agents.
Stores everything. Finds patterns. Manages follow-up schedules.
Talks to Kommo CRM.

Run:
    pip install fastapi uvicorn sqlmodel anthropic httpx python-dotenv
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Deploy:
    Any VPS (DigitalOcean, Railway, Render) — needs ~512MB RAM.
    Cost: ~$5-10/month.
"""

import os
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

import httpx
import anthropic
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlmodel import Field as SQLField, Session, SQLModel, create_engine, select
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///platestory_brain.db")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
KOMMO_API_URL = os.getenv("KOMMO_API_URL")          # e.g. https://yourcompany.kommo.com
KOMMO_ACCESS_TOKEN = os.getenv("KOMMO_ACCESS_TOKEN")
AGENT_SECRET = os.getenv("AGENT_SECRET", "change-this-in-production")

# ── Database Models ─────────────────────────────────────────────────────────────

class CustomerExtraction(SQLModel, table=True):
    """Every message extraction from every device agent lives here."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    device_id: str
    salesperson_id: str
    contact_name: str
    message: str
    event_date: Optional[datetime] = None
    cake_type: Optional[str] = None
    budget_inr: Optional[int] = None
    lead_score: str  # HOT | WARM | COLD
    suggested_action: Optional[str] = None
    key_info: Optional[str] = None
    extracted_by: str  # local | ai
    captured_at: datetime = SQLField(default_factory=datetime.utcnow)

class FollowUpTask(SQLModel, table=True):
    """Scheduled follow-up reminders, managed by the brain."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    contact_name: str
    salesperson_id: str
    event_date: datetime
    cake_type: Optional[str] = None
    follow_up_at: datetime  # When to remind
    urgency: str            # CRITICAL | HIGH | MEDIUM | NORMAL
    completed: bool = False
    kommo_deal_id: Optional[str] = None
    created_at: datetime = SQLField(default_factory=datetime.utcnow)

class CustomerProfile(SQLModel, table=True):
    """Aggregated profile per customer — built automatically from extractions."""
    id: Optional[int] = SQLField(default=None, primary_key=True)
    contact_name: str = SQLField(index=True, unique=True)
    total_interactions: int = 0
    last_seen: Optional[datetime] = None
    next_event_date: Optional[datetime] = None
    cake_types_ordered: str = ""       # CSV: "wedding,birthday"
    total_budget_inr: int = 0
    lead_score: str = "WARM"
    assigned_salesperson: Optional[str] = None
    kommo_contact_id: Optional[str] = None
    created_at: datetime = SQLField(default_factory=datetime.utcnow)

# ── DB Setup ────────────────────────────────────────────────────────────────────

engine = create_engine(DATABASE_URL)

def get_session():
    with Session(engine) as session:
        yield session

@asynccontextmanager
async def lifespan(app: FastAPI):
    SQLModel.metadata.create_all(engine)
    print("✅ Platestory Brain started. DB ready.")
    yield

# ── App ─────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Platestory Brain", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth ────────────────────────────────────────────────────────────────────────

def verify_agent(authorization: str = Header(...)):
    token = authorization.replace("Bearer ", "")
    if token != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Invalid agent token")
    return token

# ── API Models ──────────────────────────────────────────────────────────────────

class ExtractionItem(BaseModel):
    contact_name: str
    message: str
    event_date: Optional[str] = None   # ISO 8601
    cake_type: Optional[str] = None
    budget_inr: Optional[int] = None
    lead_score: str = "WARM"
    suggested_action: Optional[str] = None
    key_info: Optional[str] = None
    extracted_by: str = "local"
    captured_at: Optional[str] = None

class BatchUpload(BaseModel):
    device_id: str
    salesperson_id: str
    batch_size: int
    extractions: List[ExtractionItem]

class AIExtractRequest(BaseModel):
    prompt: str
    contact_name: str

# ── Routes ───────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "alive", "service": "platestory-brain"}


@app.post("/api/v1/extractions")
async def receive_extractions(
    batch: BatchUpload,
    session: Session = Depends(get_session),
    _auth = Depends(verify_agent)
):
    """
    Main endpoint — receives batches from all device agents.
    Stores everything, updates customer profiles, schedules follow-ups.
    """
    stored = 0

    for item in batch.extractions:
        # Store the raw extraction
        extraction = CustomerExtraction(
            device_id=batch.device_id,
            salesperson_id=batch.salesperson_id,
            contact_name=item.contact_name,
            message=item.message,
            event_date=datetime.fromisoformat(item.event_date) if item.event_date else None,
            cake_type=item.cake_type,
            budget_inr=item.budget_inr,
            lead_score=item.lead_score,
            suggested_action=item.suggested_action,
            key_info=item.key_info,
            extracted_by=item.extracted_by,
        )
        session.add(extraction)

        # Update or create customer profile
        await upsert_customer_profile(session, batch.salesperson_id, item)

        # Schedule follow-up if event date exists
        if item.event_date:
            event_dt = datetime.fromisoformat(item.event_date)
            schedule_follow_up(session, item, batch.salesperson_id, event_dt)

        stored += 1

    session.commit()

    # Async: sync hot leads to Kommo CRM
    hot_leads = [i for i in batch.extractions if i.lead_score == "HOT"]
    if hot_leads:
        asyncio.create_task(sync_to_kommo(hot_leads, batch.salesperson_id))

    return {"stored": stored, "status": "ok"}


async def upsert_customer_profile(session: Session, salesperson_id: str, item: ExtractionItem):
    profile = session.exec(
        select(CustomerProfile).where(CustomerProfile.contact_name == item.contact_name)
    ).first()

    if not profile:
        profile = CustomerProfile(
            contact_name=item.contact_name,
            assigned_salesperson=salesperson_id,
        )

    profile.total_interactions += 1
    profile.last_seen = datetime.utcnow()
    profile.lead_score = item.lead_score

    if item.event_date:
        profile.next_event_date = datetime.fromisoformat(item.event_date)

    if item.cake_type and item.cake_type not in profile.cake_types_ordered:
        existing = profile.cake_types_ordered.split(",") if profile.cake_types_ordered else []
        existing.append(item.cake_type)
        profile.cake_types_ordered = ",".join(filter(None, existing))

    if item.budget_inr:
        profile.total_budget_inr = max(profile.total_budget_inr, item.budget_inr)

    session.add(profile)


def schedule_follow_up(session: Session, item: ExtractionItem, salesperson_id: str, event_date: datetime):
    now = datetime.utcnow()
    days_to_event = (event_date - now).days

    if days_to_event < 0:
        return  # Past event, skip

    # Determine follow-up timing
    if days_to_event > 30:
        follow_up_in_days = 7
        urgency = "NORMAL"
    elif days_to_event > 14:
        follow_up_in_days = 3
        urgency = "MEDIUM"
    elif days_to_event > 7:
        follow_up_in_days = 1
        urgency = "HIGH"
    else:
        follow_up_in_days = 0
        urgency = "CRITICAL"

    follow_up_at = now + timedelta(days=follow_up_in_days)
    follow_up_at = follow_up_at.replace(hour=10, minute=0, second=0)  # 10am

    # Don't duplicate if task already exists for this contact
    existing = session.exec(
        select(FollowUpTask).where(
            FollowUpTask.contact_name == item.contact_name,
            FollowUpTask.completed == False
        )
    ).first()

    if existing:
        # Update urgency if it escalated
        if urgency in ["CRITICAL", "HIGH"] and existing.urgency == "NORMAL":
            existing.urgency = urgency
            existing.follow_up_at = follow_up_at
            session.add(existing)
        return

    task = FollowUpTask(
        contact_name=item.contact_name,
        salesperson_id=salesperson_id,
        event_date=event_date,
        cake_type=item.cake_type,
        follow_up_at=follow_up_at,
        urgency=urgency,
    )
    session.add(task)


async def sync_to_kommo(hot_leads: List[ExtractionItem], salesperson_id: str):
    """
    Pushes hot leads to Kommo CRM automatically.
    Creates or updates deals with extracted data.
    """
    if not KOMMO_API_URL or not KOMMO_ACCESS_TOKEN:
        return  # Kommo not configured yet

    async with httpx.AsyncClient() as client:
        for lead in hot_leads:
            try:
                # Check if contact exists
                search = await client.get(
                    f"{KOMMO_API_URL}/api/v4/contacts",
                    headers={"Authorization": f"Bearer {KOMMO_ACCESS_TOKEN}"},
                    params={"query": lead.contact_name},
                )
                data = search.json()
                contact_id = None

                if data.get("_embedded", {}).get("contacts"):
                    contact_id = data["_embedded"]["contacts"][0]["id"]

                # Create note on the contact with extracted info
                if contact_id:
                    note_text = f"🤖 Agent Update:\n"
                    if lead.event_date:
                        note_text += f"📅 Event: {lead.event_date}\n"
                    if lead.cake_type:
                        note_text += f"🎂 Type: {lead.cake_type}\n"
                    if lead.budget_inr:
                        note_text += f"💰 Budget: ₹{lead.budget_inr:,}\n"
                    if lead.key_info:
                        note_text += f"📝 {lead.key_info}\n"
                    note_text += f"🔥 Lead Score: {lead.lead_score}"

                    await client.post(
                        f"{KOMMO_API_URL}/api/v4/contacts/{contact_id}/notes",
                        headers={
                            "Authorization": f"Bearer {KOMMO_ACCESS_TOKEN}",
                            "Content-Type": "application/json"
                        },
                        json=[{"note_type": "common", "params": {"text": note_text}}]
                    )
            except Exception as e:
                print(f"Kommo sync error for {lead.contact_name}: {e}")


@app.post("/ai/extract")
async def ai_extract(
    request: AIExtractRequest,
    _auth = Depends(verify_agent)
):
    """
    Claude-powered extraction for ambiguous natural language dates.
    Called by device agent when local regex misses a date hint.
    """
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="AI extraction not configured")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": request.prompt}]
    )

    response_text = message.content[0].text
    return {"result": response_text}


@app.get("/api/v1/dashboard")
async def dashboard(
    session: Session = Depends(get_session),
    _auth = Depends(verify_agent)
):
    """Returns a snapshot of today's sales intelligence."""
    now = datetime.utcnow()

    # Urgent follow-ups due today or overdue
    urgent = session.exec(
        select(FollowUpTask).where(
            FollowUpTask.follow_up_at <= now + timedelta(hours=24),
            FollowUpTask.completed == False
        ).order_by(FollowUpTask.urgency)
    ).all()

    # Hot leads from last 24h
    hot_leads = session.exec(
        select(CustomerProfile).where(
            CustomerProfile.lead_score == "HOT",
            CustomerProfile.last_seen >= now - timedelta(hours=24)
        )
    ).all()

    # Events in next 7 days
    upcoming_events = session.exec(
        select(CustomerProfile).where(
            CustomerProfile.next_event_date >= now,
            CustomerProfile.next_event_date <= now + timedelta(days=7)
        ).order_by(CustomerProfile.next_event_date)
    ).all()

    return {
        "urgent_follow_ups": len(urgent),
        "hot_leads_today": len(hot_leads),
        "events_this_week": len(upcoming_events),
        "follow_ups": [
            {
                "contact": t.contact_name,
                "event_date": t.event_date.isoformat(),
                "urgency": t.urgency,
                "days_to_event": (t.event_date - now).days
            } for t in urgent
        ],
        "hot_leads": [
            {
                "contact": l.contact_name,
                "cake_type": l.cake_types_ordered,
                "budget": l.total_budget_inr,
                "salesperson": l.assigned_salesperson
            } for l in hot_leads
        ],
    }


@app.get("/api/v1/patterns")
async def patterns(session: Session = Depends(get_session), _auth = Depends(verify_agent)):
    """What's working? Patterns across all conversations."""
    all_profiles = session.exec(select(CustomerProfile)).all()

    cake_counts: dict = {}
    for p in all_profiles:
        for cake in p.cake_types_ordered.split(","):
            if cake:
                cake_counts[cake] = cake_counts.get(cake, 0) + 1

    hot_rate = len([p for p in all_profiles if p.lead_score == "HOT"]) / max(len(all_profiles), 1)

    return {
        "total_customers": len(all_profiles),
        "hot_lead_rate": round(hot_rate * 100, 1),
        "cake_type_breakdown": sorted(cake_counts.items(), key=lambda x: -x[1]),
        "avg_budget": sum(p.total_budget_inr for p in all_profiles) / max(len(all_profiles), 1),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

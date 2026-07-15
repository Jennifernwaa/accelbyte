from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DB_PATH = "webhooks.db"
DELIVERY_WORKER_STOP = threading.Event()
DELIVERY_WORKER = None
WORKER_ENABLED = os.environ.get("WEBHOOK_WORKER_ENABLED", "1") != "0"


def run_delivery_loop() -> None:
    while not DELIVERY_WORKER_STOP.wait(1.0):
        process_due_deliveries()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    global DELIVERY_WORKER
    DELIVERY_WORKER_STOP.clear()
    if WORKER_ENABLED:
        DELIVERY_WORKER = threading.Thread(target=run_delivery_loop, daemon=True)
        DELIVERY_WORKER.start()
    try:
        yield
    finally:
        DELIVERY_WORKER_STOP.set()
        if DELIVERY_WORKER is not None:
            DELIVERY_WORKER.join(timeout=2.0)


app = FastAPI(title="Webhook Delivery Service", lifespan=lifespan)


class CustomerEndpointIn(BaseModel):
    endpoint_url: str


class CustomerEndpointOut(BaseModel):
    customer_id: str
    endpoint_url: str
    is_active: bool = True


class WebhookEventIn(BaseModel):
    customer_id: str
    event_type: str
    payload: dict[str, Any]
    endpoint_url: Optional[str] = None
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))


class WebhookEventOut(BaseModel):
    event_id: str
    status: str
    customer_id: str
    endpoint_url: str


class EventStatusOut(BaseModel):
    event_id: str
    status: str
    attempts: int
    last_error: Optional[str] = None
    endpoint_url: Optional[str] = None


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS customer_endpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT NOT NULL,
                endpoint_url TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(customer_id, endpoint_url)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                customer_id TEXT NOT NULL,
                endpoint_url TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.commit()


@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def register_endpoint(customer_id: str, endpoint_url: str) -> CustomerEndpointOut:
    now = time.time()
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO customer_endpoints (customer_id, endpoint_url, is_active, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(customer_id, endpoint_url) DO UPDATE SET
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (customer_id, endpoint_url, now, now),
        )
        conn.commit()
    return CustomerEndpointOut(customer_id=customer_id, endpoint_url=endpoint_url, is_active=True)


def get_customer_endpoint(customer_id: str) -> str | None:
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT endpoint_url FROM customer_endpoints WHERE customer_id = ? AND is_active = 1 ORDER BY updated_at DESC LIMIT 1",
            (customer_id,),
        ).fetchone()
    return row["endpoint_url"] if row else None


def enqueue_event(customer_id: str, endpoint_url: str | None, event_type: str, event_id: str, payload: dict[str, Any]) -> str:
    now = time.time()
    resolved_endpoint = endpoint_url or get_customer_endpoint(customer_id)
    if not resolved_endpoint:
        raise HTTPException(status_code=404, detail="No registered endpoint found for customer")

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO webhook_events (
                event_id, customer_id, endpoint_url, event_type, payload_json,
                status, attempts, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
            """,
            (
                event_id,
                customer_id,
                resolved_endpoint,
                event_type,
                json.dumps(payload),
                now,
                now,
                now,
            ),
        )
        conn.commit()
    return event_id


def compute_next_attempt(attempts: int) -> float:
    base_delay = 1.0
    cap = 60.0
    retry_delay = min(base_delay * (2 ** max(attempts, 0)), cap)
    return retry_delay + random.uniform(0, 1.0)


def process_due_deliveries() -> None:
    now = time.time()
    MAX_ATTEMPTS = 6
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, event_id, endpoint_url, payload_json, attempts, status
            FROM webhook_events
            WHERE status IN ('pending', 'retrying')
              AND next_attempt_at <= ?
            ORDER BY next_attempt_at ASC
            LIMIT 50
            """,
            (now,),
        ).fetchall()

        for row in rows:
            attempts = row["attempts"] + 1
            endpoint_url = row["endpoint_url"]
            payload = json.loads(row["payload_json"])
            try:
                with httpx.Client(timeout=10.0) as client:
                    response = client.post(endpoint_url, json=payload)
                if 200 <= response.status_code < 300:
                    conn.execute(
                        "UPDATE webhook_events SET status = 'delivered', attempts = ?, updated_at = ?, last_error = NULL WHERE id = ?",
                        (attempts, now, row["id"]),
                    )
                elif response.status_code in {400, 401, 403, 404, 410}:
                    conn.execute(
                        "UPDATE webhook_events SET status = 'failed', attempts = ?, updated_at = ?, last_error = ? WHERE id = ?",
                        (attempts, now, f"permanent client error: {response.status_code}", row["id"]),
                    )
                elif attempts >= MAX_ATTEMPTS:
                    conn.execute(
                        "UPDATE webhook_events SET status = 'failed', attempts = ?, updated_at = ?, last_error = ? WHERE id = ?",
                        (attempts, now, f"retry budget exhausted: {response.status_code}", row["id"]),
                    )
                else:
                    delay = compute_next_attempt(attempts)
                    conn.execute(
                        "UPDATE webhook_events SET status = 'retrying', attempts = ?, next_attempt_at = ?, updated_at = ?, last_error = ? WHERE id = ?",
                        (attempts, now + delay, now, f"retryable http status: {response.status_code}", row["id"]),
                    )
            except httpx.HTTPError as exc:
                if attempts >= MAX_ATTEMPTS:
                    conn.execute(
                        "UPDATE webhook_events SET status = 'failed', attempts = ?, updated_at = ?, last_error = ? WHERE id = ?",
                        (attempts, now, str(exc), row["id"]),
                    )
                else:
                    delay = compute_next_attempt(attempts)
                    conn.execute(
                        "UPDATE webhook_events SET status = 'retrying', attempts = ?, next_attempt_at = ?, updated_at = ?, last_error = ? WHERE id = ?",
                        (attempts, now + delay, now, str(exc), row["id"]),
                    )
        conn.commit()


@app.post("/customers/{customer_id}/endpoints", status_code=201)
def register_customer_endpoint(customer_id: str, body: CustomerEndpointIn) -> CustomerEndpointOut:
    return register_endpoint(customer_id=customer_id, endpoint_url=body.endpoint_url)


@app.post("/webhooks/events", status_code=202)
def ingest_event(event: WebhookEventIn) -> WebhookEventOut:
    event_id = enqueue_event(
        customer_id=event.customer_id,
        endpoint_url=event.endpoint_url,
        event_type=event.event_type,
        event_id=event.event_id,
        payload=event.payload,
    )
    resolved_endpoint = event.endpoint_url or get_customer_endpoint(event.customer_id)
    if not resolved_endpoint:
        raise HTTPException(status_code=404, detail="No registered endpoint found for customer")
    return WebhookEventOut(
        event_id=event_id,
        status="pending",
        customer_id=event.customer_id,
        endpoint_url=resolved_endpoint,
    )


@app.get("/webhooks/events/{event_id}", response_model=EventStatusOut)
def get_event_status(event_id: str) -> EventStatusOut:
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT event_id, status, attempts, last_error, endpoint_url FROM webhook_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")
    return EventStatusOut(
        event_id=row["event_id"],
        status=row["status"],
        attempts=row["attempts"],
        last_error=row["last_error"],
        endpoint_url=row["endpoint_url"],
    )


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "webhook-delivery-service", "status": "ok"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

import json
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app, DB_PATH, enqueue_event, get_db_connection, process_due_deliveries


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def clean_db(tmp_path):
    db_path = tmp_path / "webhooks.db"
    orig = DB_PATH
    import app.main as main
    main.DB_PATH = str(db_path)
    main.init_db()
    try:
        yield db_path
    finally:
        main.DB_PATH = orig


class RetryServer(BaseHTTPRequestHandler):
    attempts = 0

    def do_POST(self):
        RetryServer.attempts += 1
        if RetryServer.attempts < 3:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b"temporarily unavailable")
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):
        return


def test_register_endpoint_and_ingest_event_persists_and_returns_accepted(client, clean_db):
    endpoint_payload = {
        "endpoint_url": "https://example.com/webhook",
    }
    register_response = client.post("/customers/cust-1/endpoints", json=endpoint_payload)
    assert register_response.status_code == 201

    payload = {
        "customer_id": "cust-1",
        "event_type": "user.created",
        "event_id": "evt-001",
        "payload": {"id": 1, "name": "Ada"},
    }

    response = client.post("/webhooks/events", json=payload)

    assert response.status_code == 202
    conn = sqlite3.connect(clean_db)
    row = conn.execute(
        "SELECT event_id, customer_id, status, attempts, endpoint_url FROM webhook_events WHERE event_id = ?",
        ("evt-001",),
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "evt-001"
    assert row[1] == "cust-1"
    assert row[2] == "pending"
    assert row[3] == 0
    assert row[4] == "https://example.com/webhook"


def test_event_status_lookup_and_retry_worker_eventually_succeeds(client, clean_db):
    server = HTTPServer(("127.0.0.1", 0), RetryServer)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    event_id = "evt-retry-1"
    enqueue_event(
        customer_id="cust-1",
        endpoint_url=f"http://127.0.0.1:{port}/hook",
        event_type="user.created",
        event_id=event_id,
        payload={"hello": "world"},
    )

    deadline = time.time() + 10
    while time.time() < deadline:
        process_due_deliveries()
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT status, attempts FROM webhook_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        if row and row[0] == "delivered":
            break
        time.sleep(0.1)

    status_response = client.get(f"/webhooks/events/{event_id}")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["event_id"] == event_id

    server.shutdown()
    server.server_close()

    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT status, attempts FROM webhook_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()

    assert row is not None
    assert row[0] == "delivered"
    assert row[1] >= 2

"""
Datadog Pager Bridge Server

Bridges Datadog On-Call webhooks ↔ MQTT ↔ Physical pager device.
Multi-tenant: each pager stores its own DD API keys in SQLite.

Routes:
  POST /webhook             — Receives DD On-Call webhook (legacy, single device)
  POST /webhook/{device_id} — Receives DD On-Call webhook for a specific pager
  POST /test-alert          — Sends a fake test alert to the pager
  GET  /setup               — Setup page with API key entry + test alert
  POST /connect             — Saves DD API keys for a device
  GET  /health              — Health check

NOTE: For production, replace the API key entry flow with Datadog OAuth2.
  This requires registering an OAuth App via the Datadog Developer Platform
  (Technology Partner access needed). The multi-tenant architecture here
  (per-device SQLite storage, per-device webhooks, wildcard MQTT subscriptions)
  is designed to support OAuth — swap the /connect form POST for an OAuth2
  authorization code flow with PKCE. See:
  https://docs.datadoghq.com/developers/authorization/oauth2_in_datadog/
"""

import json
import logging
import os
import sqlite3
import ssl
import time
import uuid
from contextlib import asynccontextmanager

import httpx
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bridge")

# --- Config from environment ---
MQTT_BROKER = os.environ["MQTT_BROKER"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASS = os.environ["MQTT_PASS"]

# Legacy API key auth (backwards compat — used as fallback if no per-device keys)
DD_API_KEY = os.environ.get("DD_API_KEY", "")
DD_APP_KEY = os.environ.get("DD_APP_KEY", "")
DD_SITE = os.environ.get("DD_SITE", "datadoghq.com")

BRIDGE_URL = os.environ.get("BRIDGE_URL", "https://dd-pager-bridge.onrender.com")
DEVICE_ID = os.environ.get("DEVICE_ID", "ddpager-poc-001")

# --- SQLite database for device credentials ---
DB_PATH = os.environ.get("DB_PATH", "./pager_bridge.db")


def init_db():
    """Create the devices table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            api_key TEXT,
            app_key TEXT,
            dd_site TEXT DEFAULT 'datadoghq.com',
            created_at REAL
        )
    """)
    conn.commit()
    conn.close()
    log.info(f"Database initialized at {DB_PATH}")


def db_get_device(device_id: str) -> dict | None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def db_upsert_device(device_id: str, api_key: str, app_key: str,
                     dd_site: str = "datadoghq.com"):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO devices (device_id, api_key, app_key, dd_site, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            api_key = excluded.api_key,
            app_key = excluded.app_key,
            dd_site = excluded.dd_site
    """, (device_id, api_key, app_key, dd_site, time.time()))
    conn.commit()
    conn.close()


def get_device_headers(device_id: str) -> tuple[dict, str] | None:
    """Get DD API headers for a device. Returns (headers, site) or None."""
    device = db_get_device(device_id)
    if device and device["api_key"]:
        return {
            "DD-API-KEY": device["api_key"],
            "DD-APPLICATION-KEY": device["app_key"],
            "Content-Type": "application/json",
        }, device["dd_site"]

    # Fall back to env var keys
    if DD_API_KEY:
        return {
            "DD-API-KEY": DD_API_KEY,
            "DD-APPLICATION-KEY": DD_APP_KEY,
            "Content-Type": "application/json",
        }, DD_SITE

    return None


# --- MQTT client (runs in background thread) ---
mqtt_client: mqtt.Client = None


def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info(f"MQTT connected to {MQTT_BROKER}")
        # Subscribe to ACK/Resolve for all devices using wildcard
        client.subscribe("dd/pager/+/ack")
        client.subscribe("dd/pager/+/resolve")
        log.info("Subscribed to dd/pager/+/ack and dd/pager/+/resolve")
    else:
        log.error(f"MQTT connect failed: rc={rc}")


def on_mqtt_message(client, userdata, msg):
    alert_id = msg.payload.decode("utf-8").strip()
    topic = msg.topic

    # Extract device_id from topic: dd/pager/{device_id}/ack or /resolve
    parts = topic.split("/")
    if len(parts) != 4:
        log.warning(f"Unknown topic format: {topic}")
        return

    device_id = parts[2]
    action = parts[3]

    if action == "ack":
        log.info(f"Pager {device_id} ACK'd alert: {alert_id}")
        _dd_api_call(device_id, alert_id, "acknowledge")
    elif action == "resolve":
        log.info(f"Pager {device_id} resolved alert: {alert_id}")
        _dd_api_call(device_id, alert_id, "resolve")
    else:
        log.warning(f"Unknown action: {action}")


def _dd_api_call(device_id: str, alert_id: str, action: str):
    """Call Datadog On-Call API to acknowledge or resolve an alert."""
    result = get_device_headers(device_id)
    if not result:
        log.warning(f"No auth for device {device_id}, skipping {action} API call")
        return

    headers, site = result
    url = f"https://api.{site}/api/v2/on-call/pages/{alert_id}/{action}"
    try:
        resp = httpx.post(url, headers=headers, json={}, timeout=10)
        log.info(f"DD {action} for {device_id}: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        log.error(f"DD {action} for {device_id} failed: {e}")


def start_mqtt():
    """Create and connect MQTT client. Safe to call multiple times."""
    global mqtt_client
    if mqtt_client:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception:
            pass

    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id=f"dd-pager-bridge-{DEVICE_ID}"
    )
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_start()
    log.info("MQTT client started")


def ensure_mqtt():
    """Ensure MQTT is connected, restarting if needed."""
    if mqtt_client and mqtt_client.is_connected():
        return
    log.warning("MQTT not connected, restarting client...")
    try:
        start_mqtt()
    except Exception as e:
        log.error(f"MQTT start failed: {e}, retrying...")
        time.sleep(1)
        try:
            start_mqtt()
        except Exception as e2:
            log.error(f"MQTT retry failed: {e2}")
            raise
    for _ in range(40):
        if mqtt_client and mqtt_client.is_connected():
            log.info("MQTT reconnected successfully")
            return
        time.sleep(0.25)
    raise RuntimeError("MQTT reconnect timed out after 10s")


def stop_mqtt():
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


def publish_to_device(device_id: str, subtopic: str, payload: str):
    """Publish a message to a specific pager's MQTT topic."""
    ensure_mqtt()
    topic = f"dd/pager/{device_id}/{subtopic}"
    mqtt_client.publish(topic, payload, qos=1)
    log.info(f"Published to {topic}")


# --- FastAPI app ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_mqtt()
    yield
    stop_mqtt()


app = FastAPI(title="Datadog Pager Bridge", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "mqtt_connected": mqtt_client.is_connected() if mqtt_client else False,
        "device_id": DEVICE_ID,
    }


# --- Webhook endpoints ---


@app.post("/webhook")
async def webhook_legacy(request: Request):
    """Legacy webhook for single-device setup."""
    return await _handle_webhook(request, DEVICE_ID)


@app.post("/webhook/{device_id}")
async def webhook_device(request: Request, device_id: str):
    """Per-device webhook endpoint."""
    return await _handle_webhook(request, device_id)


async def _handle_webhook(request: Request, device_id: str):
    try:
        body = await request.json()
        log.info(f"Webhook [{device_id}]: {json.dumps(body)[:500]}")

        alert_id = str(body.get("id", body.get("alert_id", body.get("page_id", str(uuid.uuid4())[:8]))))
        title = body.get("title", body.get("message", body.get("name", "Alert")))
        severity = body.get("severity", body.get("priority", body.get("urgency", "P?")))
        service = body.get("service", body.get("service_name", body.get("tags", {}).get("service", "unknown")))

        if isinstance(title, dict):
            title = str(title)
        if isinstance(service, dict):
            service = str(service)

        payload = json.dumps({
            "id": alert_id,
            "title": title[:120],
            "severity": str(severity)[:30],
            "service": str(service)[:60],
        })

        publish_to_device(device_id, "alert", payload)
        return {"status": "published", "alert_id": alert_id, "device_id": device_id}
    except Exception as e:
        log.error(f"Webhook error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}


@app.post("/test-alert")
async def test_alert(request: Request):
    """Send a fake test alert to a pager."""
    device_id = request.query_params.get("device", DEVICE_ID)
    try:
        body = await request.json()
        device_id = body.get("device_id", device_id)
        title = body.get("title", "Test Alert")
        severity = body.get("severity", "P3 - Test")
        service = body.get("service", "pager-setup")
    except Exception:
        title = "Test Alert"
        severity = "P3 - Test"
        service = "pager-setup"

    alert_id = f"test-{uuid.uuid4().hex[:6]}"
    payload = json.dumps({
        "id": alert_id,
        "title": title,
        "severity": severity,
        "service": service,
    })

    log.info(f"Test alert → {device_id}: {payload}")
    try:
        publish_to_device(device_id, "alert", payload)
    except Exception as e:
        log.error(f"MQTT publish failed: {e}")
        return {"status": "error", "detail": f"MQTT publish failed: {e}"}

    return {"status": "sent", "alert_id": alert_id, "device_id": device_id}


# --- Connect device (API key entry) ---


@app.post("/connect")
async def connect_device(request: Request):
    """Save DD API keys for a device and optionally create the webhook."""
    try:
        form = await request.form()
        device_id = form.get("device_id", DEVICE_ID)
        api_key = form.get("api_key", "").strip()
        app_key = form.get("app_key", "").strip()
        dd_site = form.get("dd_site", "datadoghq.com").strip()

        if not api_key or not app_key:
            return HTMLResponse("<h1>Error</h1><p>Both API Key and Application Key are required.</p>", status_code=400)

        # Verify the keys work by calling a simple DD API endpoint
        try:
            resp = httpx.get(
                f"https://api.{dd_site}/api/v1/validate",
                headers={
                    "DD-API-KEY": api_key,
                    "DD-APPLICATION-KEY": app_key,
                },
                timeout=10,
            )
            if resp.status_code != 200:
                log.error(f"Key validation failed: {resp.status_code} {resp.text[:200]}")
                return HTMLResponse(
                    f"<h1>Invalid Keys</h1><p>Datadog rejected the keys (HTTP {resp.status_code}). "
                    f"Please check and try again.</p><p><a href='/setup?device={device_id}'>Back to setup</a></p>",
                    status_code=400,
                )
        except Exception as e:
            log.error(f"Key validation error: {e}")
            return HTMLResponse(f"<h1>Connection Error</h1><p>{e}</p>", status_code=500)

        # Store keys
        db_upsert_device(device_id, api_key, app_key, dd_site)
        log.info(f"API keys stored for device {device_id}")

        # Try to auto-create webhook in the customer's Datadog org
        webhook_url = f"{BRIDGE_URL}/webhook/{device_id}"
        try:
            webhook_resp = httpx.post(
                f"https://api.{dd_site}/api/v1/integration/webhooks/configuration/webhooks",
                headers={
                    "DD-API-KEY": api_key,
                    "DD-APPLICATION-KEY": app_key,
                    "Content-Type": "application/json",
                },
                json={
                    "name": f"datadog-pager-{device_id}",
                    "url": webhook_url,
                    "payload": json.dumps({
                        "id": "$PAGE_ID",
                        "title": "$PAGE_TITLE",
                        "severity": "$PAGE_URGENCY",
                        "service": "$PAGE_SERVICE",
                    }),
                    "encode_as": "json",
                },
                timeout=10,
            )
            log.info(f"Webhook creation: {webhook_resp.status_code} {webhook_resp.text[:200]}")
        except Exception as e:
            log.warning(f"Auto-webhook creation failed (non-fatal): {e}")

        # Notify the pager that setup is complete
        try:
            publish_to_device(device_id, "setup_complete", "connected")
        except Exception as e:
            log.warning(f"Setup complete MQTT publish failed: {e}")

        # Success page
        return HTMLResponse(f"""<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Pager Connected!</title>
    <style>
        body {{ font-family: -apple-system, sans-serif; background: #f5f5f5; color: #333;
               padding: 40px 20px; max-width: 500px; margin: 0 auto; text-align: center; }}
        .checkmark {{ font-size: 80px; margin-bottom: 16px; color: #2e7d32; }}
        h1 {{ color: #632ca6; margin-bottom: 8px; }}
        p {{ color: #666; font-size: 18px; line-height: 1.5; }}
        .device {{ background: #f0e6ff; padding: 8px 16px; border-radius: 8px;
                   display: inline-block; margin: 16px 0; font-family: monospace; }}
    </style>
</head>
<body>
    <div class="checkmark">&#10003;</div>
    <h1>Pager Connected!</h1>
    <div class="device">{device_id}</div>
    <p>Your Datadog Pager is now linked to your Datadog account.
       On-Call alerts will appear on the pager automatically.</p>
    <p style="margin-top: 24px; font-size: 14px; color: #999;">You can close this tab.</p>
</body>
</html>""")

    except Exception as e:
        log.error(f"Connect error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Error</h1><p>{e}</p>", status_code=500)


# --- Setup page ---


@app.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request):
    """Setup instructions page with API key connection + test alert."""
    device_id = request.query_params.get("device", DEVICE_ID)
    connected = mqtt_client.is_connected() if mqtt_client else False
    status_color = "#632ca6" if connected else "#e74c3c"
    status_text = "Connected" if connected else "Disconnected"

    # Check if device has API keys stored
    device = db_get_device(device_id)
    dd_connected = device is not None and device.get("api_key")
    dd_status = "Connected to Datadog" if dd_connected else "Not connected"
    dd_color = "#2e7d32" if dd_connected else "#e74c3c"

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Datadog Pager Setup</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               background: #f5f5f5; color: #333; padding: 20px; max-width: 600px; margin: 0 auto; }}
        h1 {{ color: #632ca6; margin-bottom: 8px; }}
        h2 {{ margin-bottom: 16px; }}
        .badges {{ margin-bottom: 24px; display: flex; gap: 8px; flex-wrap: wrap; }}
        .status {{ display: inline-block; padding: 4px 12px; border-radius: 12px;
                   color: white; font-size: 14px; }}
        .card {{ background: white; border-radius: 12px; padding: 24px; margin-bottom: 16px;
                 box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .step {{ display: flex; gap: 12px; margin-bottom: 16px; }}
        .step-num {{ background: #632ca6; color: white; width: 28px; height: 28px; border-radius: 50%;
                     display: flex; align-items: center; justify-content: center; font-weight: bold;
                     flex-shrink: 0; font-size: 14px; }}
        .step-text {{ padding-top: 3px; }}
        .step-text strong {{ display: block; margin-bottom: 2px; }}
        .step-text code {{ background: #f0e6ff; padding: 2px 6px; border-radius: 4px; font-size: 14px; }}
        label {{ display: block; font-weight: 600; margin-bottom: 4px; margin-top: 12px; }}
        input[type=text], input[type=password], select {{
            width: 100%; padding: 10px 12px; border: 1px solid #ddd; border-radius: 8px;
            font-size: 16px; font-family: monospace; }}
        input:focus {{ outline: none; border-color: #632ca6; box-shadow: 0 0 0 2px rgba(99,44,166,0.2); }}
        .btn {{ display: block; text-align: center; text-decoration: none;
                padding: 14px 32px; border-radius: 8px; font-size: 16px; cursor: pointer;
                width: 100%; font-weight: 600; border: none; }}
        .btn-primary {{ background: #632ca6; color: white; }}
        .btn-primary:hover {{ background: #7b3ec9; }}
        .btn-secondary {{ background: white; color: #632ca6; border: 2px solid #632ca6; }}
        .btn-secondary:hover {{ background: #f0e6ff; }}
        .btn-success {{ background: #2e7d32; color: white; }}
        .device-id {{ font-family: monospace; background: #f0e6ff; padding: 4px 8px; border-radius: 4px; }}
        .hint {{ font-size: 13px; color: #888; margin-top: 4px; }}
        .hint a {{ color: #632ca6; }}
        #result {{ margin-top: 12px; padding: 12px; border-radius: 8px; display: none; }}
        .success {{ background: #e8f5e9; color: #2e7d32; }}
        .error {{ background: #fbe9e7; color: #c62828; }}
        .connected-badge {{ background: #e8f5e9; color: #2e7d32; padding: 12px; border-radius: 8px;
                            margin-bottom: 12px; text-align: center; font-weight: 600; }}
    </style>
</head>
<body>
    <h1>Datadog Pager</h1>
    <div class="badges">
        <span class="status" style="background:{status_color}">MQTT: {status_text}</span>
        <span class="status" style="background:{dd_color}">{dd_status}</span>
    </div>

    <div class="card">
        <p style="margin-bottom:12px">Device: <span class="device-id">{device_id}</span></p>
        <h2>Setup Instructions</h2>
        <div class="step">
            <div class="step-num">1</div>
            <div class="step-text">
                <strong>Power on the pager</strong>
                Plug in USB-C. LEDs will cycle rainbow during setup.
            </div>
        </div>
        <div class="step">
            <div class="step-num">2</div>
            <div class="step-text">
                <strong>Connect to pager Wi-Fi</strong>
                On your phone, connect to <code>DatadogPager-XXXX</code>
            </div>
        </div>
        <div class="step">
            <div class="step-num">3</div>
            <div class="step-text">
                <strong>Enter Wi-Fi credentials</strong>
                The captive portal will open. Select your network and enter the password.
            </div>
        </div>
        <div class="step">
            <div class="step-num">4</div>
            <div class="step-text">
                <strong>Connect to Datadog</strong>
                Enter your API keys below to link the pager to your Datadog account.
            </div>
        </div>
        <div class="step">
            <div class="step-num">5</div>
            <div class="step-text">
                <strong>Send a test alert</strong>
                Verify the pager works end-to-end.
            </div>
        </div>
    </div>

    <div class="card">
        <h2>Connect to Datadog</h2>
        {"<div class='connected-badge'>&#10003; Connected to Datadog</div>" if dd_connected else ""}
        <form action="/connect" method="POST">
            <input type="hidden" name="device_id" value="{device_id}">

            <label for="api_key">API Key</label>
            <input type="password" name="api_key" id="api_key" placeholder="Enter your Datadog API key" required>
            <p class="hint">Find at <a href="https://app.datadoghq.com/organization-settings/api-keys" target="_blank">Organization Settings &gt; API Keys</a></p>

            <label for="app_key">Application Key</label>
            <input type="password" name="app_key" id="app_key" placeholder="Enter your Datadog Application key" required>
            <p class="hint">Find at <a href="https://app.datadoghq.com/organization-settings/application-keys" target="_blank">Organization Settings &gt; Application Keys</a></p>

            <label for="dd_site">Datadog Site</label>
            <select name="dd_site" id="dd_site">
                <option value="datadoghq.com" selected>US1 (datadoghq.com)</option>
                <option value="us3.datadoghq.com">US3 (us3.datadoghq.com)</option>
                <option value="us5.datadoghq.com">US5 (us5.datadoghq.com)</option>
                <option value="datadoghq.eu">EU (datadoghq.eu)</option>
                <option value="ap1.datadoghq.com">AP1 (ap1.datadoghq.com)</option>
                <option value="ddog-gov.com">GOV (ddog-gov.com)</option>
            </select>

            <button type="submit" class="btn btn-primary" style="margin-top:16px">
                {"Reconnect to Datadog" if dd_connected else "Connect to Datadog"}
            </button>
        </form>
    </div>

    <div class="card">
        <button class="btn btn-secondary" onclick="sendTest()">Send Test Alert</button>
        <div id="result"></div>
    </div>

    <script>
        async function sendTest() {{
            const btn = document.querySelectorAll('.btn-secondary')[0];
            const result = document.getElementById('result');
            btn.disabled = true;
            btn.textContent = 'Sending...';
            try {{
                const resp = await fetch('/test-alert?device={device_id}', {{ method: 'POST' }});
                const data = await resp.json();
                result.className = 'success';
                result.style.display = 'block';
                result.textContent = 'Alert sent! Check your pager. Press A to ACK.';
            }} catch (e) {{
                result.className = 'error';
                result.style.display = 'block';
                result.textContent = 'Failed to send: ' + e.message;
            }}
            btn.disabled = false;
            btn.textContent = 'Send Test Alert';
        }}
    </script>
</body>
</html>"""

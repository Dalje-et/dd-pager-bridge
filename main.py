"""
Datadog Pager Bridge Server

Bridges Datadog On-Call webhooks ↔ MQTT ↔ Physical pager device.

Routes:
  POST /webhook       — Receives DD On-Call webhook, publishes alert to MQTT
  POST /test-alert     — Sends a fake test alert to the pager
  GET  /setup          — Setup instructions page with "Send test alert" button
  GET  /health         — Health check

MQTT:
  Subscribes to dd/pager/{device_id}/ack and dd/pager/{device_id}/resolve
  On receive → calls Datadog On-Call API to ACK/resolve the alert
"""

import json
import logging
import os
import ssl
import uuid
from contextlib import asynccontextmanager

import httpx
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bridge")

# --- Config from environment ---
MQTT_BROKER = os.environ["MQTT_BROKER"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASS = os.environ["MQTT_PASS"]

DD_API_KEY = os.environ.get("DD_API_KEY", "")
DD_APP_KEY = os.environ.get("DD_APP_KEY", "")
DD_SITE = os.environ.get("DD_SITE", "datadoghq.com")

DEVICE_ID = os.environ.get("DEVICE_ID", "ddpager-poc-001")

TOPIC_ALERT = f"dd/pager/{DEVICE_ID}/alert"
TOPIC_ACK = f"dd/pager/{DEVICE_ID}/ack"
TOPIC_RESOLVE = f"dd/pager/{DEVICE_ID}/resolve"

# --- MQTT client (runs in background thread) ---
mqtt_client: mqtt.Client = None


def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info(f"MQTT connected to {MQTT_BROKER}")
        client.subscribe(TOPIC_ACK)
        client.subscribe(TOPIC_RESOLVE)
        log.info(f"Subscribed to {TOPIC_ACK} and {TOPIC_RESOLVE}")
    else:
        log.error(f"MQTT connect failed: rc={rc}")


def on_mqtt_message(client, userdata, msg):
    alert_id = msg.payload.decode("utf-8").strip()
    topic = msg.topic

    if topic == TOPIC_ACK:
        log.info(f"Pager ACK'd alert: {alert_id}")
        _dd_acknowledge(alert_id)
    elif topic == TOPIC_RESOLVE:
        log.info(f"Pager resolved alert: {alert_id}")
        _dd_resolve(alert_id)
    else:
        log.warning(f"Unknown topic: {topic}")


def _dd_acknowledge(alert_id: str):
    """Call Datadog On-Call API to acknowledge an alert."""
    if not DD_API_KEY:
        log.warning("DD_API_KEY not set, skipping ACK API call")
        return

    url = f"https://api.{DD_SITE}/api/v2/on-call/pages/{alert_id}/acknowledge"
    headers = {
        "DD-API-KEY": DD_API_KEY,
        "DD-APPLICATION-KEY": DD_APP_KEY,
        "Content-Type": "application/json",
    }
    try:
        resp = httpx.post(url, headers=headers, json={}, timeout=10)
        log.info(f"DD ACK response: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        log.error(f"DD ACK failed: {e}")


def _dd_resolve(alert_id: str):
    """Call Datadog On-Call API to resolve an alert."""
    if not DD_API_KEY:
        log.warning("DD_API_KEY not set, skipping resolve API call")
        return

    url = f"https://api.{DD_SITE}/api/v2/on-call/pages/{alert_id}/resolve"
    headers = {
        "DD-API-KEY": DD_API_KEY,
        "DD-APPLICATION-KEY": DD_APP_KEY,
        "Content-Type": "application/json",
    }
    try:
        resp = httpx.post(url, headers=headers, json={}, timeout=10)
        log.info(f"DD resolve response: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        log.error(f"DD resolve failed: {e}")


def start_mqtt():
    """Create and connect MQTT client. Safe to call multiple times."""
    global mqtt_client
    # Stop existing client if any
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
    """Ensure MQTT is connected, restarting if needed. Blocks until connected."""
    import time
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
    # Wait up to 10 seconds for connection
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


# --- FastAPI app ---


@asynccontextmanager
async def lifespan(app: FastAPI):
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


def publish_to_pager(payload: str):
    """Publish a message to the pager's MQTT topic, reconnecting if needed."""
    ensure_mqtt()
    mqtt_client.publish(TOPIC_ALERT, payload, qos=1)
    # Fire-and-forget: paho queues QoS 1 messages and delivers when connected


@app.post("/webhook")
async def webhook(request: Request):
    """
    Receives Datadog On-Call webhook.
    Extracts alert info and publishes to MQTT for the pager.
    """
    try:
        return await _handle_webhook(request)
    except Exception as e:
        log.error(f"Webhook unhandled error: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}


async def _handle_webhook(request: Request):
    body = await request.json()
    log.info(f"Webhook received: {json.dumps(body)[:500]}")

    # Extract alert details from DD On-Call webhook payload
    # DD On-Call webhook format varies — adapt as needed
    alert_id = str(body.get("id", body.get("alert_id", body.get("page_id", str(uuid.uuid4())[:8]))))
    title = body.get("title", body.get("message", body.get("name", "Alert")))
    severity = body.get("severity", body.get("priority", body.get("urgency", "P?")))
    service = body.get("service", body.get("service_name", body.get("tags", {}).get("service", "unknown")))

    # Handle nested structures
    if isinstance(title, dict):
        title = str(title)
    if isinstance(service, dict):
        service = str(service)

    payload = json.dumps({
        "id": alert_id,
        "title": title[:120],  # truncate for e-ink display
        "severity": str(severity)[:30],
        "service": str(service)[:60],
    })

    log.info(f"Publishing to {TOPIC_ALERT}: {payload}")
    try:
        publish_to_pager(payload)
    except Exception as e:
        log.error(f"MQTT publish failed: {e}")
        return {"status": "error", "detail": f"MQTT publish failed: {e}"}

    return {"status": "published", "alert_id": alert_id}


@app.post("/test-alert")
async def test_alert():
    """Send a fake test alert to the pager."""
    alert_id = f"test-{uuid.uuid4().hex[:6]}"
    payload = json.dumps({
        "id": alert_id,
        "title": "Test Alert",
        "severity": "P3 - Test",
        "service": "pager-setup",
    })

    log.info(f"Test alert → {TOPIC_ALERT}: {payload}")
    try:
        publish_to_pager(payload)
    except Exception as e:
        log.error(f"MQTT publish failed: {e}")
        return {"status": "error", "detail": f"MQTT publish failed: {e}"}

    return {"status": "sent", "alert_id": alert_id}


@app.get("/setup", response_class=HTMLResponse)
async def setup_page():
    """Setup instructions page with test alert button."""
    connected = mqtt_client.is_connected() if mqtt_client else False
    status_color = "#632ca6" if connected else "#e74c3c"
    status_text = "Connected" if connected else "Disconnected"

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
        .status {{ display: inline-block; padding: 4px 12px; border-radius: 12px;
                   background: {status_color}; color: white; font-size: 14px; margin-bottom: 24px; }}
        .card {{ background: white; border-radius: 12px; padding: 24px; margin-bottom: 16px;
                 box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .step {{ display: flex; gap: 12px; margin-bottom: 16px; }}
        .step-num {{ background: #632ca6; color: white; width: 28px; height: 28px; border-radius: 50%;
                     display: flex; align-items: center; justify-content: center; font-weight: bold;
                     flex-shrink: 0; font-size: 14px; }}
        .step-text {{ padding-top: 3px; }}
        .step-text strong {{ display: block; margin-bottom: 2px; }}
        .step-text code {{ background: #f0e6ff; padding: 2px 6px; border-radius: 4px; font-size: 14px; }}
        button {{ background: #632ca6; color: white; border: none; padding: 16px 32px;
                  border-radius: 8px; font-size: 18px; cursor: pointer; width: 100%;
                  font-weight: 600; }}
        button:hover {{ background: #7b3ec9; }}
        button:active {{ background: #4a1f80; }}
        #result {{ margin-top: 12px; padding: 12px; border-radius: 8px; display: none; }}
        .success {{ background: #e8f5e9; color: #2e7d32; }}
        .error {{ background: #fbe9e7; color: #c62828; }}
    </style>
</head>
<body>
    <h1>Datadog Pager</h1>
    <span class="status">MQTT: {status_text}</span>

    <div class="card">
        <h2 style="margin-bottom: 16px;">Setup Instructions</h2>
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
                <strong>Send a test alert</strong>
                Once the pager shows "Listening...", tap the button below.
            </div>
        </div>
    </div>

    <div class="card">
        <button onclick="sendTest()">Send Test Alert</button>
        <div id="result"></div>
    </div>

    <script>
        async function sendTest() {{
            const btn = document.querySelector('button');
            const result = document.getElementById('result');
            btn.disabled = true;
            btn.textContent = 'Sending...';
            try {{
                const resp = await fetch('/test-alert', {{ method: 'POST' }});
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

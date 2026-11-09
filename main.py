import json
import asyncio
import sqlite3
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
import numpy as np
from sklearn.ensemble import IsolationForest

DB_FILE = "generator_data.db"
MQTT_COMMAND_TOPIC = "generator/commands"

# --- 1. DATABASE INIT & OPTIMIZATION ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Enable WAL mode for high-frequency concurrent writes
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT,
        end_time TEXT,
        duration_seconds REAL,
        avg_rpm REAL,
        peak_temp REAL,
        avg_delta_g REAL,
        initial_fuel REAL,
        final_fuel REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS telemetry_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER,
        timestamp TEXT,
        rpm INTEGER,
        coolant_temp REAL,
        fuel_level REAL,
        vibration_delta_g REAL,
        health_index REAL,
        temp_rate_of_change REAL,
        fuel_burn_rate REAL,
        FOREIGN KEY(session_id) REFERENCES sessions(session_id)
    )
    """)
    conn.commit()
    conn.close()

init_db()

# --- 2. GLOBAL ML ENGINE STATE & ATTRIBUTION ---
ml_model: Optional[IsolationForest] = None
is_ml_trained: bool = False
feature_means: Optional[np.ndarray] = None
feature_stds: Optional[np.ndarray] = None

FEATURE_NAMES = [
    "Engine Speed (RPM)", 
    "Coolant Temperature", 
    "Temp Change Rate", 
    "3D Vector Vibration", 
    "Fuel Consumption Rate"
]

def train_isolation_forest():
    global ml_model, is_ml_trained, feature_means, feature_stds
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Accept all logged data for baseline training
    cursor.execute("""
        SELECT rpm, coolant_temp, temp_rate_of_change, vibration_delta_g, fuel_burn_rate 
        FROM telemetry_logs 
        WHERE health_index >= 75;

    """)
    rows = cursor.fetchall()
    conn.close()

    if len(rows) < 20:
        print(f"ML Training Deferred: Need at least 20 samples (Current: {len(rows)})")
        return False, len(rows)

    X = np.array(rows)
    feature_means = np.mean(X, axis=0)
    feature_stds = np.std(X, axis=0) + 1e-6  # Prevent division by zero

    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)
    
    ml_model = model
    is_ml_trained = True
    print(f"--- ML ISOLATION FOREST MODEL TRAINED ({len(rows)} samples) ---")
    return True, len(rows)

train_isolation_forest()

# --- 3. STATE & SESSION MANAGEMENT ---
main_event_loop = None
active_session_id: Optional[int] = None
session_start_time: Optional[float] = None
last_mqtt_heartbeat: float = 0.0
telemetry_history: List[dict] = []
shutdown_cooldown_until: float = 0.0  # Prevent repeating redundant shutdown signals

twin_state: Dict[str, Any] = {
    "telemetry": {"rpm": 0, "coolant_temp": 0.0, "fuel_level": 100.0, "vibration_delta_g": 0.0},
    "vibration_delta_g": 0.0,
    "health_index": 100.0,
    "status": "OFFLINE",
    "active_session_id": "Off",
    "temp_rate_of_change": 0.0,
    "fuel_burn_rate": 0.0,
    "rufl_minutes": 0.0,
    "ml_trained": False,
    "anomaly_score": 0.0,
    "anomaly_attribution": "Baseline Parameters Normal",
    "alarms": [],
    "shutdown_override_active": False
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

def manage_run_session(cursor, rpm: int, fuel: float, temp: float, delta_g: float) -> Optional[int]:
    """Creates an active session when RPM > 0, if none is currently active."""
    global active_session_id, session_start_time
    now_str = datetime.now().isoformat()

    # Start a new session only when the engine starts running (RPM > 0)
    if active_session_id is None and rpm > 0:
        session_start_time = time.time()
        cursor.execute("INSERT INTO sessions (start_time, initial_fuel) VALUES (?, ?)", (now_str, fuel))
        active_session_id = cursor.lastrowid
        print(f"--- STARTED RUN SESSION {active_session_id} ---")

    return active_session_id

def close_active_session():
    """Helper to finalize the active session in DB when stopped or offline."""
    global active_session_id, session_start_time
    if active_session_id is None:
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    now_str = datetime.now().isoformat()
    duration = round(time.time() - session_start_time, 1) if session_start_time else 0.0

    cursor.execute("""
        SELECT AVG(rpm), MAX(coolant_temp), AVG(vibration_delta_g), MIN(fuel_level)
        FROM telemetry_logs WHERE session_id = ?
    """, (active_session_id,))
    row = cursor.fetchone()

    avg_rpm = round(row[0] or 0, 1) if row else 0
    max_temp = round(row[1] or 0, 1) if row else 0
    avg_vib = round(row[2] or 0, 3) if row else 0
    final_fuel = round(row[3] or 100.0, 1) if row else 100.0

    cursor.execute("""
        UPDATE sessions 
        SET end_time = ?, duration_seconds = ?, avg_rpm = ?, peak_temp = ?, avg_delta_g = ?, final_fuel = ?
        WHERE session_id = ?
    """, (now_str, duration, avg_rpm, max_temp, avg_vib, final_fuel, active_session_id))
    
    conn.commit()
    conn.close()
    
    print(f"--- CLOSED RUN SESSION {active_session_id} (Duration: {duration}s) ---")
    active_session_id = None
    session_start_time = None

def trigger_actuator_shutdown(reason: str, manual: bool = False):
    """Issues shutdown command to physical ESP32 via MQTT and notifies WebSocket clients."""
    global shutdown_cooldown_until, twin_state
    now = time.time()
    
    # Cooldown check to avoid spamming duplicate shutdown orders (5-second throttle)
    if now < shutdown_cooldown_until and not manual:
        return

    shutdown_cooldown_until = now + 5.0
    command_payload = {
        "command": "SHUTDOWN",
        "reason": reason,
        "manual_override": manual,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        mqtt_client.publish(MQTT_COMMAND_TOPIC, json.dumps(command_payload), qos=1)
        print(f"!!! ACTUATOR SHUTDOWN DISPATCHED -> {MQTT_COMMAND_TOPIC} | Reason: {reason} !!!")
    except Exception as e:
        print(f"Failed to publish MQTT shutdown command: {e}")

    twin_state["shutdown_override_active"] = True

    if main_event_loop and main_event_loop.is_running():
        asyncio.run_coroutine_threadsafe(
            manager.broadcast({
                "type": "SHUTDOWN_COMMAND_ISSUED",
                "reason": reason,
                "manual": manual,
                "timestamp": datetime.now().isoformat()
            }),
            main_event_loop
        )

# --- 4. DIGITAL TWIN TELEMETRY PROCESSING ENGINE ---
def process_digital_twin_logic(payload: dict):
    global twin_state, telemetry_history, last_mqtt_heartbeat, active_session_id
    alarms = []

    last_mqtt_heartbeat = time.time()
    rpm = int(payload.get("rpm", 0))
    temp = float(payload.get("coolant_temp", 0.0))
    fuel_level = float(payload.get("fuel_level", 100.0))
    vibration_delta_g = float(payload.get("vibration_delta_g", 0.0))
    now_timestamp = time.time()

    # --- AUTOMATIC SESSION TERMINATION ON RPM == 0 ---
    if rpm == 0 and active_session_id is not None:
        closed_id = active_session_id
        close_active_session()
        alarms.append({
            "severity": "WARNING", 
            "code": "SESSION_CLOSED", 
            "msg": f"Engine stopped. Session #{closed_id} auto-closed and saved to DB."
        })

    telemetry_history.append({"time": now_timestamp, "temp": temp, "fuel": fuel_level})
    telemetry_history = [p for p in telemetry_history if now_timestamp - p["time"] <= 30.0]

    temp_rate_of_change = 0.0
    fuel_burn_rate = 0.0
    rufl_minutes = 0.0

    if len(telemetry_history) > 3:
        dt = telemetry_history[-1]["time"] - telemetry_history[0]["time"]
        if dt > 0:
            dtemp = telemetry_history[-1]["temp"] - telemetry_history[0]["temp"]
            temp_rate_of_change = round((dtemp / dt) * 60.0, 2)

            dfuel = telemetry_history[0]["fuel"] - telemetry_history[-1]["fuel"]
            fuel_burn_rate = round(max(0.0, (dfuel / dt) * 60.0), 2)

    if fuel_burn_rate > 0:
        rufl_minutes = round(fuel_level / fuel_burn_rate, 1)

    # --- AUTONOMOUS SHUTDOWN CONDITIONS EVALUATION ---
    shutdown_triggered = False
    shutdown_reasons = []

    # Condition A: Rapid Temperature Rate Spike (Thermal Shock Protection)
    if temp_rate_of_change >= 70.0 and rpm > 0:
        shutdown_triggered = True
        shutdown_reasons.append(f"Rapid Temperature Surge (+{temp_rate_of_change}°C/min)")
        alarms.append({"severity": "CRITICAL", "code": "TEMP_SPIKE_SHUTDOWN", "msg": f"Fast temp rise: +{temp_rate_of_change}°C/min! Initiating emergency shutdown."})

    # Condition B: Low Fuel Protection (Fuel Level <= 10%)
    if fuel_level <= 5.0 and rpm > 0:
        shutdown_triggered = True
        shutdown_reasons.append(f"Low Fuel Safeguard ({fuel_level:.1f}%)")
        alarms.append({"severity": "CRITICAL", "code": "CRITICAL_FUEL_SHUTDOWN", "msg": f"Critical low fuel ({fuel_level:.1f}% <= 10%). Autonomous shutdown activated."})

    # Condition C: Engine Stall Protection
    if 0 < rpm < 500:
        shutdown_triggered = True
        shutdown_reasons.append(f"Engine Stall / Low RPM ({rpm} RPM)")
        alarms.append({"severity": "CRITICAL", "code": "STALL_SHUTDOWN", "msg": f"Engine stalled at {rpm} RPM. Emergency stop triggered."})

    # Standard Threshold Alarms
    if 10.0 < fuel_level < 15.0:
        alarms.append({"severity": "CRITICAL", "code": "CRITICAL_FUEL", "msg": f"Fuel level critical: {fuel_level:.1f}% remaining!"})
    elif 15.0 <= fuel_level < 20.0:
        alarms.append({"severity": "WARNING", "code": "LOW_FUEL", "msg": f"Fuel level low: {fuel_level:.1f}% remaining"})

    if fuel_burn_rate > 6.0 and rpm > 500:
        alarms.append({"severity": "WARNING", "code": "EXCESS_BURN_RATE", "msg": f"Abnormal depletion rate: -{fuel_burn_rate}%/min!"})

    if vibration_delta_g > 0.30:
        alarms.append({"severity": "CRITICAL", "code": "HIGH_VIB_3D", "msg": f"Excessive 3D vector vibration spike: {vibration_delta_g:.3f}g"})

    # Trigger hardware actuator shutdown if any severe fault was detected while engine is running
    if shutdown_triggered:
        trigger_actuator_shutdown(reason="; ".join(shutdown_reasons), manual=False)

    # --- ML Anomaly Detection & Parameter Root-Cause Analysis ---
    anomaly_score = 0.0
    ml_health_deduction = 0.0
    anomaly_attribution = "Normal Operation"

    if is_ml_trained and ml_model is not None:
        feature_vector = np.array([[rpm, temp, temp_rate_of_change, vibration_delta_g, fuel_burn_rate]])
        raw_score = ml_model.score_samples(feature_vector)[0]
        # Normalize score into readable 0-100 anomaly scale
        anomaly_score = round(float(abs(min(0, raw_score + 0.40)) * 200), 1)

        # Calculate deviation (Z-Score) for each metric relative to baseline training mean
        z_scores = np.abs((feature_vector[0] - feature_means) / feature_stds)
        max_idx = int(np.argmax(z_scores))
        max_dev = z_scores[max_idx]

        if raw_score < -0.50 or max_dev > 2.5:
            ml_health_deduction = min(50.0, max_dev * 10.0)
            anomaly_attribution = f"High Deviation: {FEATURE_NAMES[max_idx]} ({max_dev:.1f}σ)"
            alarms.append({"severity": "WARNING", "code": "ML_ANOMALY", "msg": f"ML Divergence: {anomaly_attribution}"})
        else:
            anomaly_attribution = "All parameters within normal baseline range"

    # Health Index Computation
    health = 100.0 - ml_health_deduction
    if temp > 75.0:
        health -= 40.0
    if vibration_delta_g > 0.30:
        health -= 45.0
    elif vibration_delta_g > 0.12:
        health -= 15.0
    if fuel_level < 15.0:
        health -= 10.0

    if 0 < rpm < 500:
        health = min(health, 27.0)

    health_index = max(0.0, min(100.0, round(health, 1)))
    
    # Engine Operating Status
    if rpm == 0:
        status = "STOPPED"
        twin_state["shutdown_override_active"] = False
    elif any(a["severity"] == "CRITICAL" for a in alarms):
        status = "CRITICAL"
    elif alarms:
        status = "WARNING"
    else:
        status = "NORMAL"

    # Database logging
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    session_id = manage_run_session(cursor, rpm, fuel_level, temp, vibration_delta_g)
    
    # Log telemetry only when an active session exists (RPM > 0)
    if session_id:
        cursor.execute("""
            INSERT INTO telemetry_logs 
            (session_id, timestamp, rpm, coolant_temp, fuel_level, vibration_delta_g, health_index, temp_rate_of_change, fuel_burn_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (session_id, datetime.now().isoformat(), rpm, temp, fuel_level, vibration_delta_g, health_index, temp_rate_of_change, fuel_burn_rate))
    
    conn.commit()
    conn.close()

    twin_state = {
        "telemetry": {
            "rpm": rpm,
            "coolant_temp": temp,
            "fuel_level": fuel_level,
            "vibration_delta_g": vibration_delta_g
        },
        "vibration_delta_g": vibration_delta_g,
        "health_index": health_index,
        "status": status,
        "active_session_id": session_id if session_id is not None else "Off",
        "temp_rate_of_change": temp_rate_of_change,
        "fuel_burn_rate": fuel_burn_rate,
        "rufl_minutes": rufl_minutes,
        "ml_trained": is_ml_trained,
        "anomaly_score": anomaly_score,
        "anomaly_attribution": anomaly_attribution,
        "alarms": alarms,
        "shutdown_override_active": twin_state.get("shutdown_override_active", False)
    }

    if main_event_loop and main_event_loop.is_running():
        asyncio.run_coroutine_threadsafe(manager.broadcast(twin_state), main_event_loop)

# --- 5. WATCHDOG TASK ---
async def connection_watchdog():
    """Sets status to OFFLINE and closes session if no telemetry received for 5 seconds."""
    global twin_state
    while True:
        await asyncio.sleep(2.0)
        if time.time() - last_mqtt_heartbeat > 5.0 and last_mqtt_heartbeat > 0:
            if twin_state["status"] != "OFFLINE":
                twin_state["status"] = "OFFLINE"
                twin_state["active_session_id"] = "Off"
                close_active_session()
                await manager.broadcast(twin_state)

# --- 6. MQTT LISTENER ---
def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        process_digital_twin_logic(payload)
    except Exception as e:
        print(f"MQTT Error Parsing Data: {e}")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_message = on_mqtt_message

# --- 7. FASTAPI APPLICATION ENDPOINTS ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    
    watchdog_task = asyncio.create_task(connection_watchdog())
    
    try:
        mqtt_client.connect("192.168.x.xx", 1883, 60)
        mqtt_client.subscribe("generator/telemetry")
        mqtt_client.loop_start()
        print("Connected to MQTT Broker")
    except Exception as e:
        print(f"MQTT Connection Warning: {e}")

    yield

    watchdog_task.cancel()
    close_active_session()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

app = FastAPI(title="Digital Twin Generator Health Monitor", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/state")
async def get_current_state():
    return twin_state

@app.post("/api/control/override")
async def manual_shutdown_override(reason: Optional[str] = "Manual Dashboard Emergency Cutoff"):
    """API endpoint to trigger manual emergency shutdown override at any time."""
    trigger_actuator_shutdown(reason=reason, manual=True)
    return {
        "status": "COMMAND_DISPATCHED",
        "action": "SHUTDOWN",
        "reason": reason,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/ml/train")
async def trigger_ml_training():
    success, count = train_isolation_forest()
    return {"trained": success, "data_points_used": count}

@app.get("/api/history/sessions")
async def get_all_sessions():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            session_id,
            start_time,
            end_time,
            COALESCE(duration_seconds, 0) AS duration_seconds,
            COALESCE(avg_rpm, 0) AS avg_rpm,
            COALESCE(peak_temp, 0) AS peak_temp,
            COALESCE(avg_delta_g, 0) AS avg_delta_g,
            COALESCE(initial_fuel, 100) AS initial_fuel,
            COALESCE(final_fuel, 100) AS final_fuel,
            ROUND(MAX(0, COALESCE(initial_fuel, 100) - COALESCE(final_fuel, 100)), 1) AS total_fuel_consumed
        FROM sessions 
        ORDER BY session_id DESC
    """)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return {"total_sessions": len(rows), "sessions": rows}

@app.get("/api/history/session/{session_id}")
async def get_session_analytics(session_id: int):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,))
    session = cursor.fetchone()
    
    if not session:
        conn.close()
        return {"error": f"Session {session_id} not found."}
        
    session_data = dict(session)

    cursor.execute("""
        SELECT 
            id, timestamp, rpm, coolant_temp, fuel_level, 
            vibration_delta_g, health_index, temp_rate_of_change, fuel_burn_rate 
        FROM telemetry_logs 
        WHERE session_id = ? 
        ORDER BY id ASC
    """, (session_id,))
    logs = [dict(r) for r in cursor.fetchall()]
    conn.close()

    min_health = min([l["health_index"] for l in logs]) if logs else 100.0
    avg_burn_rate = round(sum([l["fuel_burn_rate"] for l in logs]) / len(logs), 2) if logs else 0.0

    return {
        "session_summary": {
            **session_data,
            "min_health_index": min_health,
            "avg_fuel_burn_rate": avg_burn_rate
        },
        "telemetry_count": len(logs),
        "logs": logs
    }

@app.websocket("/ws/twin")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_json(twin_state)
        while True:
            # Handle incoming client commands over WebSocket (e.g. {"action": "SHUTDOWN"})
            raw_msg = await websocket.receive_text()
            try:
                cmd_data = json.loads(raw_msg)
                if cmd_data.get("action") == "SHUTDOWN":
                    reason = cmd_data.get("reason", "WebSocket Emergency Stop")
                    trigger_actuator_shutdown(reason=reason, manual=True)
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

    #### Full Stop
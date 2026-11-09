#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <Wire.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <math.h>

const char* WIFI_SSID         = "wifi_name";
const char* WIFI_PASSWORD     = "password";
const char* MQTT_SERVER       = "192.168.x.xx"; 


const int   MQTT_PORT         = 1883;
const char* MQTT_TOPIC_TELEMETRY = "generator/telemetry";
const char* MQTT_TOPIC_COMMANDS  = "generator/commands";

#define RELAY_PIN         23  // Low-level trigger relay output pin
#define HALL_SENSOR_PIN   16  // Hall effect interrupt pin
#define FUEL_SENSOR_PIN   34  // Fuel-Level Sensor
#define ONE_WIRE_BUS      18  // DS18B20 Data pin
#define I2C_SDA           21  // MPU6050 SDA
#define I2C_SCL           22  // MPU6050 SCL

// Configuration Parameters 
const int SAMPLE_COUNT = 100;  
const int MAGNETS_PER_REV = 1;      
const unsigned long BOOT_GUARD_MS = 3000; // 3-second delay guard before reading/publishing

// System Safety State
bool emergency_shutdown_active = false;
bool boot_stabilized = false;

// RPM Global Variables
volatile unsigned long pulse_count = 0;
unsigned long last_rpm_calc_time = 0;
float current_rpm = 0.0;
float sim_fuel_level = 100.0;

void simulateFuelDepletion() {
// Calibrate these values using the sensor's raw ADC readings:
const int FUEL_EMPTY_RAW = 0;
const int FUEL_FULL_RAW  = 4095;

long rawTotal = 0;
const int READ_COUNT = 10;

for (int i = 0; i < READ_COUNT; i++) {
rawTotal += analogRead(FUEL_SENSOR_PIN);
delay(5);
}

int rawFuelReading = rawTotal / READ_COUNT;
int fuelPercentage = map(rawFuelReading,FUEL_EMPTY_RAW, FUEL_FULL_RAW, 0, 100);

sim_fuel_level = constrain(fuelPercentage, 0, 100);
}

// Temperature Guarding Variable
float last_valid_temp = 0.0;

// Hardware Drivers
Adafruit_MPU6050 mpu;
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature tempSensor(&oneWire);

WiFiClient espClient;
PubSubClient mqttClient(espClient);

// --- Interrupt Service Routine for RPM ---
void IRAM_ATTR handlePulse() {
  pulse_count++;
}

// --- Relay Control Functions ---
void executeShutdown(const char* reason) {
  emergency_shutdown_active = true;
  
  // Low-Level Relay: 
  digitalWrite(RELAY_PIN, LOW); 
  
  current_rpm = 0.0; // Force zeroed RPM state
  
  Serial.println("Relay Triggered: Emergency Shutdown Actuated by Backend!!");
}

// --- MQTT Downlink Callback Handler ---
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  Serial.print("[MQTT Downlink] Message arrived on topic: ");
  Serial.println(topic);

  // Allocate JSON buffer for incoming command
  StaticJsonDocument<256> doc;
  DeserializationError error = deserializeJson(doc, payload, length);

  if (error) {
    Serial.print("Failed to parse command JSON: ");
    Serial.println(error.f_str());
    return;
  }

  const char* command = doc["command"];
  const char* reason  = doc["reason"] | "Unspecified Safety Override";

  if (command && strcmp(command, "SHUTDOWN") == 0) {
    executeShutdown(reason);
  }
}

// --- WiFi & MQTT Connection Setup ---
void setupWiFi() {
  delay(10);
  Serial.print("Connecting to WiFi: ");
  Serial.println(WIFI_SSID);
  
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  
  Serial.println("\nWiFi Connected!");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());
}

void reconnectMQTT() {
  while (!mqttClient.connected()) {
    Serial.print("Attempting MQTT connection...");
    String clientId = "ESP32_GeneratorTwin_";
    clientId += String(random(0xffff), HEX);
    
    if (mqttClient.connect(clientId.c_str())) {
      Serial.println(" Connected to MQTT Broker!");
      
      // Subscribe to Downlink Actuator Commands
      mqttClient.subscribe(MQTT_TOPIC_COMMANDS);
      Serial.print("Subscribed to Command Topic: ");
      Serial.println(MQTT_TOPIC_COMMANDS);
    } else {
      Serial.print(" Failed, rc=");
      Serial.print(mqttClient.state());
      Serial.println(" Retrying in 2 seconds...");
      delay(2000);
    }
  }
}

void setup() {
  pinMode(RELAY_PIN, OUTPUT);
  digitalWrite(RELAY_PIN, HIGH); // HIGH = Relay Off 

  Serial.begin(115200);
  delay(1000);
  Serial.println("Starting Digital Generator Monitor Node");

  pinMode(HALL_SENSOR_PIN, INPUT_PULLUP);

  // Attach Interrupt for Hall Sensor 
  attachInterrupt(digitalPinToInterrupt(HALL_SENSOR_PIN), handlePulse, FALLING);

  // Initialize Explicit I2C Pins for MPU6050
  Wire.begin(I2C_SDA, I2C_SCL);
  
  if (!mpu.begin()) {
    Serial.println("Could not find MPU6050 chip!");
  } else {
    Serial.println("MPU6050 Initialized Successfully.");
    mpu.setAccelerometerRange(MPU6050_RANGE_8_G);
  }
  
  // Initialize Temperature Sensor & perform throwaway conversion during boot warm-up
  tempSensor.begin();
  tempSensor.requestTemperatures(); 
  Serial.println("DS18B20 Temperature Sensor Initialized");

  // Initialize Network & MQTT Callback
  setupWiFi();
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);

}

// --- Sensor Sampling Logic ---
float getAverageVibrationDeltaG() {
  if (emergency_shutdown_active) return 0.0;

  sensors_event_t a, g, temp;
  float sum_delta_g = 0.0;
  
  for (int i = 0; i < SAMPLE_COUNT; i++) {
    mpu.getEvent(&a, &g, &temp);
    
    float ax_g = a.acceleration.x / 9.80665;
    float ay_g = a.acceleration.y / 9.80665;
    float az_g = a.acceleration.z / 9.80665;

    float vector_mag = sqrt((ax_g * ax_g) + (ay_g * ay_g) + (az_g * az_g));
    sum_delta_g += fabs(vector_mag - 1.0);

    delayMicroseconds(1000); 
  }
  
  return sum_delta_g / (float)SAMPLE_COUNT;
}

void calculateRPM() {
  if (emergency_shutdown_active) {
    current_rpm = 0.0;
    return;
  }

  unsigned long now = millis();
  unsigned long time_elapsed = now - last_rpm_calc_time;
  
  if (time_elapsed >= 1000) { 
    noInterrupts();
    unsigned long pulses = pulse_count;
    pulse_count = 0;
    interrupts();

    current_rpm = ((float)pulses / (float)MAGNETS_PER_REV) * (60000.0 / (float)time_elapsed);
    last_rpm_calc_time = now;
  }
}

void loop() {
  if (!mqttClient.connected()) {
    reconnectMQTT();
  }
  mqttClient.loop(); // Process incoming MQTT messages (like SHUTDOWN commands)

  // 1. Check Boot Stabilization Guard
  if (!boot_stabilized) {
    if (millis() < BOOT_GUARD_MS) {
      delay(500);
      return; 
    } else {
      boot_stabilized = true;
      Serial.println(" Stabilization complete. Normal telemetry stream active.");
    }
  }

  // 2. Calculate Engine Speed
  calculateRPM();

  // 3. Update Fuel Level
  simulateFuelDepletion();

  if (sim_fuel_level <= 0) {
    current_rpm = 0.0;
  }

  // 4. Read Sensors (Only when normal state)
  float vibration_delta_g = 0.0;
  float coolant_temp = 0.0;

  if (!emergency_shutdown_active) {
    vibration_delta_g = getAverageVibrationDeltaG();
    
    // DS18B20 Reading with negative/error value suppression
    tempSensor.requestTemperatures();
    float raw_temp = tempSensor.getTempCByIndex(0);

    // DEVICE_DISCONNECTED_C is -127.0°C; negative values during boot are filtered
    if (raw_temp <= 0.0 || raw_temp == DEVICE_DISCONNECTED_C) {
      coolant_temp = last_valid_temp; // Fallback to last valid reading or 0.0
    } else {
      coolant_temp = raw_temp;
      last_valid_temp = raw_temp;
    }
  } else {
    // If shutdown is active, hold telemetry at 0/safety defaults
    vibration_delta_g = 0.0;
    coolant_temp = 0.0;
    current_rpm = 0.0;
    sim_fuel_level = 0.0;
  }

  // 5. Construct Payload
  StaticJsonDocument<256> doc;
  doc["rpm"] = (int)current_rpm;
  doc["coolant_temp"] = serialized(String(coolant_temp, 1));
  doc["fuel_level"] = sim_fuel_level;
  doc["vibration_delta_g"] = serialized(String(vibration_delta_g, 3));

  // 6. Serialize Payload
  char jsonBuffer[256];
  serializeJson(doc, jsonBuffer);

  // 7. Print to Serial Monitor & Publish to MQTT
  Serial.println(jsonBuffer);
  mqttClient.publish(MQTT_TOPIC_TELEMETRY, jsonBuffer);

  delay(500); 
}
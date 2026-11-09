# Digital-Machine-Health-Monitor
The Digital Machine Health Monitor is an IoT-based system designed for real-time generator monitoring and predictive maintenance.
It uses an ESP32 and multiple sensors to monitor RPM, coolant temperature, fuel level, and machine vibration.
Sensor data is transmitted through MQTT to a Python FastAPI backend for processing and storage in SQLite.
A web-based dashboard provides live measurements, telemetry charts, operational alerts, and historical records.
An Isolation Forest machine-learning model analyses operating data to identify unusual machine behaviour.
The system also implements threshold-based fault detection and prototype remote shutdown functionality.
This project demonstrates how embedded systems, IoT communication, web technologies, and machine learning can support smarter equipment monitoring.

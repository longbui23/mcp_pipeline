# Real-time
- Detect overpressure/leaks (pressure sensor)
- Temperature sensors (prevent overheating)
- Flow Rate (early detection of leak or blockage)
- Vibration + mechanical failures
- Valve positions
- Drilling parameters

# No real-time
- Oil composition/ density/ gravity
- Tank level measurement
- Well production rates
- Equipment maintanence logs
- Temp history for trend analytics
- Environment sensors
- Weather/wind/humidity

# Steps
- Source: IOT Sensors --> Rasperri Pi Gateway
- Ingest: Kafka, Spark Stream (CheckPoint for fault tolerance)
- Preprocessing: Normalization, Scaling (Minmax, Standard), Imputation
- Feature Engineering

# Anomaly Detection Models
- LSTM Auto Encoder (time-series reconstruction)
- Isolation Forest (Detect outlier, fast for real-time)

# Visualization + Storage
- Flag anomalies
- Push to monitoring system
- Storage

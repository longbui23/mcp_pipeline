# Speed Layer Architecture


``` mermaid
Kafka Topics --> Flink streaming --> Influx DB
InfluxDB --> Grafana (Real-time visualization)
InfluxdB --> Expose API for further app development

```
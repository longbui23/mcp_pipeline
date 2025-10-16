#!/bin/bash

# List of topics
TOPICS=(
  "topic_drill_string"
  "topic_mud_logging"
  "topic_bit_data"
  "topic_well_positioning"
  "topic_hydraulics"
  "topic_casing_cementing"
  "topic_formation_eval"
  "topic_surface_equipment"
  "topic_safety_env"
  "topic_filling_storage"
)

# Loop and create each topic
for t in "${TOPICS[@]}"; do
  docker exec -it kafka kafka-topics \
    --create \
    --topic "$t" \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
done

echo "All topics created."

#!/bin/bash

# Prompt for environment
read -p "Enter environment (e.g., prod, dev, test): " ENVIRONMENT
if [ -z "$ENVIRONMENT" ]; then
  echo "Error: Environment cannot be empty"
  exit 1
fi

# Set variables
IMAGE_NAME="quay.io/nwlterry/es-python-alpine:0.3"
CONTAINER_NAME="es-python_temp_container"
CURRENT_DATE=$(date +%Y-%m-%d)
SCRIPTS=(
  "es-index_info_collector.py:es-index_info_collector"
  "es-ilm_policy_analyzer.py:es-ilm_policy_analyzer"
)

# Start the container in detached mode
podman run -d --name "$CONTAINER_NAME" "$IMAGE_NAME" sleep infinity
if [ $? -ne 0 ]; then
  echo "Error: Failed to start container"
  exit 1
fi

# Run each script and copy output files
for script_entry in "${SCRIPTS[@]}"; do
  PYTHON_SCRIPT_NAME="${script_entry%%:*}"
  SCRIPT_NAME="${script_entry##*:}"
  JSON_FILE="/app/${SCRIPT_NAME}_${CURRENT_DATE}.json"
  CSV_FILE="/app/${SCRIPT_NAME}_${CURRENT_DATE}.csv"
  LOCAL_JSON_DEST="./${ENVIRONMENT}-${SCRIPT_NAME}_${CURRENT_DATE}.json"
  LOCAL_CSV_DEST="./${ENVIRONMENT}-${SCRIPT_NAME}_${CURRENT_DATE}.csv"

  # Check if the Python script exists in the container
  podman exec "$CONTAINER_NAME" test -f "/app/$PYTHON_SCRIPT_NAME"
  if [ $? -ne 0 ]; then
    echo "Error: Python script /app/$PYTHON_SCRIPT_NAME not found in container"
    podman rm -f "$CONTAINER_NAME"
    exit 1
  fi

  # Run the Python script interactively with a pseudo-terminal
  echo "Running $PYTHON_SCRIPT_NAME in container..."
  podman exec -it "$CONTAINER_NAME" python "/app/$PYTHON_SCRIPT_NAME"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to run $PYTHON_SCRIPT_NAME"
    podman rm -f "$CONTAINER_NAME"
    exit 1
  fi

  # Copy JSON file from container to local system
  podman cp "$CONTAINER_NAME:$JSON_FILE" "$LOCAL_JSON_DEST"
  if [ $? -eq 0 ]; then
    echo "JSON file copied to $LOCAL_JSON_DEST"
  else
    echo "Error: Failed to copy JSON file $JSON_FILE from container"
  fi

  # Copy CSV file from container to local system
  podman cp "$CONTAINER_NAME:$CSV_FILE" "$LOCAL_CSV_DEST"
  if [ $? -eq 0 ]; then
    echo "CSV file copied to $LOCAL_CSV_DEST"
  else
    echo "Error: Failed to copy CSV file $CSV_FILE from container"
  fi
done

# Remove the container
podman rm -f "$CONTAINER_NAME"
if [ $? -eq 0 ]; then
  echo "Container $CONTAINER_NAME removed"
else
  echo "Error: Failed to remove container"
fi

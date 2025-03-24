#!/bin/bash

# Start Ollama in the background.
/bin/ollama serve &
# Record Process ID.
pid=$!

# Pause for Ollama to start.
sleep 5

# Pull the model
echo "🔴 Retrieving MISTRAL model..."
if ollama pull mistral:7b; then
  echo "🟢 Model retrieved successfully!"
  # Copy models to the desired directory
  #cp -r /models /root/.ollama/models 
# if [ -d "/root/.ollama/models/mistral" ]; then
#    echo "🟢 MISTRAL model is stored inside the container!"
else
  echo "🔴 Failed to retrieve the model. Check model name or network connection."
  exit 1
fi

# Wait for Ollama process to finish.
wait $pid

echo "🟢 Serving Ollama!"

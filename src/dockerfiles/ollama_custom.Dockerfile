# Use the official Ollama image as the base
FROM ollama/ollama:latest

# Set the working directory
WORKDIR /root/.ollama/models

# Copy the models from the host to the container
COPY ./models /root/.ollama/models

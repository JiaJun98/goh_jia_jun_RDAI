# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set working directory
WORKDIR /src

# Add requirements and install dependencies
ADD ./main_app/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container
COPY . /src/

# Expose the Streamlit port
EXPOSE 8501

# Command to run the Streamlit app
CMD ["streamlit", "run", "frontend_app.py"]  # Replace with your app filename
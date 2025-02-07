# Use Python 3.10 as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application files to the container
COPY . /app

COPY templates /app/templates
# Copy the model file into the container
COPY best_lr_model.pkl /app/best_lr_model.pkl

# Install build-essential and other tools for compiling certain Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the PORT environment variable
ENV PORT=8080

# Expose the Flask app's port
EXPOSE 8080

CMD ["python", "-c", "import os; from app import app; app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))"]

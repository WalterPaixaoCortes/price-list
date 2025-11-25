FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install OS deps (keep minimal; add more if your requirements need them)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project files
COPY . /app

# Create lists folder (will be mounted by user if desired)
RUN mkdir -p /app/lists

# Expose port
EXPOSE 8000

# Run uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

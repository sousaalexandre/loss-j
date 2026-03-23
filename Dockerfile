FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Upgrade pip, setuptools, and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    git \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p data_lakehouse/01_bronze \
    && mkdir -p data_lakehouse/02_silver \
    && mkdir -p data_lakehouse/03_gold

# Expose port for Streamlit
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command: run Streamlit app
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
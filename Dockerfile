# Use a lightweight Debian-based Python image
FROM python:3.10-slim

# Prevent interactive prompts during package installations
ENV DEBIAN_FRONTEND=noninteractive

# Install essential compilation tools and Icarus Verilog
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    iverilog \
    procps \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy dependency list first to leverage Docker's caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

# Expose port 10000 for Render's HTTP health checks
EXPOSE 10000

# Fire up the Discord Bot daemon
CMD ["python", "bot.py"]

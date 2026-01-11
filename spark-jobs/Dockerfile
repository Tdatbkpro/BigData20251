FROM bde2020/spark-worker:3.0.1-hadoop3.2

WORKDIR /opt/spark-apps

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set permissions
RUN chmod +x *.sh

# Default command
CMD ["/bin/bash", "-c", "tail -f /dev/null"]
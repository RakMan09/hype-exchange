# Image for the Python tools: bidder bots and the traffic generator.
FROM python:3.12-slim
WORKDIR /app
COPY bots/requirements.txt bots/requirements.txt
RUN pip install --no-cache-dir -r bots/requirements.txt
COPY bots bots
COPY gen gen
# Command is provided by docker-compose (run_bidders.py or firehose.py).

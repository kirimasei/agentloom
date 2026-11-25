FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install --no-cache-dir ".[hub]"

ENV AGENTLOOM_HOST=0.0.0.0 \
    AGENTLOOM_PORT=3000 \
    AGENTLOOM_DATA_DIR=/app/data

EXPOSE 3000

VOLUME ["/app/data"]

CMD ["agentloom", "hub"]

# This project's app image: the agentic-lakehouse engine (requirements.txt)
# plus the project's config and code. Each compose service runs it with a
# different agentic-lakehouse command; config/ is also mounted at runtime, so
# editing it needs a restart, not a rebuild.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN useradd --create-home --uid 1000 app && mkdir -p proposals && chown app proposals
USER app

ENTRYPOINT ["agentic-lakehouse"]

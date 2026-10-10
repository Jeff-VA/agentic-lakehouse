# The platform's own processes: event stream, flush, rollups, drift agent and
# API gateway. Each compose service runs this image with a different run.py
# command. config/ is mounted at runtime, so editing it needs a restart, not a
# rebuild; .env (the drift agent's API key) is read at runtime, never copied in.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# Dependencies first, so code changes rebuild in seconds.
COPY pyproject.toml .
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies']))" \
        > requirements.txt \
    && pip install -r requirements.txt \
    && rm requirements.txt

COPY core core
COPY pipelines pipelines
COPY run.py .
RUN useradd --create-home --uid 1000 app && mkdir proposals && chown app proposals
USER app

ENTRYPOINT ["python", "run.py"]

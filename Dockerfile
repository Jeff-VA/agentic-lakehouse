# The platform's own processes: setup, the demo event stream, flush, rollups,
# drift agent and API gateway. Each compose service runs this image with a
# different agentic-lakehouse command. config/ is mounted at runtime, so editing
# it needs a restart, not a rebuild; .env (the drift agent's API key) is read at
# runtime, never copied in.
#
# A project of its own builds on the released image:
#   FROM ghcr.io/jeff-va/agentic-lakehouse:<version>
#   COPY . /app    # its config/ and its own code
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src

# Dependencies first, so code changes rebuild in seconds.
COPY pyproject.toml .
RUN python -c "import tomllib; print('\\n'.join(tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies']))" \
        > requirements.txt \
    && pip install -r requirements.txt \
    && rm requirements.txt

COPY README.md LICENSE ./
COPY agentic_lakehouse agentic_lakehouse
RUN pip install --no-deps . && cd / && rm -rf /src

# The command runs from the project: its config/ (mounted) and its own code,
# here the example's simulator.
WORKDIR /app
COPY examples examples
RUN useradd --create-home --uid 1000 app && mkdir proposals && chown app proposals
USER app

ENTRYPOINT ["agentic-lakehouse"]

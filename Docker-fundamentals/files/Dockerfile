FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked


# when you are calling this fle from the root directoryuse this 
COPY Module-01/Docker/ingest.py . 

# when you are calling this fle from the Dockerfile directory use this 
# COPY ingest.py . 

ENTRYPOINT ["uv", "run", "python", "ingest.py"]


# When you are calling this file fom the roo directory  use this
#  docker build -t ingest:v -f Module-01/Docker/Dockerfile .
#  docker run --network='host' ingest:v

# When you are calling this file fom the Dockerfile directory use this
# docker build -t ingest:v
FROM python:3.11-slim
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

RUN pip install pandas pyarrow

COPY main.py .


# COPY pyproject.toml uv.lock ./
# RUN uv sync --locked


# when you are calling this fle from the root directoryuse this 
# COPY ingest.py . 

# when you are calling this fle from the Dockerfile directory use this 
# COPY ingest.py . 

# ENTRYPOINT ["uv", "run", "python", "ingest.py"]

CMD ["python", "main.py"]  # added main.py


# When you are calling this file fom the Dockerfile directory use this
# docker build -t ingest:v
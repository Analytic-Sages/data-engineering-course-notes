# Module 01: Containerization and Data Ingestion with Docker

In this module, we build the foundation of our data engineering stack by setting up virtual environments, containerizing our application database, and writing a python ingestion script to fetch and store data.

---

## Core Concepts Covered in this Module

### A. Python Virtual Environments
* Managing Python dependencies isolated from the system Python.
* Using modern package managers (like `uv`) to maintain reproducible build environments.

### B. PostgreSQL in Docker
* Running a production-grade relational database (PostgreSQL) inside a Docker container.
* Connecting to the database from a local host machine using database GUIs like DBeaver.

### C. Containerizing Data Pipelines (Dockerfiles)
* Writing custom Docker images (`Dockerfile`) to package our Python ingestion scripts.
* Ensuring the script runs consistently regardless of the underlying operating system.

### D. Multi-Container Orchestration (Docker Compose)
* Defining and running multi-container applications (our ingestion app and the Postgres database) using a single `docker-compose.yml` file.
* Setting up networking so containers can securely communicate with each other.

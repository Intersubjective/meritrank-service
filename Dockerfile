# Pull base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=on

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        python3-poetry\
        uvicorn \ 
        gcc \
        curl \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Set work directory
WORKDIR /app

# Install dependencies
COPY poetry.lock pyproject.toml /app/
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi --no-dev

# Copy project
COPY . /app

# Expose the application's port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "--factory", "meritrank_service.asgi:create_meritrank_app", "--host", "0.0.0.0", "--port", "8000"]


HEALTHCHECK CMD curl --fail http://localhost:8000/healthcheck || exit 1
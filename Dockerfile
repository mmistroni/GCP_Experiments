# Use an official Python runtime as a parent image
# Choose a Python version that matches your project's needs (e.g., 3.9, 3.10, 3.11, 3.12)
FROM python:3.10-slim-buster

# Set the working directory in the container
WORKDIR /app

# Install system dependencies needed for Python packages (if any, typically build-essential, libpq-dev for psycopg2 etc.)
# You might need to add more depending on your specific Python packages.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry (a Python dependency management and packaging tool)
# We install it globally and then configure it to store virtualenvs in the project directory
# for easier Codespaces integration.
RUN curl -sSL https://install.python-poetry.org | python -
ENV PATH="/root/.poetry/bin:${PATH}"
RUN poetry config virtualenvs.in-project true

# Copy pyproject.toml and poetry.lock (if it exists) to leverage Poetry's caching
# This step helps with faster rebuilds if only source code changes.
COPY pyproject.toml poetry.lock* ./

# Install project dependencies using Poetry
# The '--no-root' flag means Poetry won't install your current project itself,
# which is often desired in a dev container where the code will be mounted.
# The '--no-interaction' flag prevents interactive prompts.
RUN poetry install --no-root --no-interaction --sync

# Copy the rest of your application code into the container
# This is usually done after installing dependencies to make use of Docker layer caching.
COPY . .

# (Optional) If your application runs on a specific port, expose it.
# For example, if you have a web application running on port 8000:
# EXPOSE 8000

# Set a default command to run when the container starts.
# This can be activating the virtual environment, or starting your application.
# For a devcontainer, the postCreateCommand in devcontainer.json often handles activation.
# This CMD is usually overridden by devcontainer.json's "postCreateCommand"
# CMD ["bash"]

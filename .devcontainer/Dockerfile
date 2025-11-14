# .devcontainer/Dockerfile
FROM python:3.11-slim

# Install dependencies for development
RUN apt-get update && apt-get install -y \
    git \    
    nano \
    && rm -rf /var/lib/apt/lists/*

# Install uv (fast package/env manager)
RUN pip install uv

# Define workdir
WORKDIR /workspace

# Copy only dependency descriptors first (for caching)
COPY pyproject.toml uv.lock* ./

# Install into a local venv inside the container
RUN uv sync --frozen --all-extras

# Set the PYTHONPATH
ENV PYTHONPATH=/workspace

# Port to expose
EXPOSE 8070

# We are starting the app with postStartCommand in devcontainer, but let's just add a default shell
CMD ["/bin/bash"]
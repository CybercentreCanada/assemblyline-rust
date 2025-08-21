FROM cccs/assemblyline

# Install Rust and its components
USER root
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libclang-dev \
    libssl-dev \
    libmagic-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

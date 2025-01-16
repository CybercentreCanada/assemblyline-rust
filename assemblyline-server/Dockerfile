FROM rust:1.83.0-bookworm AS builder

# Add more build tools
RUN apt-get update && apt-get install -yy libclang-dev libmagic-dev

# Copy in the source to build
WORKDIR /usr/src/
#COPY --exclude=target ./ ./
COPY ./assemblyline-client/ ./assemblyline-client
COPY ./assemblyline-filestore/ ./assemblyline-filestore
COPY ./assemblyline-markings/ ./assemblyline-markings
COPY ./assemblyline-models/ ./assemblyline-models
COPY ./assemblyline-server/ ./assemblyline-server
COPY ./environment_template/ ./environment_template
COPY ./redis-objects/ ./redis-objects
COPY ./Cargo.toml ./Cargo.lock ./
RUN ls -lah  ./

# Build the executable
RUN cargo build --bin assemblyline-server --release --target-dir /out/
RUN test -f "/out/release/assemblyline-server"

# Start over with an empty image
FROM debian:bookworm-slim

# Get required apt packages
# RUN apt-get update && apt-get install -yy libssl1.1 && rm -rf /var/lib/apt/lists/*
# (Already installed on this base image)
RUN apt-get update && apt-get install -yy ca-certificates libmagic1 && rm -rf /var/lib/apt/lists/*

# lock root
RUN passwd -l root

# add a non root user
RUN useradd -b /home -U -m user
WORKDIR /home/user
USER user

# Copy in the executable for this container
COPY --from=builder /out/release/assemblyline-server /usr/bin/assemblyline-server
RUN ulimit -n 262144
CMD ["/usr/bin/assemblyline-server"]
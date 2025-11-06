FROM rust:1.90.0-bookworm AS builder

# Add more build tools
RUN apt-get update && apt-get install -yy libclang-dev libmagic-dev libpython3-dev

# Copy in the source to build
WORKDIR /usr/src/
COPY ./Cargo.toml ./Cargo.lock ./

COPY ./assemblyline-client ./assemblyline-client
COPY ./assemblyline-filestore ./assemblyline-filestore
COPY ./assemblyline-markings ./assemblyline-markings
COPY ./assemblyline-models ./assemblyline-models
COPY ./assemblyline-server ./assemblyline-server
COPY ./environment_template ./environment_template
COPY ./redis-objects ./redis-objects

# Build the executable
RUN cargo build --bin assemblyline-server --release --target-dir /out/
RUN test -f "/out/release/assemblyline-server"
RUN ls -lah /out 

# Download python packages we need
# When changing the python version it needs to be kept in sync between 
#  - the debian image
#  - pyo3
#  - the python image here
#  - the python package installed with api below
FROM python:3.11 AS pybuilder
RUN pip install --target /packages msoffcrypto-tool


# Start over with an empty image
FROM debian:bookworm-slim

# Get required apt packages
# libssl1.1 is already installed on this base image
RUN apt-get update && apt-get install -yy ca-certificates libmagic1 libpython3.11 && rm -rf /var/lib/apt/lists/*

# copy in python packages from container that downloaded them
COPY --from=pybuilder /packages /usr/local/lib/python3/dist-packages/
ENV PYTHONPATH=/usr/local/lib/python3/dist-packages/

# lock root
RUN passwd -l root

# add a non root user
RUN useradd -b /home -U -m user
WORKDIR /home/user
USER user

# Copy in the executable for this container
COPY --from=builder /out/release/assemblyline-server /usr/bin/assemblyline-server
CMD ["/usr/bin/assemblyline-server"]
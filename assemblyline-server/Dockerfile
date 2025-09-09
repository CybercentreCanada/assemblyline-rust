FROM rust:1.89.0-bookworm AS builder

# Add more build tools
RUN apt-get update && apt-get install -yy libclang-dev libmagic-dev libpython3-dev

# Copy in the source to build
WORKDIR /usr/src/
COPY ./Cargo.toml ./Cargo.lock ./

# COPY ./assemblyline-client/Cargo.toml ./assemblyline-client/Cargo.toml
# COPY ./assemblyline-filestore/Cargo.toml ./assemblyline-filestore/Cargo.toml
# COPY ./assemblyline-markings/Cargo.toml ./assemblyline-markings/Cargo.toml
# COPY ./assemblyline-models/Cargo.toml ./assemblyline-models/Cargo.toml
# COPY ./assemblyline-server/Cargo.toml ./assemblyline-server/Cargo.toml
# COPY ./environment_template/Cargo.toml ./environment_template/Cargo.toml
# COPY ./redis-objects/Cargo.toml ./redis-objects/Cargo.toml

# RUN mkdir ./assemblyline-client/src; touch assemblyline-client/src/lib.rs
# RUN mkdir ./assemblyline-filestore/src; touch assemblyline-filestore/src/lib.rs
# RUN mkdir ./assemblyline-markings/src; touch assemblyline-markings/src/lib.rs
# RUN mkdir ./assemblyline-models/src; touch assemblyline-models/src/lib.rs
# RUN mkdir ./assemblyline-server/src; touch assemblyline-server/src/lib.rs
# RUN mkdir ./environment_template/src; touch environment_template/src/lib.rs
# RUN mkdir ./redis-objects/src; touch redis-objects/src/lib.rs

# RUN cargo build --release --target-dir /out/

# RUN rm -rf ./assemblyline-client
# RUN rm -rf ./assemblyline-filestore
# RUN rm -rf ./assemblyline-markings
# RUN rm -rf ./assemblyline-models
# RUN rm -rf ./assemblyline-server
# RUN rm -rf ./environment_template
# RUN rm -rf ./redis-objects

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
RUN ulimit -n 262144
CMD ["/usr/bin/assemblyline-server"]
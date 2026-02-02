FROM python:3.11 AS pybuilder

RUN pip install --target /packages msoffcrypto-tool


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

# copy in python packages we will want
COPY --from=pybuilder /packages /usr/local/lib/python3/dist-packages/
ENV PYTHONPATH=/usr/local/lib/python3/dist-packages/

# Build the executable
RUN cargo build --target-dir /out
RUN cargo build --release --target-dir /out

# Remove the code so we don't accidentally use it again
RUN rm Cargo.toml Cargo.lock
RUN rm -rf ./assemblyline-client
RUN rm -rf ./assemblyline-filestore
RUN rm -rf ./assemblyline-markings
RUN rm -rf ./assemblyline-models
RUN rm -rf ./assemblyline-server
RUN rm -rf ./environment_template
RUN rm -rf ./redis-objects

CMD ["cargo"]
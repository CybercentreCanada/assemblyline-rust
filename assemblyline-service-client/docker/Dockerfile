ARG rust_build_image=rust:1.90.0-bookworm
ARG branch=latest
ARG base=cccs/assemblyline
ARG version

FROM ${rust_build_image} AS builder-rust

# Add more build tools
RUN apt-get update && apt-get install -yy libclang-dev libmagic-dev libpython3-dev

# Copy in the source to build
WORKDIR /usr/src/
COPY ./assemblyline-service-client/docker/Cargo.toml ./Cargo.lock ./

COPY ./assemblyline-filestore ./assemblyline-filestore
COPY ./assemblyline-markings ./assemblyline-markings
COPY ./assemblyline-models ./assemblyline-models
COPY ./assemblyline-service-client ./assemblyline-service-client
COPY ./redis-objects ./redis-objects

# Build the executable
RUN cargo build --bin assemblyline-service-client --release --target-dir /out/
RUN test -f "/out/release/assemblyline-service-client"
RUN ls -lah /out


# Download python packages we need for assemblyline-service-base
FROM ${base}:${branch} AS builder-python

USER root
RUN apt-get update \
    && apt-get install -yy build-essential libfuzzy-dev \
    && rm -rf /var/lib/apt/lists/*

# install pip packages, as assemblyline so they go into our .local with the ones already there
USER assemblyline
RUN touch /tmp/before-pip

COPY dist* dist/
RUN pip install --no-cache-dir -f dist --user \
    assemblyline-core==$version \
    assemblyline-v4-service[updater]==$version \
    && rm -rf ~/.cache/pip

# Remove files that existed before the pip install so that our copy command below doesn't take a snapshot of
# files that already exist in the base image
RUN find /var/lib/assemblyline/.local -type f ! -newer /tmp/before-pip -delete

# Switch back to root and change the ownership of the files to be copied due to bitbucket pipeline uid nonsense
USER root
RUN chown root:root -R /var/lib/assemblyline/.local


# Restart a new image, this time the output one
FROM $base:$branch

# Get the updated local dir from builder
COPY --chown=assemblyline:assemblyline --from=builder-python /var/lib/assemblyline/.local /var/lib/assemblyline/.local

# Setup environment varibles
ENV PYTHONPATH=/opt/al_service
ENV SERVICE_API_HOST=http://al_service_server:5003
ENV SERVICE_API_KEY=ThisIsARandomAuthKey...ChangeMe!
ENV CONTAINER_MODE=true

USER root
# Install git binary for gitpython pip package
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Create directory for service
RUN mkdir -p /opt/al_service
RUN touch /opt/al_service/__init__.py
# COPY docker/process_handler.py /etc/

# Create directory for service updates
RUN mkdir /updates
RUN chown -R assemblyline:assemblyline /updates

USER assemblyline

# Copy in the executable for this container
COPY --from=builder-rust /out/release/assemblyline-service-client /usr/bin/assemblyline-service-client
CMD ["/usr/bin/assemblyline-service-client"]

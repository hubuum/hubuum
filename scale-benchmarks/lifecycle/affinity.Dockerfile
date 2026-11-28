ARG HUBUUM_IMAGE
FROM ${HUBUUM_IMAGE}

# Measurement-only derivative: preserve the production binaries and entrypoint.
USER root
RUN apk add --no-cache util-linux
USER hubuum:hubuum

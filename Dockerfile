FROM ubuntu:24.04

# Meta
LABEL \
  "name"="ufc-ripper" \
  "maintainer"="Mahesh Bandara Wijerathna <m4heshd@gmail.com> (m4heshd)"

# Init
WORKDIR /ufcr

# Environment variables
ENV RUN_ENV=container

# Setup app
COPY ./package/linux/ .
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /ufcr/bin \
    && ln -sfn /usr/bin/ffmpeg /ufcr/bin/ffmpeg \
    && ln -sfn /usr/bin/ffprobe /ufcr/bin/ffprobe \
    && chmod +x ./ufc-ripper

# Ports
EXPOSE 8383

# Volumes
VOLUME ["/ufcr/config"]
VOLUME ["/downloads"]

# Start
CMD ["./ufc-ripper"]

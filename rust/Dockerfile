# Purpose: Creates the image for running the SIS updater.
#podman build -t sis_poller:latest -f Dockerfile
FROM alpine:latest

RUN apk update &&\
    apk --no-cache git &&\
    apk --no-cache rust cargo

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser
WORKDIR /home/appuser

RUN cd /home/appusuer &&\
    git clone https://github.com/uofuseismo/sisPoller.git &&\
    cd sisPoller/rust &&\
    cargo build --release --target-dir=/home/appuser/

WORKDIR /home/appuser/release
#RUN RUST_LOG=info ./sisPoller


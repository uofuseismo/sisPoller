# Purpose: Creates the image for running the SIS updater.
#podman build -t sispoller:latest -f Dockerfile
FROM alpine:latest as sispoller

RUN apk update &&\
    apk add --no-cache pkgconfig &&\
    apk add --no-cache git &&\
    apk add --no-cache rust cargo &&\
    apk add openssl &&\
    apk add --no-cache openssl-dev

RUN mkdir -p /home/appuser &&\
    addgroup -S appgroup &&\
    adduser -S appuser -G appgroup
USER appuser
WORKDIR /home/appuser

RUN cd /home/appuser &&\
    git clone https://github.com/uofuseismo/sisPoller.git &&\
    cd sisPoller/rust &&\
    cargo build --release --target-dir=/home/appuser/ &&\
    cd /home/appuser &&\
    rm -rf sisPoller &&\
    rm -r build &&\
    rm -r deps &&\
    rm -r examples &&\
    rm -r incremental &&\
    rm sisPoller.d

WORKDIR /home/appuser/release
#RUN RUST_LOG=info ./sisPoller
#RUN RUST_LOG=info ./sisPoller


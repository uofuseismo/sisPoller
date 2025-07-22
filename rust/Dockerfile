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
    mkdir -p bin &&\
    git clone https://github.com/uofuseismo/sisPoller.git &&\
    cd sisPoller/rust &&\
    cargo build --release --target-dir=./production &&\
    cp production/release/sisPoller /home/appuser/bin &&\
    cd /home/appuser/ &&\
    rm -rf sisPoller
    
ENV PATH="$PATH:/home/appuser/bin"
WORKDIR /home/appuser/
#RUN RUST_LOG=info ./sisPoller
#RUN RUST_LOG=info ./sisPoller


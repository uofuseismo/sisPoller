# Purpose: Creates the image for running the SIS updater.
# Usage: buildah build -t sispoller:latest -f Dockerfile
FROM alpine:latest as sispoller

RUN apk update &&\
    apk add --no-cache pkgconfig &&\
    apk add --no-cache git &&\
    apk add --no-cache cargo &&\
    apk add --no-cache openssl-dev &&\
    apk add openssl

#RUN apk add musl-dev &&\
#    apk add openssl &&\
#    apk add sqlite-libs &&\
#    apk add sqlite

# Create a user
RUN addgroup -S appgroup &&\
    adduser -S appuser --home /home/appuser --shell /bin/bash --disabled-password --gecos -G appgroup

#RUN mkdir -p /home/appuser &&\
#    addgroup -S appgroup &&\
#    adduser -S appuser -G appgroup

USER appuser
WORKDIR /home/appuser

RUN cd /home/appuser &&\
    mkdir -p bin &&\
    git clone https://github.com/uofuseismo/sisPoller.git &&\
    cd sisPoller/rust &&\
    RUST_LOG=info cargo build --release &&\
    cp target/release/sis_poller /home/appuser/bin &&\
    cd /home/appuser/ &&\
    rm -rf sisPoller
    
ENV PATH="$PATH:/home/appuser/bin"
WORKDIR /home/appuser/
#RUN RUST_LOG=info ./sisPoller
#RUN RUST_LOG=info ./sisPoller
#ENTRYPOINT


# syntax=docker/dockerfile:1
# Multi-backend container: Envoy front proxy + controller + Python/Go/Java/Rust sample backends.

ARG ENVOY_IMAGE=docker.io/envoyproxy/envoy:v1.40.2

FROM docker.io/library/golang:1.25-bookworm AS go-build
WORKDIR /src
COPY controller/go.mod controller/go.sum ./controller/
RUN cd controller && go mod download
COPY controller ./controller
COPY backends/go ./backends/go
RUN cd controller && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/controller . \
 && cd ../backends/go && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/go-backend .

FROM docker.io/library/rust:1.91-bookworm AS rust-build
WORKDIR /src
COPY backends/rust/Cargo.toml backends/rust/Cargo.lock ./
# cache dependencies separately from the source
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src target/release/deps/backend*
COPY backends/rust/src ./src
RUN cargo build --release && cp target/release/backend /out-backend

FROM docker.io/library/eclipse-temurin:21-jdk-noble AS java-build
WORKDIR /src
COPY backends/java/Server.java .
RUN javac --release 21 -d /out Server.java

FROM ${ENVOY_IMAGE} AS envoy

FROM docker.io/library/ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 openjdk-21-jre-headless tini ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=envoy /usr/local/bin/envoy /usr/local/bin/envoy
COPY --from=go-build /out/controller /usr/local/bin/mb-controller
COPY --from=go-build /out/go-backend /opt/mb/backends/go/backend
COPY --from=rust-build /out-backend /opt/mb/backends/rust/backend
COPY --from=java-build /out /opt/mb/backends/java
COPY backends/python/server.py /opt/mb/backends/python/server.py
COPY config/config.yaml /etc/multi-backend/config.yaml
RUN mkdir -p /var/lib/multi-backend /var/log/multi-backend

# 8080 = proxy (Envoy listener), 9000 = control API
EXPOSE 8080 9000
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s CMD curl -fsS http://127.0.0.1:9000/healthz || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/mb-controller"]

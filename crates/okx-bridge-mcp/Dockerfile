FROM rust:1.95-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p okx-bridge-mcp

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 okxbridge
COPY --from=build /src/target/release/okx-bridge-mcp /usr/local/bin/okx-bridge-mcp
USER 10001
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/okx-bridge-mcp"]

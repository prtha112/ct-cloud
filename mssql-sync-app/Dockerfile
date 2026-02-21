FROM rust:1.93.1 as builder
WORKDIR /usr/src/mssql-sync-app
COPY . .
RUN cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/mssql-sync-app /usr/local/bin/mssql-sync-app
CMD ["mssql-sync-app"]

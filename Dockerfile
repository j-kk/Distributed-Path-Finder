FROM rust:latest AS builder

RUN update-ca-certificates

# Create appuser
ENV USER=pathfinder
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"


WORKDIR /pathfinder


COPY Cargo.toml .
COPY Cargo.lock .

COPY src ./src

# We no longer need to use the x86_64-unknown-linux-musl target
RUN cargo clean && \
    cargo build --release

####################################################################################################
## Final image
####################################################################################################
FROM debian:bullseye-slim

RUN apt update
RUN apt install -y libssl-dev
RUN apt install -y ca-certificates

# Import from builder.
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

WORKDIR /pathfinder

# Copy our build
COPY --from=builder /pathfinder/target/release/pathfinder ./

# Use an unprivileged user.
USER pathfinder:pathfinder

CMD ["/pathfinder/pathfinder"]
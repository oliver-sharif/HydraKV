# Stage 1: Build the application
FROM docker.io/library/golang:1.26.0-alpine AS builder

# disable telemetry
ENV GOTELEMETRY=off

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      apk add --no-cache clang musl-dev linux-headers; \
    else \
      apk add --no-cache gcc musl-dev linux-headers; \
    fi
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# CGO_ENABLED=1 is required for the xxhash implementation
# -ldflags="-w -s" reduces binary size
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then export CC=clang; fi; \
    CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s -extldflags '-static'" -o hydrakv main.go

# Stage 2: Create the final image
FROM gcr.io/distroless/static-debian12

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/hydrakv .

# Copy HTML templates needed at runtime
COPY --from=builder /app/server/templates ./server/templates

# Expose the port
EXPOSE 9191

# Run the application
ENTRYPOINT ["/app/hydrakv"]
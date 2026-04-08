# Build the manager binary
# TODO(supply-chain): Pin base images to digest for reproducible builds.
# Example: FROM golang:1.25-bookworm@sha256:<digest>
FROM golang:1.26-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Install dependencies separately to leverage Docker layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source tree.
COPY . .

# Build the controller manager binary.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
# TODO(supply-chain): Pin base images to digest for reproducible builds.
# Example: FROM gcr.io/distroless/static:nonroot@sha256:<digest>
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager /manager
USER 65532:65532

ENTRYPOINT ["/manager"]

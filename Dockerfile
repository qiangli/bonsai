## Bonsai — single-stage build, distroless runtime.
##
## Build:    docker build -t bonsai .
## Run:      docker run -p 8080:8080 -p 9080:9080 -v $(pwd)/data:/data bonsai

FROM golang:1.26 AS build
ARG VERSION=docker
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/bonsai ./cmd/bonsai

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/bonsai /bonsai
EXPOSE 8080 9080
ENTRYPOINT ["/bonsai"]
CMD ["server", "--dir", "/data", "--http", ":8080", "--grpc", ":9080"]

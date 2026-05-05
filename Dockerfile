## dgraph2 — single-stage build, distroless runtime.
##
## Build:    docker build -t dgraph2 .
## Run:      docker run -p 8080:8080 -p 9080:9080 -v $(pwd)/data:/data dgraph2

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dgraph2-server ./cmd/dgraph2-server

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/dgraph2-server /dgraph2-server
EXPOSE 8080 9080
ENTRYPOINT ["/dgraph2-server"]
CMD ["--dir", "/data", "--http", ":8080", "--grpc", ":9080"]

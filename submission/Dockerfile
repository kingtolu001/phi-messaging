FROM golang:1.26.2-bookworm AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS tests
CMD ["go", "test", "-race", "-count=1", "-timeout=180s", "-v", "./..."]

FROM source AS build
RUN CGO_ENABLED=0 go build -trimpath -o /server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot AS app
COPY --from=build /server /server
ENTRYPOINT ["/server"]

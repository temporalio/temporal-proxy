FROM alpine:3.23.4 AS base
RUN adduser -D -u 1000 proxy

FROM scratch
COPY --from=base /etc/passwd /etc/passwd
COPY --from=base /etc/group /etc/group

# Copy the pre-built binary (goreleaser will provide this).
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/proxy /usr/local/bin/

USER proxy
VOLUME ["/etc/proxy"]

ENTRYPOINT ["proxy"]
CMD ["--help"]

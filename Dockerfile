FROM alpine:3.24.1 AS base
RUN adduser -D -u 1000 proxy && mkdir -p /rootfs/tmp

FROM scratch
COPY --from=base /etc/passwd /etc/passwd
COPY --from=base /etc/group /etc/group
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=base --chmod=1777 /rootfs/tmp /tmp

# Copy the pre-built binary (goreleaser will provide this).
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/proxy /usr/local/bin/

USER proxy
VOLUME ["/etc/proxy"]

ENTRYPOINT ["proxy"]
CMD ["--help"]

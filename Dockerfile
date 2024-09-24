FROM alpine:latest AS prep
RUN apk --update add ca-certificates
RUN apk --no-cache add tzdata

FROM alpine:latest

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=prep /usr/share/zoneinfo /usr/share/zoneinfo

COPY ./bin/jaeger-doris /jaegerdoris
COPY config.yaml /etc/jaeger/config.yaml

EXPOSE 17271
ENTRYPOINT ["/jaegerdoris"]
CMD ["--config", "/etc/jaeger/config.yaml"]

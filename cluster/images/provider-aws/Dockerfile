FROM BASEIMAGE
RUN apk --no-cache add ca-certificates bash

ARG ARCH
ARG TINI_VERSION

ADD stack /usr/local/bin/crossplane-aws-stack

COPY stack-package /

EXPOSE 8080
ENTRYPOINT ["crossplane-aws-stack"]

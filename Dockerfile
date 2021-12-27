FROM 201341/elasticell-dev

RUN mkdir -p /apps/deepfabric

COPY ./ /go/src/github.com/201341/elasticell

RUN cd /go/src/github.com/201341/elasticell \
    && go test -v ./...

WORKDIR /apps/deepfabric
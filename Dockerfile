FROM golang:latest AS reclaimerbuild
RUN git clone https://github.com/quantifyearth/reclaimer.git
WORKDIR /go/reclaimer
RUN go mod tidy
RUN go build

FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.0

RUN apt-get update -qqy && \
	apt-get install -qy \
		git \
		python3-pip \
		libpq-dev \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

COPY --from=reclaimerbuild /go/reclaimer/reclaimer /bin/reclaimer

RUN rm /usr/lib/python3.*/EXTERNALLY-MANAGED
RUN pip install gdal[numpy]==3.10.0

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY ./ /root/star
WORKDIR /root/star
RUN chmod 755 ./scripts/run.sh

# We create a DATADIR - this should be mapped at container creation
# time to a volume somewhere else
ENV DATADIR=/data
RUN mkdir -p /data

# This is because outside of Docker we want to ensure
# the Python virtualenv is set, but in Docker we don't
# use a virtualenv, as docker *is* a virtualenv
ENV VIRTUAL_ENV=/

RUN python3 -m pytest ./tests
RUN python3 -m pylint prepare_layers prepare_species utils tests

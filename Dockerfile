FROM golang:latest AS reclaimerbuild
RUN git clone https://github.com/quantifyearth/reclaimer.git
WORKDIR /go/reclaimer
RUN go mod tidy
RUN go build

FROM golang:latest AS littlejohnbuild
RUN git clone https://github.com/quantifyearth/littlejohn.git
WORKDIR /go/littlejohn
RUN go mod tidy
RUN go build

FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.4

RUN apt-get update -qqy && \
	apt-get install -qy \
		git \
		cmake \
		python3-pip \
		shellcheck \
		r-base \
		libpq-dev \
		libtirpc-dev \
	&& rm -rf /var/lib/apt/lists/* \
	&& rm -rf /var/cache/apt/*

COPY --from=reclaimerbuild /go/reclaimer/reclaimer /bin/reclaimer
COPY --from=littlejohnbuild /go/littlejohn/littlejohn /bin/littlejohn

RUN rm /usr/lib/python3.*/EXTERNALLY-MANAGED
RUN pip install gdal[numpy]==3.11.4

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN mkdir /root/R
ENV R_LIBS_USER=/root/R
RUN Rscript -e 'install.packages(c("lme4","lmerTest","emmeans"), repos="https://cloud.r-project.org")'

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
ENV VIRTUAL_ENV=/usr
ENV PYTHONPATH=/root/star

RUN python3 -m pytest ./tests
RUN python3 -m pylint prepare_layers prepare_species utils tests
RUN python3 -m mypy prepare_layers prepare_species utils tests
RUN shellcheck ./scripts/run.sh

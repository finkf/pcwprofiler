FROM golang:latest
MAINTAINER Florian Fink
ENV GITURL="https://github.com/cisocrgroup"

RUN apt-get update \
	&& apt-get install -y cmake libxerces-c-dev libcppunit-dev locales \
	&& sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
    && dpkg-reconfigure --frontend=noninteractive locales \
	&& update-locale LANG=en_US.UTF-8

# install the profiler
RUN	git clone ${GITURL}/Profiler --branch devel --single-branch /tmp/profiler \
	&& cd /tmp/profiler \
	&& mkdir build \
	&& cd build \
	&& cmake -DCMAKE_BUILD_TYPE=release .. \
	&& make compileFBDic trainFrequencyList profiler \
	&& mkdir -p /apps \
	&& cp bin/compileFBDic bin/trainFrequencyList bin/profiler /apps/ \
	&& cd / \
    && rm -rf /tmp/profiler

ENV LANG="en_US.UTF-8"
# install the profiler's language backend
RUN	git clone ${GITURL}/Resources --branch master --single-branch /tmp/resources \
	&& cd /tmp/resources/lexica \
	&& make FBDIC=/apps/compileFBDic TRAIN=/apps/trainFrequencyList \
	&& mkdir -p /language-data \
	&& cp -r german latin greek german.ini latin.ini greek.ini /language-data \
	&& cd / \
	&& rm -rf /tmp/resources

COPY pcwprofiler /go/bin/
CMD pcwprofiler \
	-dsn "${MYSQL_USER}:${MYSQL_PASSWORD}@(db)/${MYSQL_DATABASE}" \
	-profiler /apps/profiler \
	-language-dir /language-data \
	-project-dir /project-data \
    -debug

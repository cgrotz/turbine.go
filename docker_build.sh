#!/bin/sh -e
# This script builds a statically linked turbined executable and builds a docker container image containing the ui and the excutable
# The container image is then exported into a tar file
CGO_ENABLED=0 GOOS=linux go build -a -x -installsuffix cgo -tags netgo -ldflags '-w' .
docker build -t cgrotz/turbine .
docker save -o turbine.tar cgrotz/turbine

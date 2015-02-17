FROM scratch

MAINTAINER Christoph Grotz <christoph.grotz@gmail.com>

ADD ./ui/build /ui/build
ADD turbined turbined

EXPOSE  3000

CMD ["/turbined", "run"]

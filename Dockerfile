FROM busybox
MAINTAINER Christoph Grotz <christoph.grotz@gmail.com>

ADD ui/build ./ui
ADD turbined .

EXPOSE  3000

CMD ["turbined", "run"]

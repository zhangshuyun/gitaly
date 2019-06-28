
#
# This will create a docker image for Gitaly that is suitable for testing, but
# is not expected to be used in a production environment, yet.
#
# See the _support/load-cluster/docker-compose.yml for an example of how to use
# this image
#
FROM registry.gitlab.com/gitlab-org/gitlab-build-images:ruby-2.6-golang-1.12-git-2.21

RUN mkdir -p /app/ruby

RUN git clone https://gitlab.com/gitlab-org/gitlab-shell.git /app/gitlab-shell

COPY ./ruby/Gemfile /app/ruby/
COPY ./ruby/Gemfile.lock /app/ruby/

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -qq && \
    apt-get install -qq -y rubygems bundler cmake build-essential libicu-dev && \
    cd /app/ruby && bundle install --path vendor/bundle && \
    rm -rf /var/lib/apt/lists/*

COPY . /app

RUN mkdir /app/bin
RUN rm /app/_build/makegen

RUN cd /app && make _build/makegen

RUN cd /app && make PREFIX=/app/bin

CMD ["/app/bin/gitaly", "/app/config.toml"]


FROM ubuntu

RUN apt-get update
RUN apt-get install wget -y
RUN apt-get install gnupg2 -y
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update
RUN apt-get -y install postgresql

EXPOSE 5920
ENTRYPOINT postgres -D /usr/local/pgsql/data
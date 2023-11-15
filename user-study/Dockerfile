FROM python:3.9

ARG DATA

RUN pip install Django psycopg2

WORKDIR /home/user_study
RUN mkdir -p /home/data
RUN mkdir -p /home/${DATA}
ADD ${DATA} /home/${DATA}
ADD params.json .
ADD user_study/ .

ENTRYPOINT if [ ! -f /home/data/db.sqlite3 ] ; then python manage.py makemigrations web ; python manage.py migrate ; fi && python manage.py runserver 0.0.0.0:8000

#!/bin/bash

set -e

if [ ! -d /home/data ]
then
    mkdir /home/data
    python manage.py makemigrations web
    python manage.py migrate
fi

python manage.py runserver 0.0.0.0:8000
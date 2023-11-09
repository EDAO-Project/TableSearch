#!/bin/bash

set -e

if [ ! -f /home/data/db.sqlite3 ]
then
    python manage.py makemigrations web
    python manage.py migrate
fi

python manage.py runserver 0.0.0.0:8000
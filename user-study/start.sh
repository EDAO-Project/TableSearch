#!/bin/bash

set -e

python manage.py makemigrations web
python manage.py migrate
python manage.py runserver
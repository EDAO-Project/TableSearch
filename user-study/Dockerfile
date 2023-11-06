FROM python:3.9

WORKDIR /user_study
RUN mkdir data/
ADD params.json .
ADD user_study/ .
ADD requirements.txt .

RUN pip install -r requirements.txt
RUN pip install Django

RUN python manage.py makemigrations web
RUN python manage.py migrate

ENTRYPOINT [ "python", "manage.py", "runserver" ]
FROM python:3.9

WORKDIR /home/user_study
RUN mkdir /home/data/
ADD params.json .
ADD user_study/ .
ADD requirements.txt .

RUN pip install -r requirements.txt
RUN pip install Django

ENTRYPOINT [ "run.sh" ]
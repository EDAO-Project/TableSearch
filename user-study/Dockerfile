FROM python:3.9

WORKDIR /home/user_study
ADD params.json .
ADD user_study/ .
ADD requirements.txt .
ADD run.sh .

RUN pip install -r requirements.txt
RUN pip install Django

ENTRYPOINT [ "run.sh" ]
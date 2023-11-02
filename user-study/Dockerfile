FROM python:3.9

WORKDIR /user_study
ADD start.sh .
ADD params.json .
ADD user_study/ .
ADD requirements.txt .

RUN pip install -r requirements.txt
RUN pip install Django

ENTRYPOINT [ "./start.sh" ]
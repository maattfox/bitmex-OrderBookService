FROM python:3.6

ADD ../requirements.txt /app/
WORKDIR /bitmexOrderBook

RUN pip install -r requirements.txt

ADD app.py /

CMD["python", "./app.py"]


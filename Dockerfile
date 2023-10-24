FROM python:3.9-slim-buster
WORKDIR /app
COPY . /app
RUN pip install -r /app/requirements.txt
ENV CONF prod
CMD python3 /app/run.py --config=$CONF
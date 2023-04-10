FROM python:3.8

WORKDIR /usr/app/src
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Football.py ./
COPY Snowflake.py ./
COPY main.py ./
COPY /sql/football_copy.sql ./sql/
COPY /sql/dim_football_load.sql ./sql/

EXPOSE 8080
FROM python:2.7
WORKDIR /app
RUN groupadd --gid 1001 app && useradd -g app --uid 1001 --shell /usr/sbin/nologin app
RUN apt-get update && apt-get install -y --no-install-recommends java
COPY ./requirements.txt /app/requirements.txt
RUN pip install -U 'pip>=8' && pip install --upgrade --no-cache-dir -r requirements.txt
COPY . /app
RUN chown app:app kcl.properties
USER app
ENV CONNECTOR=file_buffer \
    REGION_NAME=us-east-1 \
    STREAM_NAME=mystream \
    APPLICATION_NAME=mystream-group1 \
    FILE_BUFFER=/dev/stdout
CMD run.sh

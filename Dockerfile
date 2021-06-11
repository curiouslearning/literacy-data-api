# syntax=docker/dockerfile:1
FROM node:14
RUN apt-get update
RUN apt-get install -y memcached
RUN apt-get install dos2unix
WORKDIR /app
COPY package.json .
run npm install
COPY . .
RUN chmod +x ./startup.sh
RUN dos2unix ./startup.sh
CMD ["./startup.sh"]

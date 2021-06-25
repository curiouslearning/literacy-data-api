# syntax=docker/dockerfile:1
FROM node:14
WORKDIR /app
COPY package.json npm-shrinkwrap.json .
run npm install
COPY . .
CMD ["node", "src/index.js"]

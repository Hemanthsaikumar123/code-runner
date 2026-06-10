FROM node:22-alpine

RUN apk add --no-cache python3

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
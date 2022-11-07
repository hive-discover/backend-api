FROM node:16.15-buster-slim

RUN apt-get update && apt-get install -y python3 g++ make

WORKDIR /NodeJS
COPY package.json .
RUN npm install --production --omit=dev
RUN npm prune --production --omit=dev

# Solve the problem by reinstaling bcrypt
RUN npm uninstall bcrypt
RUN npm i bcrypt

#HEALTHCHECK --interval=3s --timeout=5s --start-period=120s \
#  CMD curl -f http://localhost:3000/ || exit 1

ENV NODE_ENV production

COPY . .
ENTRYPOINT [ "node", "server.js" ]

# registry.hive-discover.tech/api-v1:0.1.9.7
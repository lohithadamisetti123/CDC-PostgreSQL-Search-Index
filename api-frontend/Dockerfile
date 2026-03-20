FROM node:20-slim

WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Only copy package.json (no package-lock.json needed)
COPY package.json ./
RUN npm install

COPY . .

CMD ["node", "server.js"]

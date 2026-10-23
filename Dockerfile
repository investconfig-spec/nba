FROM node:22-slim
ENV NODE_ENV=production
WORKDIR /app

# Code + single-file frontend
COPY server.js nba.html ./

# Seeded staging cache (rewritten by server.js at runtime)
COPY nba_staging.db ./

EXPOSE 8092
CMD ["node", "server.js"]

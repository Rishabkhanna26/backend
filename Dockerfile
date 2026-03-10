FROM node:22-bookworm-slim

# WhatsApp agent needs a real browser. Install Chromium + runtime deps.
# This avoids relying on build-step apt installs that may not persist to runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    ca-certificates \
    fonts-liberation \
    libglib2.0-0 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxss1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for better Docker layer caching.
COPY package.json package-lock.json ./

# Puppeteer (transitive via whatsapp-web.js) tries to download its own Chrome at install time.
# We're using system Chromium instead.
ENV PUPPETEER_SKIP_DOWNLOAD=true
RUN npm ci --omit=dev

COPY . .

ENV NODE_ENV=production
# Railway usually routes to 8080; you can override via service env vars.
ENV PORT=8080
# Hint to whatsapp-web.js / puppeteer to use system Chromium.
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

EXPOSE 8080

CMD ["npm", "run", "start:whatsapp"]


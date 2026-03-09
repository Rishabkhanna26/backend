# AlgoChat Backend

This folder contains:

- Next.js API routes under `app/api`
- WhatsApp and realtime runtime under `src/server.js`

## Run API

```bash
npm install
npm run dev:api
```

API runs on `http://localhost:5000`.

## Run WhatsApp server

```bash
npm run dev:whatsapp
```

WhatsApp and realtime server run on `http://localhost:5001` when `PORT=5001` is set in `.env`.

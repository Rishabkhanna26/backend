# Backend Guide

This backend is now organized so each concern is easier to find and read.

## Quick Map
- `backend/src/server.js`: API/Socket bootstrap, route wiring, server startup/shutdown.
- `backend/src/server/runtime-config.js`: all server runtime/env config and origin helpers.
- `backend/src/server/backend-auth.js`: backend auth parsing + middleware helpers.
- `backend/src/server/payment-link-timers.js`: scheduled payment-link processing logic.
- `backend/src/whatsapp.js`: WhatsApp session lifecycle + automation flow.
- `backend/config/`: logger, DB pool config, Sentry setup.
- `backend/db/`: schema init and seed scripts.
- `backend/migrations/`: SQL migrations.
- `backend/scripts/`: migration runner/test scripts.

## Request Flow (HTTP)
1. `server.js` initializes security middleware and rate limits.
2. `requireBackendAuth` (from `backend-auth.js`) validates backend tokens.
3. Route handlers call domain functions (`whatsapp.js`, `lib/db-helpers.js`, etc).
4. Errors are handled by Sentry middleware, then 404/global handlers.

## WhatsApp + Socket Flow
1. Socket auth is validated in `server.js`.
2. Clients join `admin:<id>` rooms.
3. `whatsappEvents` from `whatsapp.js` are forwarded to socket rooms.
4. `/whatsapp/*` routes call `startWhatsApp`, `stopWhatsApp`, `sendAdminMessage`.

## Scheduled Payment Link Flow
- Timer service lives in `payment-link-timers.js`.
- `server.js` only starts/stops it and injects dependencies.
- Logic and side effects are centralized in one file to keep `server.js` readable.

## Safe Editing Rules
- For auth changes: edit `backend-auth.js` first.
- For env/config changes: edit `runtime-config.js`.
- For scheduled-payment behavior: edit `payment-link-timers.js`.
- For API wiring only: edit `server.js`.

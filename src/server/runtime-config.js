export const DEFAULT_PORT = 3001;
export const BASE_PORT = Number(process.env.PORT) || DEFAULT_PORT;

export const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "http://localhost:3000";
export const FRONTEND_ORIGINS = new Set(
  (process.env.FRONTEND_ORIGINS || `${FRONTEND_ORIGIN},http://localhost:3001`)
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean)
);

export const BACKEND_SCOPE = "backend";

export const PAYMENT_LINK_TIMER_ENABLED =
  String(process.env.PAYMENT_LINK_TIMER_ENABLED || "true").toLowerCase() !== "false";
export const PAYMENT_LINK_TIMER_POLL_MS = Math.min(
  Math.max(Number(process.env.PAYMENT_LINK_TIMER_POLL_SECONDS || 30) * 1000, 5000),
  5 * 60 * 1000
);
export const PAYMENT_LINK_TIMER_BATCH_SIZE = Math.min(
  Math.max(Number(process.env.PAYMENT_LINK_TIMER_BATCH_SIZE || 10), 1),
  100
);
export const PAYMENT_LINK_TIMER_RETRY_MINUTES = Math.min(
  Math.max(Number(process.env.PAYMENT_LINK_TIMER_RETRY_MINUTES || 10), 1),
  24 * 60
);

export const isLocalhostOrigin = (origin) =>
  /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin);

export const resolveOrigin = (origin) => {
  if (!origin) return FRONTEND_ORIGIN;
  if (FRONTEND_ORIGINS.has(origin) || isLocalhostOrigin(origin)) return origin;
  return FRONTEND_ORIGIN;
};

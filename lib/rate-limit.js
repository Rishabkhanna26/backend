const limits = new Map();
const MAX_TRACKED_KEYS = 5000;

const now = () => Date.now();

const pruneExpired = (currentTime) => {
  for (const [key, value] of limits.entries()) {
    if (!value || value.resetAt <= currentTime) {
      limits.delete(key);
    }
  }
};

export function getClientIp(request) {
  const forwarded = request?.headers?.get?.('x-forwarded-for') || '';
  if (forwarded) {
    return forwarded.split(',')[0].trim();
  }
  return (
    request?.headers?.get?.('x-real-ip') ||
    request?.headers?.get?.('cf-connecting-ip') ||
    'unknown'
  );
}

export function consumeRateLimit({ bucket, key, max, windowMs }) {
  const currentTime = now();
  if (limits.size > MAX_TRACKED_KEYS) {
    pruneExpired(currentTime);
  }

  const scopedKey = `${bucket}:${key}`;
  const existing = limits.get(scopedKey);
  let entry = existing;
  if (!entry || entry.resetAt <= currentTime) {
    entry = {
      count: 0,
      resetAt: currentTime + windowMs,
    };
  }

  entry.count += 1;
  limits.set(scopedKey, entry);

  return {
    allowed: entry.count <= max,
    remaining: Math.max(max - entry.count, 0),
    retryAfterMs: Math.max(entry.resetAt - currentTime, 0),
  };
}

export function resetRateLimit({ bucket, key }) {
  limits.delete(`${bucket}:${key}`);
}

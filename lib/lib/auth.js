import crypto from 'crypto';
import jwt from 'jsonwebtoken';

const DEFAULT_DEV_JWT_SECRET = 'algoaura-dev-secret-key-2024';
const isProduction = process.env.NODE_ENV === 'production';
// during build on platforms like Vercel NODE_ENV is "production", so we
// can't throw just because JWT_SECRET is missing; use the dev secret as a
// fallback and warn in logs instead. A real deployment should still set
// JWT_SECRET as a secure random string.
const JWT_SECRET = process.env.JWT_SECRET || DEFAULT_DEV_JWT_SECRET;

if (isProduction && !process.env.JWT_SECRET) {
  // eslint-disable-next-line no-console
  console.warn('WARNING: JWT_SECRET not set; using default development value.');
}

export function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(password, salt, 64).toString('hex');
  return `scrypt$${salt}$${hash}`;
}

export function verifyPassword(password, storedHash) {
  if (!storedHash) return false;
  const parts = storedHash.split('$');
  if (parts.length !== 3 || parts[0] !== 'scrypt') return false;
  const salt = parts[1];
  const hash = parts[2];
  try {
    const derived = crypto.scryptSync(password, salt, 64);
    const stored = Buffer.from(hash, 'hex');
    if (stored.length !== derived.length) return false;
    return crypto.timingSafeEqual(stored, derived);
  } catch (err) {
    return false;
  }
}

export function signAuthToken(payload, options = {}) {
  const expiresIn = options.expiresIn || '7d';
  return jwt.sign(payload, JWT_SECRET, { expiresIn });
}

export function verifyAuthToken(token) {
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (err) {
    return null;
  }
}

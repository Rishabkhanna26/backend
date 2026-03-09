import crypto from 'crypto';
import jwt from 'jsonwebtoken';

const DEFAULT_DEV_JWT_SECRET = 'algochat-dev-secret-key-2024';
const isProduction = process.env.NODE_ENV === 'production';
const JWT_SECRET = process.env.JWT_SECRET || (isProduction ? '' : DEFAULT_DEV_JWT_SECRET);

if (!JWT_SECRET) {
  throw new Error('JWT_SECRET must be set in production');
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

import { NextResponse } from 'next/server';
import crypto from 'crypto';
import { getConnection } from '../../../../lib/db-helpers';
import { hashPassword } from '../../../../lib/auth';
import { consumeRateLimit, getClientIp } from '../../../../lib/rate-limit';

export const runtime = 'nodejs';
const RESET_WINDOW_MS = 15 * 60 * 1000;
const RESET_MAX_ATTEMPTS = 10;

function hashToken(token) {
  return crypto.createHash('sha256').update(token).digest('hex');
}

export async function POST(request) {
  try {
    const body = await request.json();
    const identifier = String(body?.email || body?.identifier || '').trim();
    const tempPassword = String(body?.tempPassword || '').trim();
    const newPassword = String(body?.newPassword || '').trim();

    if (!identifier || !tempPassword || !newPassword) {
      return NextResponse.json(
        { error: 'Email/phone, temporary password, and new password are required' },
        { status: 400 }
      );
    }

    const identifierLower = identifier.toLowerCase();
    const phoneDigits = identifier.replace(/\D/g, '');
    const idValue = Number.isFinite(Number(identifier)) ? Number(identifier) : -1;
    const clientIp = getClientIp(request);
    const rateLimit = consumeRateLimit({
      bucket: 'auth_reset_password',
      key: `${clientIp}:${identifierLower || phoneDigits || 'unknown'}`,
      max: RESET_MAX_ATTEMPTS,
      windowMs: RESET_WINDOW_MS,
    });
    if (!rateLimit.allowed) {
      return NextResponse.json(
        { error: 'Too many password reset attempts. Please try again later.' },
        {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil(rateLimit.retryAfterMs / 1000)),
          },
        }
      );
    }

    if (newPassword.length < 8) {
      return NextResponse.json(
        { error: 'New password must be at least 8 characters long' },
        { status: 400 }
      );
    }

    const phoneCandidates = Array.from(new Set([identifier, phoneDigits].filter(Boolean)));
    const phoneClause = phoneCandidates.length
      ? ` OR phone IN (${phoneCandidates.map(() => '?').join(', ')})`
      : '';

    const connection = await getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT id, reset_token_hash, reset_expires_at
         FROM admins
         WHERE LOWER(email) = ?${phoneClause} OR id = ?
         LIMIT 1`,
        [identifierLower, ...phoneCandidates, idValue]
      );

      if (!rows || rows.length === 0) {
        return NextResponse.json({ error: 'Invalid temporary password' }, { status: 400 });
      }

      const user = rows[0];
      if (!user.reset_token_hash || !user.reset_expires_at) {
        return NextResponse.json({ error: 'Temporary password expired or invalid' }, { status: 400 });
      }

      const expiresAt = new Date(user.reset_expires_at);
      if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() < Date.now()) {
        return NextResponse.json({ error: 'Temporary password expired' }, { status: 400 });
      }

      const tokenHash = hashToken(tempPassword);
      if (tokenHash !== user.reset_token_hash) {
        return NextResponse.json({ error: 'Invalid temporary password' }, { status: 400 });
      }

      const passwordHash = hashPassword(newPassword);
      await connection.query(
        `UPDATE admins
         SET password_hash = ?, reset_token_hash = NULL, reset_expires_at = NULL
         WHERE id = ?`,
        [passwordHash, user.id]
      );

      return NextResponse.json({ success: true });
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Reset password error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

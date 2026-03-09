import { NextResponse } from 'next/server';
import { getConnection } from '../../../../lib/db-helpers';
import { signAuthToken, verifyPassword } from '../../../../lib/auth';
import { consumeRateLimit, getClientIp, resetRateLimit } from '../../../../lib/rate-limit';

const LOGIN_WINDOW_MS = 15 * 60 * 1000;
const LOGIN_MAX_ATTEMPTS = 10;

export async function POST(request) {
  try {
    const { email, password } = await request.json();
    const identifier = (email || '').trim();
    const identifierLower = identifier.toLowerCase();
    const phoneDigits = identifier.replace(/\D/g, '');
    const clientIp = getClientIp(request);
    const rateLimitKey = `${clientIp}:${identifierLower || phoneDigits || 'unknown'}`;
    const rateLimit = consumeRateLimit({
      bucket: 'auth_login',
      key: rateLimitKey,
      max: LOGIN_MAX_ATTEMPTS,
      windowMs: LOGIN_WINDOW_MS,
    });
    if (!rateLimit.allowed) {
      return NextResponse.json(
        { error: 'Too many login attempts. Please try again later.' },
        {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil(rateLimit.retryAfterMs / 1000)),
          },
        }
      );
    }

    if (!identifier || !password) {
      return NextResponse.json(
        { error: 'Email or phone and password are required' },
        { status: 400 }
      );
    }

    const connection = await getConnection();

    try {
      await connection.execute(
        `UPDATE admins
         SET status = 'inactive'
         WHERE status = 'active'
           AND access_expires_at IS NOT NULL
           AND access_expires_at <= NOW()`
      );

      const idValue = Number.isFinite(Number(identifier)) ? Number(identifier) : -1;
      const phoneCandidates = Array.from(
        new Set([identifier, phoneDigits].filter(Boolean))
      );
      const phoneClause = phoneCandidates.length
        ? ` OR phone IN (${phoneCandidates.map(() => '?').join(', ')})`
        : '';

      const [users] = await connection.execute(
        `SELECT id, name, email, phone, password_hash, admin_tier, status,
                business_category, business_type, booking_enabled, access_expires_at
         FROM admins
         WHERE LOWER(email) = ?${phoneClause} OR id = ?
         LIMIT 1`,
        [identifierLower, ...phoneCandidates, idValue]
      );

      if (!users || users.length === 0) {
        return NextResponse.json(
          { error: 'Invalid email or password' },
          { status: 401 }
        );
      }

      const user = users[0];
      if (user.status !== 'active') {
        return NextResponse.json(
          { error: 'Account is inactive or expired' },
          { status: 403 }
        );
      }

      const isValid = verifyPassword(password, user.password_hash);
      if (!isValid) {
        return NextResponse.json(
          { error: 'Invalid email or password' },
          { status: 401 }
        );
      }
      resetRateLimit({
        bucket: 'auth_login',
        key: rateLimitKey,
      });

      const token = signAuthToken({
        id: user.id,
        name: user.name,
        email: user.email,
        phone: user.phone,
        admin_tier: user.admin_tier,
        business_category: user.business_category,
        business_type: user.business_type,
        booking_enabled: user.booking_enabled,
        access_expires_at: user.access_expires_at,
      });

      const response = NextResponse.json({
        user: {
          id: user.id,
          name: user.name,
          email: user.email,
          phone: user.phone,
          admin_tier: user.admin_tier,
          business_category: user.business_category,
          business_type: user.business_type,
          booking_enabled: user.booking_enabled,
          access_expires_at: user.access_expires_at,
        },
      });

      response.cookies.set({
        name: 'auth_token',
        value: token,
        httpOnly: true,
        sameSite: 'lax',
        path: '/',
        maxAge: 60 * 60 * 24 * 7,
        secure: process.env.NODE_ENV === 'production',
      });

      return response;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Login error:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}

import crypto from 'crypto';
import { NextResponse } from 'next/server';
import { getConnection } from '../../../../../lib/db-helpers';
import { signAuthToken, verifyAuthToken } from '../../../../../lib/auth';
import { consumeRateLimit, getClientIp } from '../../../../../lib/rate-limit';

const LOGIN_OTP_WINDOW_MS = 15 * 60 * 1000;
const LOGIN_OTP_MAX_ATTEMPTS = 12;
const LOGIN_OTP_MAX_CODE_ATTEMPTS = 5;

function hashLoginOtpCode(adminId, code) {
  return crypto
    .createHash('sha256')
    .update(`${adminId}:${String(code || '').trim()}:${process.env.JWT_SECRET || 'algochat-login-otp'}`)
    .digest('hex');
}

function safeHashEqual(expected, actual) {
  const expectedHex = String(expected || '').trim();
  const actualHex = String(actual || '').trim();
  if (!expectedHex || !actualHex || expectedHex.length !== actualHex.length) return false;
  try {
    const left = Buffer.from(expectedHex, 'hex');
    const right = Buffer.from(actualHex, 'hex');
    if (left.length !== right.length || left.length === 0) return false;
    return crypto.timingSafeEqual(left, right);
  } catch {
    return false;
  }
}

function buildLoginResponse(user) {
  const token = signAuthToken({
    id: user.id,
    name: user.name,
    email: user.email,
    phone: user.phone,
    admin_tier: user.admin_tier,
    business_category: user.business_category,
    business_type: user.business_type,
    service_label: user.service_label,
    product_label: user.product_label,
    booking_enabled: user.booking_enabled,
    status: user.status,
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
      service_label: user.service_label,
      product_label: user.product_label,
      booking_enabled: user.booking_enabled,
      status: user.status,
      restricted_mode: user.status !== 'active',
      access_expires_at: user.access_expires_at,
      two_factor_enabled: user.two_factor_enabled === true,
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
}

export async function POST(request) {
  try {
    const body = await request.json();
    const twoFactorToken = String(body?.two_factor_token || '').trim();
    const code = String(body?.code || body?.temp_password || '').trim();
    if (!twoFactorToken || !code) {
      return NextResponse.json(
        { error: 'Temporary password and verification token are required.' },
        { status: 400 }
      );
    }

    const payload = verifyAuthToken(twoFactorToken);
    if (!payload?.id || payload?.scope !== 'login_2fa') {
      return NextResponse.json(
        { error: 'Login session expired. Please login again.' },
        { status: 401 }
      );
    }

    const clientIp = getClientIp(request);
    const rateLimit = consumeRateLimit({
      bucket: 'auth_login_2fa_verify',
      key: `${clientIp}:${payload.id}`,
      max: LOGIN_OTP_MAX_ATTEMPTS,
      windowMs: LOGIN_OTP_WINDOW_MS,
    });
    if (!rateLimit.allowed) {
      return NextResponse.json(
        { error: 'Too many verification attempts. Please login again.' },
        {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil(rateLimit.retryAfterMs / 1000)),
          },
        }
      );
    }

    const connection = await getConnection();
    try {
      const [rows] = await connection.query(
        `SELECT id, name, email, phone, admin_tier, status,
                business_category, business_type, service_label, product_label,
                booking_enabled, access_expires_at, two_factor_enabled,
                login_otp_hash, login_otp_expires_at, login_otp_attempts
         FROM admins
         WHERE id = ?
         LIMIT 1`,
        [payload.id]
      );
      if (!rows || rows.length === 0) {
        return NextResponse.json(
          { error: 'Login session expired. Please login again.' },
          { status: 401 }
        );
      }

      const user = rows[0];
      if (user.two_factor_enabled !== true) {
        return NextResponse.json(
          { error: '2-step login is not enabled for this account.' },
          { status: 400 }
        );
      }

      const expiresAt = user.login_otp_expires_at ? new Date(user.login_otp_expires_at).getTime() : 0;
      if (!user.login_otp_hash || !expiresAt || expiresAt <= Date.now()) {
        await connection.query(
          `UPDATE admins
           SET login_otp_hash = NULL,
               login_otp_expires_at = NULL,
               login_otp_attempts = 0
           WHERE id = ?`,
          [user.id]
        );
        return NextResponse.json(
          { error: 'Temporary password expired. Please login again.' },
          { status: 401 }
        );
      }

      const inputHash = hashLoginOtpCode(user.id, code);
      if (!safeHashEqual(user.login_otp_hash, inputHash)) {
        const nextAttempts = Number(user.login_otp_attempts || 0) + 1;
        if (nextAttempts >= LOGIN_OTP_MAX_CODE_ATTEMPTS) {
          await connection.query(
            `UPDATE admins
             SET login_otp_hash = NULL,
                 login_otp_expires_at = NULL,
                 login_otp_attempts = 0
             WHERE id = ?`,
            [user.id]
          );
          return NextResponse.json(
            { error: 'Too many incorrect temporary password attempts. Please login again.' },
            { status: 429 }
          );
        }
        await connection.query(
          `UPDATE admins
           SET login_otp_attempts = ?
           WHERE id = ?`,
          [nextAttempts, user.id]
        );
        return NextResponse.json(
          {
            error: `Invalid temporary password. ${LOGIN_OTP_MAX_CODE_ATTEMPTS - nextAttempts} attempts left.`,
          },
          { status: 401 }
        );
      }

      await connection.query(
        `UPDATE admins
         SET login_otp_hash = NULL,
             login_otp_expires_at = NULL,
             login_otp_attempts = 0
         WHERE id = ?`,
        [user.id]
      );

      return buildLoginResponse(user);
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Login 2FA verify error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

import crypto from 'crypto';
import { NextResponse } from 'next/server';
import { getConnection } from '../../../../lib/db-helpers';
import { signAuthToken, verifyPassword } from '../../../../lib/auth';
import { consumeRateLimit, getClientIp, resetRateLimit } from '../../../../lib/rate-limit';
import { buildGmailTransporterFromEnv, normalizeSmtpEmail } from '../../../../lib/mailer.js';

const LOGIN_WINDOW_MS = 15 * 60 * 1000;
const LOGIN_MAX_ATTEMPTS = 10;
const LOGIN_OTP_TTL_MS = 10 * 60 * 1000;
const LOGIN_OTP_LENGTH = 6;

function createLoginOtpCode() {
  const min = 10 ** (LOGIN_OTP_LENGTH - 1);
  const max = 10 ** LOGIN_OTP_LENGTH;
  return String(crypto.randomInt(min, max));
}

function hashLoginOtpCode(adminId, code) {
  return crypto
    .createHash('sha256')
    .update(`${adminId}:${String(code || '').trim()}:${process.env.JWT_SECRET || 'algochat-login-otp'}`)
    .digest('hex');
}

function maskEmail(value) {
  const email = String(value || '').trim();
  const atIndex = email.indexOf('@');
  if (atIndex <= 0) return email;
  const name = email.slice(0, atIndex);
  const domain = email.slice(atIndex + 1);
  if (!domain) return email;
  if (name.length <= 2) return `${name[0] || '*'}***@${domain}`;
  return `${name.slice(0, 2)}***@${domain}`;
}

function buildTransporter() {
  return buildGmailTransporterFromEnv();
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
    const { email, password } = await request.json();
    const identifier = String(email || '').trim();
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
                business_category, business_type, service_label, product_label,
                booking_enabled, access_expires_at, two_factor_enabled
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

      if (user.two_factor_enabled === true) {
        if (!user.email) {
          return NextResponse.json(
            { error: '2-step login is enabled, but no email is configured for this account.' },
            { status: 400 }
          );
        }
        const transporter = buildTransporter();
        if (!transporter) {
          return NextResponse.json(
            { error: 'SMTP is not configured. Please set SMTP_EMAIL and SMTP_PASSWORD.' },
            { status: 500 }
          );
        }

        const code = createLoginOtpCode();
        const codeHash = hashLoginOtpCode(user.id, code);
        const expiresAt = new Date(Date.now() + LOGIN_OTP_TTL_MS);

        await connection.query(
          `UPDATE admins
           SET login_otp_hash = ?,
               login_otp_expires_at = ?,
               login_otp_attempts = 0
           WHERE id = ?`,
          [codeHash, expiresAt.toISOString(), user.id]
        );

        const smtpFrom = normalizeSmtpEmail(process.env.SMTP_EMAIL);
        await transporter.sendMail({
          from: smtpFrom ? `"AlgoChat CRM" <${smtpFrom}>` : undefined,
          to: user.email,
          subject: 'AlgoChat Login Verification Code',
          text: [
            `Hello ${user.name || 'there'},`,
            '',
            `Your temporary login password is: ${code}`,
            '',
            'Enter this code on the login screen to continue.',
            'This code expires in 10 minutes.',
            '',
            'If this was not you, please change your password immediately.',
          ].join('\n'),
        });

        const twoFactorToken = signAuthToken(
          {
            scope: 'login_2fa',
            id: user.id,
            email: user.email,
          },
          { expiresIn: '15m' }
        );

        return NextResponse.json({
          requires_two_factor: true,
          two_factor_token: twoFactorToken,
          two_factor_hint: maskEmail(user.email),
          message: 'Temporary login password sent to your email.',
        });
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
    console.error('Login error:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}

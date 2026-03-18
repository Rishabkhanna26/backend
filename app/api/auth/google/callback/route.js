import crypto from 'crypto';
import { NextResponse } from 'next/server';
import { signAuthToken } from '../../../../../lib/auth';
import { getConnection } from '../../../../../lib/db-helpers';
import { sanitizeEmail, sanitizeNameUpper, sanitizePhone } from '../../../../../lib/sanitize.js';

const STATE_COOKIE = 'oauth_google_state';
const GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_USERINFO_URL = 'https://openidconnect.googleapis.com/v1/userinfo';

const buildRedirectUrl = (request, path) => {
  const frontendUrl = String(process.env.FRONTEND_URL || process.env.FRONTEND_ORIGIN || '').trim();
  if (frontendUrl) {
    return new URL(path, frontendUrl);
  }
  return new URL(path, request.url);
};

const decodeState = (value) => {
  if (!value) return null;
  try {
    const decoded = Buffer.from(value, 'base64url').toString('utf8');
    return JSON.parse(decoded);
  } catch (err) {
    return null;
  }
};

const clearStateCookie = (response) => {
  response.cookies.set({
    name: STATE_COOKIE,
    value: '',
    httpOnly: true,
    sameSite: 'lax',
    path: '/',
    maxAge: 0,
    secure: process.env.NODE_ENV === 'production',
  });
};

const buildPlaceholderPhone = (provider, subject) => {
  const seed = `${provider}:${subject || crypto.randomBytes(8).toString('hex')}`;
  const hash = crypto.createHash('sha1').update(seed).digest('hex');
  const numeric = hash
    .split('')
    .map((ch) => (parseInt(ch, 16) % 10).toString())
    .join('')
    .slice(0, 14);
  return sanitizePhone(numeric, { min: 10, max: 14 }) || '9000000000';
};

const isAccessExpired = (value) => {
  if (!value) return false;
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return false;
  return time <= Date.now();
};

export async function GET(request) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  const cookieState = request.cookies.get(STATE_COOKIE)?.value;
  const parsedState = decodeState(cookieState);
  const storedState = parsedState?.state || cookieState;
  const storedRedirectUri = parsedState?.redirectUri;

  if (!code || !state || !storedState || state !== storedState) {
    const response = NextResponse.redirect(buildRedirectUrl(request, '/login?oauth=failed'));
    clearStateCookie(response);
    return response;
  }

  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  const configuredRedirectUri = String(process.env.GOOGLE_REDIRECT_URI || '').trim();
  const frontendUrl = String(process.env.FRONTEND_URL || process.env.FRONTEND_ORIGIN || '').trim();
  const inferredRedirectUri = frontendUrl
    ? new URL('/api/auth/google/callback', frontendUrl).toString()
    : '';
  const redirectUri =
    storedRedirectUri ||
    configuredRedirectUri ||
    inferredRedirectUri ||
    new URL('/api/auth/google/callback', request.url).toString();

  if (!clientId || !clientSecret || !redirectUri) {
    const response = NextResponse.redirect(buildRedirectUrl(request, '/login?oauth=failed'));
    clearStateCookie(response);
    return response;
  }

  try {
    const tokenResponse = await fetch(GOOGLE_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id: clientId,
        client_secret: clientSecret,
        redirect_uri: redirectUri,
        grant_type: 'authorization_code',
      }),
    });

    const tokenData = await tokenResponse.json();
    if (!tokenResponse.ok) {
      throw new Error(tokenData?.error_description || 'Token exchange failed');
    }

    const accessToken = tokenData?.access_token;
    if (!accessToken) {
      throw new Error('Missing access token');
    }

    const profileResponse = await fetch(GOOGLE_USERINFO_URL, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    const profile = await profileResponse.json();
    if (!profileResponse.ok) {
      throw new Error(profile?.error || 'Failed to fetch user info');
    }

    const email = sanitizeEmail(profile?.email);
    if (!email) {
      throw new Error('Missing email');
    }

    const displayName =
      sanitizeNameUpper(
        profile?.name ||
          [profile?.given_name, profile?.family_name].filter(Boolean).join(' ') ||
          'Google User'
      ) || 'GOOGLE USER';

    const connection = await getConnection();
    try {
      const [existing] = await connection.query(
        `SELECT id, name, email, phone, admin_tier, status,
                business_category, business_type, booking_enabled, access_expires_at
         FROM admins
         WHERE LOWER(email) = ?
         LIMIT 1`,
        [email]
      );

      let admin = existing[0];
      if (!admin) {
        const [superAdmins] = await connection.query(
          `SELECT COUNT(*) as count FROM admins WHERE admin_tier = 'super_admin'`
        );
        const hasSuperAdmin = Number(superAdmins[0]?.count || 0) > 0;
        const adminTier = hasSuperAdmin ? 'client_admin' : 'super_admin';
        const status = hasSuperAdmin ? 'inactive' : 'active';
        const placeholderPhone = buildPlaceholderPhone('google', profile?.sub || email);
        const [rows] = await connection.query(
          `INSERT INTO admins (
             name, phone, email, admin_tier, status,
             business_category, business_type
           )
           VALUES (?, ?, ?, ?, ?, 'general', 'both')
           RETURNING id, name, email, phone, admin_tier, status, business_category, business_type, booking_enabled, access_expires_at`,
          [displayName, placeholderPhone, email, adminTier, status]
        );
        admin = rows[0];
      }

      if (!admin || isAccessExpired(admin.access_expires_at)) {
        const response = NextResponse.redirect(buildRedirectUrl(request, '/login?oauth=access_expired'));
        clearStateCookie(response);
        return response;
      }

      const jwtToken = signAuthToken({
        id: admin.id,
        name: admin.name,
        email: admin.email,
        phone: admin.phone,
        admin_tier: admin.admin_tier,
        business_category: admin.business_category,
        business_type: admin.business_type,
        service_label: admin.service_label,
        product_label: admin.product_label,
        booking_enabled: admin.booking_enabled,
        status: admin.status,
        access_expires_at: admin.access_expires_at,
      });

      const response = NextResponse.redirect(buildRedirectUrl(request, '/dashboard'));
      response.cookies.set({
        name: 'auth_token',
        value: jwtToken,
        httpOnly: true,
        sameSite: 'lax',
        path: '/',
        maxAge: 60 * 60 * 24 * 7,
        secure: process.env.NODE_ENV === 'production',
      });
      clearStateCookie(response);
      return response;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Google OAuth error:', error);
    const response = NextResponse.redirect(buildRedirectUrl(request, '/login?oauth=failed'));
    clearStateCookie(response);
    return response;
  }
}

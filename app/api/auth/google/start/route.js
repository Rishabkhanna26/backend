import crypto from 'crypto';
import { NextResponse } from 'next/server';

const STATE_COOKIE = 'oauth_google_state';
const STATE_TTL_SECONDS = 10 * 60;
const GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';

const encodeState = (value) =>
  Buffer.from(JSON.stringify(value)).toString('base64url');

export async function GET(request) {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const redirectUri =
    process.env.GOOGLE_REDIRECT_URI ||
    new URL('/api/auth/google/callback', request.url).toString();

  if (!clientId || !redirectUri) {
    return NextResponse.json(
      { error: 'Google OAuth is not configured' },
      { status: 500 }
    );
  }

  const state = crypto.randomBytes(16).toString('hex');
  const statePayload = encodeState({ state, redirectUri });
  const authUrl = new URL(GOOGLE_AUTH_URL);
  authUrl.searchParams.set('client_id', clientId);
  authUrl.searchParams.set('redirect_uri', redirectUri);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('scope', 'openid email profile');
  authUrl.searchParams.set('state', state);
  authUrl.searchParams.set('prompt', 'select_account');

  const response = NextResponse.redirect(authUrl);
  response.cookies.set({
    name: STATE_COOKIE,
    value: statePayload,
    httpOnly: true,
    sameSite: 'lax',
    path: '/',
    maxAge: STATE_TTL_SECONDS,
    secure: process.env.NODE_ENV === 'production',
  });
  return response;
}

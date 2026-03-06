const REFRESH_BUFFER_MS = 30_000;
const FALLBACK_TTL_MS = 9 * 60 * 1000;

let cachedToken = '';
let cachedExpiresAt = 0;
let inflightRequest = null;

const isTokenUsable = () =>
  Boolean(cachedToken) && Date.now() < cachedExpiresAt - REFRESH_BUFFER_MS;

export function clearBackendJwtCache() {
  cachedToken = '';
  cachedExpiresAt = 0;
  inflightRequest = null;
}

export async function getBackendJwt({ forceRefresh = false } = {}) {
  if (!forceRefresh && isTokenUsable()) {
    return cachedToken;
  }

  if (!forceRefresh && inflightRequest) {
    return inflightRequest;
  }

  inflightRequest = (async () => {
    const response = await fetch('/api/auth/token', {
      method: 'GET',
      credentials: 'include',
    });
    if (!response.ok) {
      clearBackendJwtCache();
      throw new Error('Unable to authenticate backend request');
    }

    const data = await response.json();
    if (!data?.token) {
      clearBackendJwtCache();
      throw new Error('Invalid backend token response');
    }

    cachedToken = data.token;
    const expiresAt = Number(data.expiresAt);
    cachedExpiresAt = Number.isFinite(expiresAt)
      ? expiresAt
      : Date.now() + FALLBACK_TTL_MS;
    return cachedToken;
  })();

  try {
    return await inflightRequest;
  } finally {
    inflightRequest = null;
  }
}

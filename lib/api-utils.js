export function parsePagination(searchParams, { defaultLimit = 50, maxLimit = 200 } = {}) {
  const limitRaw = searchParams?.get('limit');
  let limit = Number(limitRaw);
  if (!Number.isFinite(limit) || limit <= 0) {
    limit = defaultLimit;
  }
  limit = Math.min(limit, maxLimit);

  const offsetRaw = searchParams?.get('offset');
  let offset = Number(offsetRaw);
  if (!Number.isFinite(offset) || offset < 0) {
    offset = 0;
  }
  return { limit, offset };
}

export function parseSearch(searchParams) {
  const value = searchParams?.get('q');
  return value ? value.trim() : '';
}

export function parseStatus(searchParams, fallback = 'all') {
  const value = searchParams?.get('status');
  return value ? value.trim() : fallback;
}

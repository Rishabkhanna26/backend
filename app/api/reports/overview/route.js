import { requireAuth } from '../../../../lib/auth-server';
import { getReportOverview } from '../../../../lib/db-helpers';

const reportCache = new Map();
const REPORT_TTL_MS = 60_000;

function resolveRange(range) {
  const now = new Date();
  let days = 7;
  if (range === '30days') days = 30;
  if (range === '90days') days = 90;
  if (range === '1year') days = 365;
  const start = new Date(now);
  start.setDate(start.getDate() - days);
  return start.toISOString().slice(0, 10);
}

export async function GET(request) {
  try {
    const authUser = await requireAuth();
    const { searchParams } = new URL(request.url);
    const range = searchParams.get('range') || '7days';
    const startDate = resolveRange(range);
    const adminKey = authUser.admin_tier === 'super_admin' ? 'all' : String(authUser.id);
    const cacheKey = `${adminKey}:${range}`;
    const now = Date.now();
    const cached = reportCache.get(cacheKey);
    if (cached && now - cached.at < REPORT_TTL_MS) {
      return Response.json({ success: true, data: cached.data, meta: { cached: true } });
    }

    const overview = await getReportOverview(
      startDate,
      authUser.admin_tier === 'super_admin' ? null : authUser.id
    );
    reportCache.set(cacheKey, { at: now, data: overview });
    return Response.json({ success: true, data: overview, meta: { cached: false } });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

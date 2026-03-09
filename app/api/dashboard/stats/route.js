import { getDashboardStats } from '../../../../lib/db-helpers';
import { requireAuth } from '../../../../lib/auth-server';

const statsCache = new Map();
const STATS_TTL_MS = 30_000;

export async function GET(req) {
  try {
    const authUser = await requireAuth();
    const adminKey = authUser.admin_tier === 'super_admin' ? 'all' : String(authUser.id);
    const now = Date.now();
    const cached = statsCache.get(adminKey);
    if (cached && now - cached.at < STATS_TTL_MS) {
      return Response.json({ success: true, data: cached.data, meta: { cached: true } });
    }

    const stats = await getDashboardStats(
      authUser.admin_tier === 'super_admin' ? null : authUser.id
    );
    statsCache.set(adminKey, { at: now, data: stats });
    return Response.json({ success: true, data: stats, meta: { cached: false } });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

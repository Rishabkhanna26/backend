import { requireAuth } from '../../../lib/auth-server';
import { createBroadcast, getAllBroadcasts, getBroadcastStats } from '../../../lib/db-helpers';
import { parsePagination, parseSearch } from '../../../lib/api-utils';

function normalizeSchedule(value) {
  if (!value) return null;
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed.includes('T')) {
    const [date, time] = trimmed.split('T');
    if (!date || !time) return null;
    const normalizedTime = time.length === 5 ? `${time}:00` : time;
    return `${date} ${normalizedTime}`;
  }
  return trimmed;
}

export async function GET(request) {
  try {
    const authUser = await requireAuth();
    const { searchParams } = new URL(request.url);
    const { limit, offset } = parsePagination(searchParams);
    const search = parseSearch(searchParams);
    const [broadcasts, stats] = await Promise.all([
      getAllBroadcasts(
        authUser.admin_tier === 'super_admin' ? null : authUser.id,
        { search, limit: limit + 1, offset }
      ),
      getBroadcastStats(authUser.admin_tier === 'super_admin' ? null : authUser.id),
    ]);
    const hasMore = broadcasts.length > limit;
    const data = hasMore ? broadcasts.slice(0, limit) : broadcasts;
    const response = Response.json({
      success: true,
      data,
      stats,
      meta: {
        limit,
        offset,
        hasMore,
        nextOffset: hasMore ? offset + limit : null,
      },
    });
    response.headers.set('Cache-Control', 'private, max-age=30, stale-while-revalidate=120');
    return response;
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function POST(request) {
  try {
    const user = await requireAuth();
    const body = await request.json();
    const title = String(body?.title || '').trim();
    const message = String(body?.message || '').trim();
    const targetAudience = String(body?.target_audience || body?.targetAudienceType || 'all').trim() || 'all';
    const scheduledAt = normalizeSchedule(body?.scheduled_at);

    if (!title || !message) {
      return Response.json(
        { success: false, error: 'Title and message are required' },
        { status: 400 }
      );
    }

    const status = scheduledAt ? 'scheduled' : 'draft';
    const broadcast = await createBroadcast({
      title,
      message,
      targetAudienceType: targetAudience,
      scheduledAt,
      status,
      createdBy: user.id,
    });

    return Response.json({ success: true, data: broadcast });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

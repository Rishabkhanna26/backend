import { getAllRequirements } from '../../../lib/db-helpers';
import { parsePagination, parseSearch, parseStatus } from '../../../lib/api-utils';
import { requireReadAuth } from '../../../lib/auth-server';

export async function GET(req) {
  try {
    const authUser = await requireReadAuth();
    const { searchParams } = new URL(req.url);
    const { limit, offset } = parsePagination(searchParams);
    const search = parseSearch(searchParams);
    const status = parseStatus(searchParams);
    const requirements = await getAllRequirements(
      authUser.id,
      { search, status, limit: limit + 1, offset }
    );
    const hasMore = requirements.length > limit;
    const data = hasMore ? requirements.slice(0, limit) : requirements;
    const response = Response.json({
      success: true,
      data,
      meta: {
        limit,
        offset,
        hasMore,
        nextOffset: hasMore ? offset + limit : null,
      },
    });
    response.headers.set('Cache-Control', 'private, max-age=10, stale-while-revalidate=30');
    return response;
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

import { deleteMessagesOlderThan, getAllMessages } from '../../../lib/db-helpers';
import { parsePagination, parseSearch } from '../../../lib/api-utils';
import { requireAuth } from '../../../lib/auth-server';

export async function GET(req) {
  try {
    const authUser = await requireAuth();
    await deleteMessagesOlderThan(15);
    const { searchParams } = new URL(req.url);
    const { limit, offset } = parsePagination(searchParams);
    const search = parseSearch(searchParams);
    const messages = await getAllMessages(authUser.id, {
      search,
      limit: limit + 1,
      offset,
    });
    const hasMore = messages.length > limit;
    const data = hasMore ? messages.slice(0, limit) : messages;
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

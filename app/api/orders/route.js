import { requireAuth } from '../../../lib/auth-server';
import { parsePagination } from '../../../lib/api-utils';
import { getOrders } from '../../../lib/db-helpers';
import { hasProductAccess } from '../../../lib/business.js';

export async function GET(request) {
  try {
    const user = await requireAuth();
    if (!hasProductAccess(user)) {
      return Response.json(
        { success: false, error: 'Orders are disabled for this business type.' },
        { status: 403 }
      );
    }
    const { searchParams } = new URL(request.url);
    const { limit, offset } = parsePagination(searchParams, { defaultLimit: 200, maxLimit: 500 });
    const adminScopeId = user.admin_tier === 'super_admin' ? null : user.id;

    const orders = await getOrders(adminScopeId, { limit: limit + 1, offset });
    const hasMore = orders.length > limit;

    return Response.json({
      success: true,
      data: hasMore ? orders.slice(0, limit) : orders,
      meta: {
        limit,
        offset,
        hasMore,
        nextOffset: hasMore ? offset + limit : null,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

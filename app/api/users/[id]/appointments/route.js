import { getAppointmentsForUser, getUserById } from '../../../../../lib/db-helpers';
import { parsePagination, parseStatus } from '../../../../../lib/api-utils';
import { requireReadAuth } from '../../../../../lib/auth-server';
import { hasAppointmentAccess } from '../../../../../lib/business.js';

export async function GET(req, context) {
  try {
    const authUser = await requireReadAuth();
    if (!authUser.restricted_mode && !hasAppointmentAccess(authUser)) {
      return Response.json(
        { success: false, error: 'Appointments are disabled for this admin.' },
        { status: 403 }
      );
    }

    const params = await context.params;
    const userId = Number(params?.id);
    if (!Number.isFinite(userId)) {
      return Response.json({ success: false, error: 'Invalid user id' }, { status: 400 });
    }

    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const user = await getUserById(userId, adminScopeId);
    if (!user) {
      return Response.json({ success: false, error: 'Contact not found' }, { status: 404 });
    }

    const { searchParams } = new URL(req.url);
    const { limit, offset } = parsePagination(searchParams, { defaultLimit: 10, maxLimit: 50 });
    const status = parseStatus(searchParams);
    const appointments = await getAppointmentsForUser(userId, adminScopeId, {
      status,
      limit: limit + 1,
      offset,
    });

    const hasMore = appointments.length > limit;
    const data = hasMore ? appointments.slice(0, limit) : appointments;

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

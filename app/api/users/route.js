import { addUser, getAllUsers, getUserById, getUserByPhone } from '../../../lib/db-helpers';
import { parsePagination, parseSearch } from '../../../lib/api-utils';
import { requireAuth } from '../../../lib/auth-server';
import { sanitizeEmail, sanitizeNameUpper, sanitizePhone } from '../../../lib/sanitize.js';

export async function GET(req) {
  try {
    const authUser = await requireAuth();
    const { searchParams } = new URL(req.url);
    const { limit, offset } = parsePagination(searchParams);
    const search = parseSearch(searchParams);
    const users = await getAllUsers(
      authUser.admin_tier === 'super_admin' ? null : authUser.id,
      { search, limit: limit + 1, offset }
    );
    const hasMore = users.length > limit;
    const data = hasMore ? users.slice(0, limit) : users;
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

export async function POST(req) {
  try {
    const authUser = await requireAuth();
    const body = await req.json().catch(() => ({}));
    const name = sanitizeNameUpper(body?.name);
    const email = sanitizeEmail(body?.email);
    const phone = sanitizePhone(body?.phone);
    if (!phone) {
      return Response.json({ success: false, error: 'Valid phone number is required.' }, { status: 400 });
    }

    let userId = null;
    try {
      userId = await addUser(phone, name, email, authUser.id);
    } catch (error) {
      if (error?.code === '23505') {
        const existing = await getUserByPhone(phone);
        return Response.json(
          { success: false, error: 'Contact already exists.', data: existing || null },
          { status: 409 }
        );
      }
      throw error;
    }

    const created = userId
      ? await getUserById(userId, authUser.admin_tier === 'super_admin' ? null : authUser.id)
      : null;
    if (!created) {
      return Response.json({ success: false, error: 'Failed to create contact.' }, { status: 500 });
    }

    return Response.json({ success: true, data: created });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

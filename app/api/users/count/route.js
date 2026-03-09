import { requireAuth } from '../../../../lib/auth-server';
import { countUsersSince } from '../../../../lib/db-helpers';

const parseSince = (value) => {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
};

export async function GET(request) {
  try {
    const user = await requireAuth();
    const { searchParams } = new URL(request.url);
    const since = parseSince(searchParams.get('since'));
    const adminId = user.admin_tier === 'super_admin' ? null : user.id;
    const count = await countUsersSince(adminId, since);
    return Response.json({ success: true, count });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

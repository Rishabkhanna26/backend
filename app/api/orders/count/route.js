import { requireAuth } from '../../../../lib/auth-server';
import { countOrdersSince } from '../../../../lib/db-helpers';
import { hasProductAccess } from '../../../../lib/business.js';

const parseSince = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

export async function GET(request) {
  try {
    const user = await requireAuth();
    if (!hasProductAccess(user)) {
      return Response.json({ success: true, count: 0 });
    }
    const { searchParams } = new URL(request.url);
    const sinceParam = searchParams.get('since');
    const sinceDate = parseSince(sinceParam);
    const adminScopeId = user.admin_tier === 'super_admin' ? null : user.id;
    const count = await countOrdersSince(adminScopeId, sinceDate);
    return Response.json({ success: true, count });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

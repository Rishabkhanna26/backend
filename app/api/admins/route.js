import { requireAuth } from '../../../lib/auth-server';
import { getAdmins } from '../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return Response.json({ success: false, error: 'Forbidden' }, { status: 403 });
    }
    const admins = await getAdmins();
    return Response.json({ success: true, data: admins });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

import { requireAuth } from '../../../../../lib/auth-server';
import { getLatestRequirementForUser, getUserById } from '../../../../../lib/db-helpers';

export async function GET(req, context) {
  try {
    const authUser = await requireAuth();
    const params = await context.params;
    const userId = Number(params?.id);
    if (!Number.isFinite(userId)) {
      return Response.json({ success: false, error: 'Invalid user id' }, { status: 400 });
    }

    if (authUser.admin_tier !== 'super_admin') {
      const user = await getUserById(userId, authUser.id);
      if (!user) {
        return Response.json({ success: false, error: 'Contact not found' }, { status: 404 });
      }
    }

    const requirement = await getLatestRequirementForUser(
      userId,
      authUser.admin_tier === 'super_admin' ? null : authUser.id
    );

    return Response.json({ success: true, data: requirement });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

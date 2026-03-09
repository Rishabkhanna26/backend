import { getUserById, updateUserAutomation } from '../../../../lib/db-helpers';
import { requireAuth } from '../../../../lib/auth-server';

export const runtime = 'nodejs';

export async function GET(req, context) {
  try {
    const authUser = await requireAuth();
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

    return Response.json({ success: true, data: user });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function PATCH(req, context) {
  try {
    const authUser = await requireAuth();
    const params = await context.params;
    const userId = Number(params?.id);
    if (!Number.isFinite(userId)) {
      return Response.json({ success: false, error: 'Invalid user id' }, { status: 400 });
    }

    const body = await req.json().catch(() => ({}));
    if (typeof body?.automation_disabled !== 'boolean') {
      return Response.json(
        { success: false, error: 'automation_disabled must be true or false.' },
        { status: 400 }
      );
    }

    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const updated = await updateUserAutomation(userId, body.automation_disabled, adminScopeId);
    if (!updated) {
      return Response.json({ success: false, error: 'Contact not found' }, { status: 404 });
    }

    return Response.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

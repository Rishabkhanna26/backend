import { requireAuth } from '../../../../lib/auth-server';
import { protectModificationAction } from '../../../../lib/api-protection';
import { updateRequirementStatus } from '../../../../lib/db-helpers';

const ALLOWED_STATUSES = new Set(['pending', 'in_progress', 'completed']);

export async function PATCH(request, context) {
  try {
    const authUser = await requireAuth();
    const guard = await protectModificationAction(authUser, 'update');
    if (guard) return guard;
    const params = await context.params;
    const requirementId = Number(params?.id);
    if (!Number.isFinite(requirementId)) {
      return Response.json({ success: false, error: 'Invalid requirement id' }, { status: 400 });
    }

    const body = await request.json();
    const status = String(body?.status || '');
    if (!ALLOWED_STATUSES.has(status)) {
      return Response.json({ success: false, error: 'Invalid status' }, { status: 400 });
    }

    const updated = await updateRequirementStatus(
      requirementId,
      status,
      authUser.id
    );
    if (!updated) {
      return Response.json({ success: false, error: 'Requirement not found' }, { status: 404 });
    }

    return Response.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

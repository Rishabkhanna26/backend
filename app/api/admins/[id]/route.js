import { requireAuth } from '../../../../lib/auth-server';
import {
  approveBusinessTypeChangeRequest,
  countSuperAdmins,
  deleteAdminAndData,
  getAdminById,
  getPendingBusinessTypeChangeRequest,
  rejectBusinessTypeChangeRequest,
  updateAdminAccess,
} from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function PATCH(req, context) {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return Response.json({ success: false, error: 'Forbidden' }, { status: 403 });
    }
    const params = await context.params;
    const adminId = Number(params?.id);
    if (!Number.isFinite(adminId)) {
      return Response.json({ success: false, error: 'Invalid admin id' }, { status: 400 });
    }
    const body = await req.json();
    const admin_tier = body?.admin_tier;
    const status = body?.status;
    const business_type = body?.business_type;
    const business_category = body?.business_category;
    const booking_enabled =
      typeof body?.booking_enabled === 'boolean' ? body.booking_enabled : undefined;
    const access_duration_value = Number(body?.access_duration_value || 0);
    const access_duration_unit =
      String(body?.access_duration_unit || 'days').toLowerCase() === 'months'
        ? 'months'
        : 'days';

    const allowedTiers = new Set(['super_admin', 'client_admin']);
    const allowedStatus = new Set(['active', 'inactive']);
    const allowedBusinessTypes = new Set(['product', 'service', 'both']);

    if (admin_tier && !allowedTiers.has(admin_tier)) {
      return Response.json({ success: false, error: 'Invalid admin role' }, { status: 400 });
    }
    if (status && !allowedStatus.has(status)) {
      return Response.json({ success: false, error: 'Invalid status' }, { status: 400 });
    }
    if (business_type && !allowedBusinessTypes.has(business_type)) {
      return Response.json({ success: false, error: 'Invalid business type' }, { status: 400 });
    }

    const target = await getAdminById(adminId);
    if (!target) {
      return Response.json({ success: false, error: 'Admin not found' }, { status: 404 });
    }

    if (admin_tier === 'super_admin' && target.admin_tier !== 'super_admin') {
      const superCount = await countSuperAdmins();
      if (superCount >= 2) {
        return Response.json(
          { success: false, error: 'Only 2 super admins are allowed at a time.' },
          { status: 400 }
        );
      }
    }

    if (admin_tier === 'client_admin' && target.admin_tier === 'super_admin') {
      const superCount = await countSuperAdmins();
      if (superCount <= 1) {
        return Response.json(
          { success: false, error: 'At least one super admin is required.' },
          { status: 400 }
        );
      }
    }

    if (adminId === user.id) {
      if (status === 'inactive') {
        return Response.json({ success: false, error: 'You cannot deactivate your own account.' }, { status: 400 });
      }
      if (admin_tier && admin_tier !== 'super_admin') {
        return Response.json({ success: false, error: 'You cannot change your own role.' }, { status: 400 });
      }
    }

    const allowOverride = user.admin_tier === 'super_admin';
    const currentBusinessType = target.business_type || 'both';
    const requestedBusinessType =
      business_type && business_type !== currentBusinessType ? business_type : null;
    let approvedRequest = null;
    let rejectedRequest = null;
    if (requestedBusinessType) {
      const pendingRequest = await getPendingBusinessTypeChangeRequest(adminId);
      if (pendingRequest) {
        const matchesRequest =
          pendingRequest.requested_business_type === requestedBusinessType;
        const paymentCleared =
          !pendingRequest.payment_required || pendingRequest.payment_status === 'paid';
        if (matchesRequest && paymentCleared) {
          approvedRequest = pendingRequest;
        } else if (allowOverride) {
          rejectedRequest = pendingRequest;
        } else if (!matchesRequest) {
          return Response.json(
            { success: false, error: 'Business type change requires a pending request.' },
            { status: 400 }
          );
        } else {
          return Response.json(
            { success: false, error: 'Payment is still pending for this business type request.' },
            { status: 400 }
          );
        }
      } else if (!allowOverride) {
        return Response.json(
          { success: false, error: 'Business type change requires a pending request.' },
          { status: 400 }
        );
      }
    }

    const hasDuration =
      Number.isFinite(access_duration_value) && access_duration_value > 0;
    const effectiveStatus = hasDuration ? 'active' : status;

    let access_expires_at = undefined;

    if (effectiveStatus === 'inactive') {
      access_expires_at = null;
    } else if (effectiveStatus === 'active' || hasDuration) {
      if (hasDuration) {
        const now = new Date();
        const expires = new Date(now);
        if (access_duration_unit === 'months') {
          expires.setMonth(expires.getMonth() + access_duration_value);
        } else {
          expires.setDate(expires.getDate() + access_duration_value);
        }
        access_expires_at = expires.toISOString();
      } else if (Object.prototype.hasOwnProperty.call(body, 'access_duration_value')) {
        access_expires_at = null;
      }
    }

    if (approvedRequest) {
      try {
        const approved = await approveBusinessTypeChangeRequest({
          requestId: approvedRequest.id,
          resolvedBy: user.id,
        });
        if (!approved) {
          approvedRequest = null;
        }
      } catch (error) {
        approvedRequest = null;
      }
    }

    if (rejectedRequest) {
      try {
        await rejectBusinessTypeChangeRequest({
          requestId: rejectedRequest.id,
          resolvedBy: user.id,
        });
      } catch (error) {
        // ignore rejection failures and proceed with direct update
      }
    }

    const updated = await updateAdminAccess(adminId, {
      admin_tier,
      status: effectiveStatus,
      business_type: approvedRequest ? undefined : business_type,
      booking_enabled,
      business_category: typeof business_category === 'string' ? business_category : undefined,
      access_expires_at,
    });
    if (!updated) {
      return Response.json({ success: false, error: 'Admin not found' }, { status: 404 });
    }
    return Response.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function DELETE(req, context) {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return Response.json({ success: false, error: 'Forbidden' }, { status: 403 });
    }
    const params = await context.params;
    const adminId = Number(params?.id);
    if (!Number.isFinite(adminId)) {
      return Response.json({ success: false, error: 'Invalid admin id' }, { status: 400 });
    }
    if (adminId === user.id) {
      return Response.json({ success: false, error: 'You cannot delete your own account.' }, { status: 400 });
    }

    const target = await getAdminById(adminId);
    if (!target) {
      return Response.json({ success: false, error: 'Admin not found' }, { status: 404 });
    }

    if (target.admin_tier === 'super_admin') {
      const superCount = await countSuperAdmins();
      if (superCount <= 1) {
        return Response.json({ success: false, error: 'Cannot delete the last super admin.' }, { status: 400 });
      }
    }

    const result = await deleteAdminAndData(adminId, user.id);
    if (!result.ok) {
      if (result.reason === 'no_super_admin_to_transfer') {
        return Response.json(
          { success: false, error: 'No super admin available to transfer contacts.' },
          { status: 400 }
        );
      }
      return Response.json({ success: false, error: 'Admin not found' }, { status: 404 });
    }

    return Response.json({ success: true, data: result.admin });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

import { requireAuth } from '../../../../lib/auth-server';
import { protectModificationAction } from '../../../../lib/api-protection';
import { deleteAppointment, updateAppointment } from '../../../../lib/db-helpers';
import { hasAppointmentAccess, hasBookingAccess, hasServiceAccess } from '../../../../lib/business.js';

const ALLOWED_STATUSES = new Set(['booked', 'completed', 'cancelled']);
const ALLOWED_PAYMENT_METHODS = new Set(['cash', 'card', 'upi', 'bank', 'wallet', 'other', '']);
const ALLOWED_APPOINTMENT_KINDS = new Set(['service', 'booking']);

export async function PATCH(request, context) {
  try {
    const authUser = await requireAuth();
    const guard = await protectModificationAction(authUser, 'update');
    if (guard) return guard;
    if (!hasAppointmentAccess(authUser)) {
      return Response.json(
        { success: false, error: 'Appointments are disabled for this admin.' },
        { status: 403 }
      );
    }
    const params = await context.params;
    const appointmentId = Number(params?.id);
    if (!Number.isFinite(appointmentId)) {
      return Response.json({ success: false, error: 'Invalid appointment id' }, { status: 400 });
    }

    const body = await request.json();
    const updates = {};

    if (Object.prototype.hasOwnProperty.call(body, 'status')) {
      const status = String(body?.status || '');
      if (!ALLOWED_STATUSES.has(status)) {
        return Response.json({ success: false, error: 'Invalid status' }, { status: 400 });
      }
      updates.status = status;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'appointment_type')) {
      updates.appointment_type = String(body?.appointment_type || '').trim() || null;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'appointment_kind')) {
      const appointmentKind = String(body?.appointment_kind || '').trim().toLowerCase();
      if (!ALLOWED_APPOINTMENT_KINDS.has(appointmentKind)) {
        return Response.json({ success: false, error: 'Invalid appointment kind' }, { status: 400 });
      }
      if (appointmentKind === 'booking' && !hasBookingAccess(authUser)) {
        return Response.json({ success: false, error: 'Booking access is not enabled.' }, { status: 403 });
      }
      if (appointmentKind === 'service' && !hasServiceAccess(authUser)) {
        return Response.json({ success: false, error: 'Service appointments are disabled for this admin.' }, { status: 403 });
      }
      updates.appointment_kind = appointmentKind;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'start_time')) {
      const start = new Date(body?.start_time);
      if (Number.isNaN(start.getTime())) {
        return Response.json({ success: false, error: 'Invalid start time' }, { status: 400 });
      }
      updates.start_time = start.toISOString();
    }

    if (Object.prototype.hasOwnProperty.call(body, 'end_time')) {
      const end = new Date(body?.end_time);
      if (Number.isNaN(end.getTime())) {
        return Response.json({ success: false, error: 'Invalid end time' }, { status: 400 });
      }
      updates.end_time = end.toISOString();
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_total')) {
      updates.payment_total = body?.payment_total;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'payment_paid')) {
      updates.payment_paid = body?.payment_paid;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'payment_method')) {
      const method = String(body?.payment_method || '');
      if (!ALLOWED_PAYMENT_METHODS.has(method)) {
        return Response.json({ success: false, error: 'Invalid payment method' }, { status: 400 });
      }
      updates.payment_method = method || null;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'payment_notes')) {
      updates.payment_notes = String(body?.payment_notes || '').trim() || null;
    }

    if (Object.keys(updates).length === 0) {
      return Response.json({ success: false, error: 'No updates provided' }, { status: 400 });
    }

    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const updated = await updateAppointment(appointmentId, updates, adminScopeId);
    if (!updated) {
      return Response.json({ success: false, error: 'Appointment not found' }, { status: 404 });
    }

    return Response.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function DELETE(request, context) {
  try {
    const authUser = await requireAuth();
    const guard = await protectModificationAction(authUser, 'delete');
    if (guard) return guard;
    if (!hasAppointmentAccess(authUser)) {
      return Response.json(
        { success: false, error: 'Appointments are disabled for this admin.' },
        { status: 403 }
      );
    }
    const params = await context.params;
    const appointmentId = Number(params?.id);
    if (!Number.isFinite(appointmentId)) {
      return Response.json({ success: false, error: 'Invalid appointment id' }, { status: 400 });
    }

    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const result = await deleteAppointment(appointmentId, adminScopeId);
    if (!result?.success) {
      const errorMessage = result?.error || 'Appointment not found';
      const status = errorMessage === 'Appointment not found' ? 404 : 400;
      return Response.json({ success: false, error: errorMessage }, { status });
    }

    return Response.json({ success: true });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

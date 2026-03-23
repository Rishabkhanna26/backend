import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import {
  getEffectiveRazorpayCredentials,
  getAdminPaymentLink,
  updateAdminPaymentLinkStatus,
  creditAdminPaidTokens,
  extendAdminDashboardSubscription,
  updateAdminAccess,
  markBusinessTypeChangeRequestPaid,
} from '../../../../lib/db-helpers';
import { normalizeBusinessType } from '../../../../lib/business.js';
import { isRazorpayConfigured, verifyRazorpayPaymentLink } from '../../../../lib/razorpay.js';

export const runtime = 'nodejs';

const normalizeStatus = (value) => {
  const status = String(value || '').trim().toLowerCase();
  if (['paid', 'cancelled', 'expired', 'failed', 'created'].includes(status)) return status;
  if (status === 'partially_paid') return 'pending';
  return 'pending';
};

const parseNoteBoolean = (value) => {
  if (value === true || value === false) return value;
  const normalized = String(value || '').trim().toLowerCase();
  if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
  if (['false', '0', 'no', 'off'].includes(normalized)) return false;
  return null;
};

export async function POST(request) {
  try {
    await requireAuth();
    const body = await request.json();
    const paymentLinkId = String(body?.payment_link_id || '').trim();
    if (!paymentLinkId) {
      return NextResponse.json(
        { success: false, error: 'payment_link_id is required.' },
        { status: 400 }
      );
    }

    const existingLink = await getAdminPaymentLink(paymentLinkId);
    const collectorCreds = (await getEffectiveRazorpayCredentials(existingLink?.admin_id)) || {};
    if (!isRazorpayConfigured(collectorCreds)) {
      return NextResponse.json(
        { success: false, error: 'Razorpay is not configured for billing collections.' },
        { status: 400 }
      );
    }

    const verification = await verifyRazorpayPaymentLink({
      paymentLinkId,
      credentials: collectorCreds,
    });

    const status = verification?.verified ? 'paid' : normalizeStatus(verification?.linkStatus);
    await updateAdminPaymentLinkStatus({
      paymentLinkId,
      status,
      paidAmount: verification?.paidAmount || 0,
      paidAt: verification?.paidAt || null,
      rawJson: verification?.raw || null,
    });

    if (
      status === 'paid' &&
      existingLink?.status !== 'paid' &&
      existingLink?.purpose === 'prepaid'
    ) {
      await creditAdminPaidTokens({
        adminId: existingLink.admin_id,
        inputTokens: existingLink.input_tokens,
        outputTokens: existingLink.output_tokens,
      });
    }

    if (
      status === 'paid' &&
      existingLink?.status !== 'paid' &&
      existingLink?.purpose === 'dashboard'
    ) {
      await extendAdminDashboardSubscription({
        adminId: existingLink.admin_id,
        months: existingLink.subscription_months || 1,
      });
      const notes = verification?.raw?.link?.notes || {};
      const normalizedType = normalizeBusinessType(notes?.profile_type, null);
      const bookingFromNotes = parseNoteBoolean(notes?.booking_enabled);
      const updates = {};
      if (normalizedType) {
        updates.business_type = normalizedType;
      }
      if (normalizedType === 'product') {
        updates.booking_enabled = false;
      } else if (bookingFromNotes !== null) {
        updates.booking_enabled = bookingFromNotes;
      }
      if (Object.keys(updates).length > 0) {
        try {
          await updateAdminAccess(existingLink.admin_id, updates);
        } catch (_error) {
          // Ignore profile update failures after successful payment verification.
        }
      }
    }

    if (
      status === 'paid' &&
      existingLink?.status !== 'paid' &&
      existingLink?.purpose === 'business_type_change'
    ) {
      await markBusinessTypeChangeRequestPaid({
        paymentLinkId,
        paidAmount: verification?.paidAmount || 0,
        paidAt: verification?.paidAt || null,
      });
    }

    return NextResponse.json({
      success: true,
      data: {
        status,
        paid_amount: verification?.paidAmount || 0,
        currency: verification?.currency || 'INR',
        paid_at: verification?.paidAt || null,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

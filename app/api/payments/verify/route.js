import { NextResponse } from 'next/server';
import { getSessionUser } from '../../../../lib/auth-server';
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
    const user = await getSessionUser();
    const body = await request.json();
    const paymentLinkId = String(body?.payment_link_id || '').trim();
    if (!paymentLinkId) {
      return NextResponse.json(
        { success: false, error: 'payment_link_id is required.' },
        { status: 400 }
      );
    }

    const existingLink = await getAdminPaymentLink(paymentLinkId);
    if (!existingLink) {
      return NextResponse.json(
        { success: false, error: 'Payment link not found.' },
        { status: 404 }
      );
    }
    if (user && user.admin_tier !== 'super_admin' && existingLink.admin_id !== user.id) {
      return NextResponse.json(
        { success: false, error: 'Forbidden' },
        { status: 403 }
      );
    }
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
    const updatedRows = await updateAdminPaymentLinkStatus({
      paymentLinkId,
      status,
      paidAmount: verification?.paidAmount || 0,
      paidAt: verification?.paidAt || null,
      rawJson: verification?.raw || null,
      onlyIfNotPaid: status === 'paid',
    });
    const transitionedToPaid = status === 'paid' && Number(updatedRows || 0) > 0;

    if (
      transitionedToPaid &&
      existingLink?.purpose === 'prepaid'
    ) {
      await creditAdminPaidTokens({
        adminId: existingLink.admin_id,
        inputTokens: existingLink.input_tokens,
        outputTokens: existingLink.output_tokens,
      });
    }

    if (
      transitionedToPaid &&
      existingLink?.purpose === 'dashboard'
    ) {
      await extendAdminDashboardSubscription({
        adminId: existingLink.admin_id,
        months: existingLink.subscription_months || 1,
      });
      const notes = verification?.raw?.link?.notes || {};
      const normalizedType = normalizeBusinessType(notes?.profile_type, null);
      const bookingFromNotes = parseNoteBoolean(notes?.booking_enabled);
      const updates = {
        status: 'active',
        access_expires_at: null,
      };
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
      transitionedToPaid &&
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
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

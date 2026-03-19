import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import {
  getEffectiveRazorpayCredentials,
  getAdminPaymentLink,
  updateAdminPaymentLinkStatus,
  creditAdminPaidTokens,
  extendAdminDashboardSubscription,
  markBusinessTypeChangeRequestPaid,
} from '../../../../lib/db-helpers';
import { isRazorpayConfigured, verifyRazorpayPaymentLink } from '../../../../lib/razorpay.js';

export const runtime = 'nodejs';

const normalizeStatus = (value) => {
  const status = String(value || '').trim().toLowerCase();
  if (['paid', 'cancelled', 'expired', 'failed', 'created'].includes(status)) return status;
  if (status === 'partially_paid') return 'pending';
  return 'pending';
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

import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import {
  createAdminPaymentLinkRecord,
  createBusinessTypeChangeRequest,
  getAdminById,
  getDashboardChargeRates,
  getEffectiveRazorpayCredentials,
  getLatestBusinessTypeChangeRequest,
  getPendingBusinessTypeChangeRequest,
} from '../../../../lib/db-helpers';
import { computeMaintenanceTotals } from '../../../../lib/billing';
import { createRazorpayPaymentLink, isRazorpayConfigured } from '../../../../lib/razorpay';
import { normalizeBusinessType } from '../../../../lib/business';

export const runtime = 'nodejs';

const toTrimmed = (value) => String(value || '').trim();

const buildBillingCallbackUrl = () => {
  const explicit = toTrimmed(process.env.RAZORPAY_BILLING_CALLBACK_URL);
  if (explicit) return explicit;
  const frontendOrigin =
    toTrimmed(process.env.FRONTEND_ORIGIN) ||
    toTrimmed(process.env.PUBLIC_URL) ||
    toTrimmed(process.env.RENDER_EXTERNAL_URL) ||
    'http://localhost:3000';
  try {
    return new URL('/billing/thank-you', frontendOrigin).toString();
  } catch (_error) {
    return '';
  }
};

const ALLOWED_BUSINESS_TYPES = new Set(['product', 'service', 'both']);

const resolveMonthlyCharge = (type, rates) => {
  if (type === 'service') return Number(rates?.service_inr || 0);
  if (type === 'product') return Number(rates?.product_inr || 0);
  return Number(rates?.both_inr || 0);
};

export async function GET() {
  try {
    const user = await requireAuth();
    const request = await getLatestBusinessTypeChangeRequest(user.id);
    return NextResponse.json({ success: true, data: request || null });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function POST(request) {
  try {
    const user = await requireAuth();
    const body = await request.json().catch(() => ({}));
    const requestedTypeRaw =
      typeof body?.business_type === 'string'
        ? body.business_type.trim().toLowerCase()
        : '';
    if (!ALLOWED_BUSINESS_TYPES.has(requestedTypeRaw)) {
      return NextResponse.json(
        { success: false, error: 'Invalid business type requested.' },
        { status: 400 }
      );
    }

    const adminProfile = await getAdminById(user.id);
    if (!adminProfile) {
      return NextResponse.json({ success: false, error: 'User not found' }, { status: 404 });
    }

    const currentType = normalizeBusinessType(adminProfile.business_type, 'both');
    if (requestedTypeRaw === currentType) {
      return NextResponse.json(
        { success: false, error: 'Your business type is already set to this value.' },
        { status: 400 }
      );
    }

    const pending = await getPendingBusinessTypeChangeRequest(user.id);
    if (pending) {
      return NextResponse.json(
        { success: false, error: 'A business type change request is already pending.' },
        { status: 409 }
      );
    }

    const rates = await getDashboardChargeRates();
    const currentMonthly = resolveMonthlyCharge(currentType, rates);
    const requestedMonthly = resolveMonthlyCharge(requestedTypeRaw, rates);
    if (!(currentMonthly > 0 && requestedMonthly > 0)) {
      return NextResponse.json(
        { success: false, error: 'Business type pricing is not configured yet.' },
        { status: 400 }
      );
    }

    const delta = Number((requestedMonthly - currentMonthly).toFixed(2));
    const paymentRequired = delta > 0;
    let paymentLink = null;
    let paymentTotals = null;
    let paymentStatus = paymentRequired ? 'unpaid' : 'waived';
    let paymentLinkId = null;
    let paymentLinkUrl = null;

    if (paymentRequired) {
      const collectorCreds = (await getEffectiveRazorpayCredentials(user.id)) || {};
      if (!isRazorpayConfigured(collectorCreds)) {
        return NextResponse.json(
          { success: false, error: 'Razorpay is not configured for billing collections.' },
          { status: 400 }
        );
      }

      paymentTotals = computeMaintenanceTotals({ baseInr: delta });
      const description = `AlgoChat business type change for ${
        user.name || user.email || `Admin #${user.id}`
      }`;
      paymentLink = await createRazorpayPaymentLink({
        amount: paymentTotals.totalInr,
        currency: 'INR',
        description: description.slice(0, 255),
        callbackUrl: buildBillingCallbackUrl(),
        callbackMethod:
          toTrimmed(process.env.RAZORPAY_CALLBACK_METHOD).toLowerCase() === 'post' ? 'post' : 'get',
        referenceId: `admin_${user.id}_business_type_${Date.now()}`,
        credentials: collectorCreds,
        customer: {
          name: toTrimmed(user?.name || ''),
          email: toTrimmed(user?.email || ''),
          contact: toTrimmed(user?.phone || '').replace(/\D/g, ''),
        },
        notes: {
          admin_id: String(user.id),
          purpose: 'business_type_change',
          requested_type: requestedTypeRaw,
          current_type: currentType,
          monthly_current_inr: String(currentMonthly),
          monthly_requested_inr: String(requestedMonthly),
          monthly_delta_inr: String(delta),
          base_amount_inr: String(paymentTotals.baseInr),
          maintenance_fee_inr: String(paymentTotals.maintenanceFeeInr),
        },
      });

      if (!paymentLink?.id || !paymentLink?.shortUrl) {
        return NextResponse.json(
          { success: false, error: 'Razorpay returned an invalid payment link response.' },
          { status: 502 }
        );
      }

      await createAdminPaymentLinkRecord({
        adminId: user.id,
        paymentLinkId: paymentLink.id,
        amount: paymentTotals.totalInr,
        baseAmount: paymentTotals.baseInr,
        maintenanceFee: paymentTotals.maintenanceFeeInr,
        currency: paymentLink.currency || 'INR',
        purpose: 'business_type_change',
        rawJson: paymentLink.raw || null,
      });

      paymentLinkId = paymentLink.id;
      paymentLinkUrl = paymentLink.shortUrl;
    }

    const requestRecord = await createBusinessTypeChangeRequest({
      adminId: user.id,
      currentBusinessType: currentType,
      requestedBusinessType: requestedTypeRaw,
      reason: typeof body?.reason === 'string' ? body.reason : '',
      monthlyCurrentInr: currentMonthly,
      monthlyRequestedInr: requestedMonthly,
      monthlyDeltaInr: delta,
      paymentRequired,
      paymentStatus,
      paymentLinkId,
      paymentLinkUrl,
    });
    if (!requestRecord) {
      return NextResponse.json(
        { success: false, error: 'Failed to create business type request.' },
        { status: 500 }
      );
    }

    return NextResponse.json({
      success: true,
      data: {
        ...requestRecord,
        payment: paymentLink
          ? {
              payment_link_id: paymentLink.id,
              short_url: paymentLink.shortUrl,
              amount: paymentTotals?.totalInr,
              base_amount: paymentTotals?.baseInr,
              maintenance_fee: paymentTotals?.maintenanceFeeInr,
              currency: paymentLink.currency || 'INR',
            }
          : null,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

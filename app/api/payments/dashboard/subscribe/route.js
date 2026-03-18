import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../../lib/auth-server';
import {
  createAdminPaymentLinkRecord,
  getAdminBillingSettings,
  getAdminById,
  getDashboardChargeRates,
  getEffectiveRazorpayCredentials,
} from '../../../../../lib/db-helpers';
import { normalizeBusinessType } from '../../../../../lib/business.js';
import { computeMaintenanceTotals } from '../../../../../lib/billing.js';
import { createRazorpayPaymentLink, isRazorpayConfigured } from '../../../../../lib/razorpay.js';

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

const resolveDiscountPct = (months) => {
  if (months === 3) return 5;
  if (months === 6) return 8;
  if (months === 12) return 10;
  return 0;
};

export async function POST(request) {
  try {
    const user = await requireAuth();
    if (user.admin_tier === 'super_admin') {
      return NextResponse.json(
        { success: false, error: 'Super admins do not require dashboard subscriptions.' },
        { status: 400 }
      );
    }

    const body = await request.json();
    const months = Math.max(1, Math.trunc(Number(body?.subscription_months) || 1));
    if (![1, 3, 6, 12].includes(months)) {
      return NextResponse.json(
        { success: false, error: 'Subscription months must be 1, 3, 6, or 12.' },
        { status: 400 }
      );
    }

    const settings = await getAdminBillingSettings(user.id);
    if (settings?.dashboard_charge_enabled === false) {
      return NextResponse.json(
        { success: false, error: 'Dashboard charges are disabled for your account.' },
        { status: 400 }
      );
    }

    const adminProfile = await getAdminById(user.id);
    const rates = await getDashboardChargeRates();
    const businessType = normalizeBusinessType(adminProfile?.business_type, 'both');
    const bookingEnabled = Boolean(adminProfile?.booking_enabled);
    const baseMonthly =
      businessType === 'service'
        ? rates.service_inr
        : businessType === 'product'
        ? rates.product_inr
        : rates.both_inr;
    const bookingCharge = bookingEnabled ? rates.booking_inr : 0;
    const monthlyTotal = Number(baseMonthly || 0) + Number(bookingCharge || 0);

    if (!Number.isFinite(monthlyTotal) || monthlyTotal <= 0) {
      return NextResponse.json(
        { success: false, error: 'Dashboard charges are not configured yet.' },
        { status: 400 }
      );
    }

    const baseAmount = monthlyTotal * months;
    const discountPct = resolveDiscountPct(months);
    const discountAmount = Number((baseAmount * (discountPct / 100)).toFixed(2));
    const discountedBase = Number((baseAmount - discountAmount).toFixed(2));
    const maintenanceTotals = computeMaintenanceTotals({ baseInr: discountedBase });

    const collectorCreds = (await getEffectiveRazorpayCredentials(user.id)) || {};
    if (!isRazorpayConfigured(collectorCreds)) {
      return NextResponse.json(
        { success: false, error: 'Razorpay is not configured for billing collections.' },
        { status: 400 }
      );
    }

    const description = `AlgoChat dashboard subscription for ${user.name || user.email || `Admin #${user.id}`}`;
    const paymentLink = await createRazorpayPaymentLink({
      amount: maintenanceTotals.totalInr,
      currency: 'INR',
      description: description.slice(0, 255),
      callbackUrl: buildBillingCallbackUrl(),
      callbackMethod:
        toTrimmed(process.env.RAZORPAY_CALLBACK_METHOD).toLowerCase() === 'post' ? 'post' : 'get',
      referenceId: `admin_${user.id}_dashboard_${months}_${Date.now()}`,
      credentials: collectorCreds,
      customer: {
        name: toTrimmed(user?.name || ''),
        email: toTrimmed(user?.email || ''),
        contact: toTrimmed(user?.phone || '').replace(/\D/g, ''),
      },
      notes: {
        admin_id: String(user.id),
        purpose: 'dashboard',
        months: String(months),
        monthly_amount_inr: String(monthlyTotal),
        discount_pct: String(discountPct),
        base_amount_inr: String(maintenanceTotals.baseInr),
        maintenance_fee_inr: String(maintenanceTotals.maintenanceFeeInr),
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
      amount: maintenanceTotals.totalInr,
      baseAmount: maintenanceTotals.baseInr,
      maintenanceFee: maintenanceTotals.maintenanceFeeInr,
      currency: paymentLink.currency || 'INR',
      purpose: 'dashboard',
      subscriptionMonths: months,
      discountPct,
      dashboardMonthlyAmount: monthlyTotal,
      rawJson: paymentLink.raw || null,
    });

    return NextResponse.json({
      success: true,
      data: {
        payment_link_id: paymentLink.id,
        short_url: paymentLink.shortUrl,
        amount: maintenanceTotals.totalInr,
        base_amount: maintenanceTotals.baseInr,
        maintenance_fee: maintenanceTotals.maintenanceFeeInr,
        currency: paymentLink.currency || 'INR',
        subscription_months: months,
        discount_pct: discountPct,
        dashboard_monthly_amount: monthlyTotal,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

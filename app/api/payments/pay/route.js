import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import {
  createAdminPaymentLinkRecord,
  getAdminBillingSettings,
  getAdminPaygUsageSummary,
  getAdminPaymentTotals,
  getEffectiveRazorpayCredentials,
} from '../../../../lib/db-helpers';
import {
  DEFAULT_INPUT_USD_PER_1M,
  DEFAULT_OPENAI_MODEL,
  DEFAULT_OUTPUT_USD_PER_1M,
  DEFAULT_USD_TO_INR_RATE,
  computeMaintenanceTotals,
  RAZORPAY_FEE_RATE,
  SERVICE_FEE_RATE,
  computePaymentTotals,
} from '../../../../lib/billing.js';
import {
  createRazorpayPaymentLink,
  isRazorpayConfigured,
} from '../../../../lib/razorpay.js';

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

export async function POST() {
  try {
    const user = await requireAuth();
    const settings = await getAdminBillingSettings(user.id);
    const usage = await getAdminPaygUsageSummary(user.id);
    const payments = await getAdminPaymentTotals(user.id, { purpose: 'payg' });

    const baseCostInr = Number(usage?.total_cost_inr || 0);
    const totals = computePaymentTotals({ baseInr: baseCostInr });
    const totalPaid = Number(payments?.total_paid || 0);
    const dueAmount = Math.max(Number(totals.totalInr) - totalPaid, 0);

    if (!Number.isFinite(dueAmount) || dueAmount <= 0) {
      return NextResponse.json(
        { success: false, error: 'No outstanding balance to pay.' },
        { status: 400 }
      );
    }

    const collectorCreds = (await getEffectiveRazorpayCredentials(user.id)) || {};
    if (!isRazorpayConfigured(collectorCreds)) {
      return NextResponse.json(
        { success: false, error: 'Razorpay is not configured for billing collections.' },
        { status: 400 }
      );
    }

    const maintenanceTotals = computeMaintenanceTotals({ baseInr: dueAmount });
    const totalAmount = maintenanceTotals.totalInr;
    const description = `AlgoChat pay-as-you-go AI charges for ${user.name || user.email || `Admin #${user.id}`}`;
    const paymentLink = await createRazorpayPaymentLink({
      amount: totalAmount,
      currency: 'INR',
      description: description.slice(0, 255),
      callbackUrl: buildBillingCallbackUrl(),
      callbackMethod:
        toTrimmed(process.env.RAZORPAY_CALLBACK_METHOD).toLowerCase() === 'post' ? 'post' : 'get',
      referenceId: `admin_${user.id}_${Date.now()}`,
      credentials: collectorCreds,
      customer: {
        name: toTrimmed(user?.name || ''),
        email: toTrimmed(user?.email || ''),
        contact: toTrimmed(user?.phone || '').replace(/\D/g, ''),
      },
      notes: {
        admin_id: String(user.id),
        model: DEFAULT_OPENAI_MODEL,
        input_usd_per_1m: String(settings?.input_price_usd_per_1m ?? DEFAULT_INPUT_USD_PER_1M),
        output_usd_per_1m: String(settings?.output_price_usd_per_1m ?? DEFAULT_OUTPUT_USD_PER_1M),
        usd_to_inr: String(DEFAULT_USD_TO_INR_RATE),
        base_inr: String(baseCostInr),
        service_fee_pct: String(SERVICE_FEE_RATE * 100),
        razorpay_fee_pct: String(RAZORPAY_FEE_RATE * 100),
        base_amount_inr: String(maintenanceTotals.baseInr),
        maintenance_fee_inr: String(maintenanceTotals.maintenanceFeeInr),
        purpose: 'payg',
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
      amount: totalAmount,
      baseAmount: maintenanceTotals.baseInr,
      maintenanceFee: maintenanceTotals.maintenanceFeeInr,
      currency: paymentLink.currency || 'INR',
      purpose: 'payg',
      rawJson: paymentLink.raw || null,
    });

    return NextResponse.json({
      success: true,
      data: {
        payment_link_id: paymentLink.id,
        short_url: paymentLink.shortUrl,
        amount: totalAmount,
        base_amount: maintenanceTotals.baseInr,
        maintenance_fee: maintenanceTotals.maintenanceFeeInr,
        currency: paymentLink.currency || 'INR',
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

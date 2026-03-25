import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import {
  createAdminPaymentLinkRecord,
  getAdminBillingSettings,
  getEffectiveRazorpayCredentials,
} from '../../../../lib/db-helpers';
import {
  DEFAULT_INPUT_USD_PER_1M,
  DEFAULT_OPENAI_MODEL,
  DEFAULT_OUTPUT_USD_PER_1M,
  DEFAULT_USD_TO_INR_RATE,
  MIN_PREPAID_TOPUP_INR,
  computeMaintenanceTotals,
} from '../../../../lib/billing.js';
import { createRazorpayPaymentLink, isRazorpayConfigured } from '../../../../lib/razorpay.js';

export const runtime = 'nodejs';

const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

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

const computeTokensFromAmount = ({ amountInr, usdPer1M, usdToInr }) => {
  const safeAmount = Math.max(0, toNumber(amountInr, 0));
  const pricePer1M = Math.max(0.0001, toNumber(usdPer1M, 0));
  const rate = Math.max(0.01, toNumber(usdToInr, DEFAULT_USD_TO_INR_RATE));
  const tokens = Math.floor((safeAmount / (pricePer1M * rate)) * 1_000_000);
  return Math.max(0, tokens);
};

export async function POST(request) {
  try {
    const user = await requireAuth();
    const body = await request.json();
    const amountInr = toNumber(body?.amount_inr, 0);

    if (!Number.isFinite(amountInr) || amountInr < MIN_PREPAID_TOPUP_INR) {
      return NextResponse.json(
        { success: false, error: `Minimum top-up is ₹${MIN_PREPAID_TOPUP_INR}.` },
        { status: 400 }
      );
    }

    const settings = await getAdminBillingSettings(user.id);
    const tokenSystemEnabled =
      user.admin_tier === 'super_admin' || settings?.charge_enabled === true;
    if (!tokenSystemEnabled) {
      return NextResponse.json(
        { success: false, error: 'Token billing is disabled for your account.' },
        { status: 403 }
      );
    }
    const inputUsdPer1M = Number(settings?.input_price_usd_per_1m ?? DEFAULT_INPUT_USD_PER_1M);
    const outputUsdPer1M = Number(settings?.output_price_usd_per_1m ?? DEFAULT_OUTPUT_USD_PER_1M);
    const usdToInr = DEFAULT_USD_TO_INR_RATE;

    const inputAmount = amountInr / 2;
    const outputAmount = amountInr - inputAmount;
    const inputTokens = computeTokensFromAmount({
      amountInr: inputAmount,
      usdPer1M: inputUsdPer1M,
      usdToInr,
    });
    const outputTokens = computeTokensFromAmount({
      amountInr: outputAmount,
      usdPer1M: outputUsdPer1M,
      usdToInr,
    });

    if (!inputTokens && !outputTokens) {
      return NextResponse.json(
        { success: false, error: 'Unable to calculate tokens for this amount.' },
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

    const maintenanceTotals = computeMaintenanceTotals({ baseInr: amountInr });
    const totalAmount = maintenanceTotals.totalInr;
    const description = `AlgoChat prepaid AI token pack for ${user.name || user.email || `Admin #${user.id}`}`;
    const paymentLink = await createRazorpayPaymentLink({
      amount: totalAmount,
      currency: 'INR',
      description: description.slice(0, 255),
      callbackUrl: buildBillingCallbackUrl(),
      callbackMethod:
        String(process.env.RAZORPAY_CALLBACK_METHOD || '').trim().toLowerCase() === 'post'
          ? 'post'
          : 'get',
      referenceId: `admin_${user.id}_prepaid_${Date.now()}`,
      credentials: collectorCreds,
      customer: {
        name: String(user?.name || '').trim(),
        email: String(user?.email || '').trim(),
        contact: String(user?.phone || '').trim().replace(/\D/g, ''),
      },
      notes: {
        admin_id: String(user.id),
        purpose: 'prepaid',
        model: DEFAULT_OPENAI_MODEL,
        input_usd_per_1m: String(inputUsdPer1M),
        output_usd_per_1m: String(outputUsdPer1M),
        usd_to_inr: String(usdToInr),
        base_amount_inr: String(maintenanceTotals.baseInr),
        maintenance_fee_inr: String(maintenanceTotals.maintenanceFeeInr),
        input_tokens: String(inputTokens),
        output_tokens: String(outputTokens),
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
      purpose: 'prepaid',
      inputTokens,
      outputTokens,
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
        input_tokens: inputTokens,
        output_tokens: outputTokens,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

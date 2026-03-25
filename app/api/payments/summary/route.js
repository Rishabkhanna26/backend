import { NextResponse } from 'next/server';
import { requireReadAuth } from '../../../../lib/auth-server';
import {
  getAdminById,
  getAdminBillingSettings,
  getAdminAiUsageSummary,
  getAdminPaygUsageSummary,
  getAdminPaymentTotals,
  getAdminTokenBalances,
  getDashboardChargeRates,
} from '../../../../lib/db-helpers';
import { normalizeBusinessType } from '../../../../lib/business.js';
import {
  DEFAULT_INPUT_USD_PER_1M,
  DEFAULT_OPENAI_MODEL,
  DEFAULT_OUTPUT_USD_PER_1M,
  DEFAULT_USD_TO_INR_RATE,
  FREE_INPUT_TOKEN_CAP,
  FREE_INPUT_TOKENS_PER_MONTH,
  FREE_OUTPUT_TOKEN_CAP,
  FREE_OUTPUT_TOKENS_PER_MONTH,
  MAINTENANCE_FEE_RATE,
  MIN_PREPAID_TOPUP_INR,
  RAZORPAY_FEE_RATE,
  SERVICE_FEE_RATE,
  computeMaintenanceTotals,
  computePaymentTotals,
} from '../../../../lib/billing.js';

export const runtime = 'nodejs';

const round = (value, digits = 2) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Number(num.toFixed(digits));
};

export async function GET() {
  try {
    const user = await requireReadAuth();
    const settings = await getAdminBillingSettings(user.id);
    const adminProfile = await getAdminById(user.id);
    const usage = await getAdminAiUsageSummary(user.id);
    const tokenBalances = await getAdminTokenBalances(user.id, { refresh: true });
    const payments = await getAdminPaymentTotals(user.id, { purpose: 'payg' });

    const monthStart = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), 1));
    const monthUsage = await getAdminPaygUsageSummary(user.id, { since: monthStart });

    const inputUsd = Number(settings?.input_price_usd_per_1m ?? DEFAULT_INPUT_USD_PER_1M);
    const outputUsd = Number(settings?.output_price_usd_per_1m ?? DEFAULT_OUTPUT_USD_PER_1M);
    const usdToInr = DEFAULT_USD_TO_INR_RATE;
    const totalTokens = Number(usage?.total_tokens || 0);

    const pricing = {
      model: DEFAULT_OPENAI_MODEL,
      input_usd_per_1m: inputUsd,
      output_usd_per_1m: outputUsd,
      usd_to_inr_rate: usdToInr,
      input_inr_per_1m: round(inputUsd * usdToInr, 2),
      output_inr_per_1m: round(outputUsd * usdToInr, 2),
      service_fee_pct: round(SERVICE_FEE_RATE * 100, 2),
      razorpay_fee_pct: round(RAZORPAY_FEE_RATE * 100, 2),
      maintenance_fee_pct: round(MAINTENANCE_FEE_RATE * 100, 2),
    };

    const baseCostInr = Number(usage?.total_cost_inr || 0);
    const totals = computePaymentTotals({ baseInr: baseCostInr });
    const totalPaid = Number(payments?.total_paid || 0);
    const totalDue = Math.max(Number(totals.totalInr) - totalPaid, 0);
    const maintenanceTotals = computeMaintenanceTotals({ baseInr: totalDue });

    const freeUntil = settings?.free_until ? new Date(settings.free_until) : null;
    const tokenSystemEnabled =
      user.admin_tier === 'super_admin' || settings?.charge_enabled === true;
    const isFreeBySetting =
      !tokenSystemEnabled ||
      (freeUntil && !Number.isNaN(freeUntil.getTime()) && freeUntil > new Date());
    const freeInputRemaining = Number(tokenBalances?.free_input_tokens || 0);
    const freeOutputRemaining = Number(tokenBalances?.free_output_tokens || 0);
    const paidInputRemaining = Number(tokenBalances?.paid_input_tokens || 0);
    const paidOutputRemaining = Number(tokenBalances?.paid_output_tokens || 0);
    const hasPrepaid = paidInputRemaining + paidOutputRemaining > 0;
    const hasFree = freeInputRemaining + freeOutputRemaining > 0;
    const isFree = isFreeBySetting || hasFree;

    const nextReset = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth() + 1, 1));

    const dashboardRates = await getDashboardChargeRates();
    const businessType = normalizeBusinessType(adminProfile?.business_type, 'both');
    const bookingEnabled = Boolean(adminProfile?.booking_enabled);
    const baseDashboardCharge =
      businessType === 'service'
        ? dashboardRates.service_inr
        : businessType === 'product'
        ? dashboardRates.product_inr
        : dashboardRates.both_inr;
    const bookingCharge = bookingEnabled ? dashboardRates.booking_inr : 0;
    const dashboardChargeEnabled = settings?.dashboard_charge_enabled !== false;
    const dashboardTotal = dashboardChargeEnabled ? baseDashboardCharge + bookingCharge : 0;
    const subscriptionExpiresAt = adminProfile?.dashboard_subscription_expires_at
      ? new Date(adminProfile.dashboard_subscription_expires_at)
      : null;
    const accessGrantAt = adminProfile?.access_expires_at
      ? new Date(adminProfile.access_expires_at)
      : null;
    const accessGrantActive =
      accessGrantAt && !Number.isNaN(accessGrantAt.getTime()) && accessGrantAt > new Date();
    const freeUntilActive =
      freeUntil && !Number.isNaN(freeUntil.getTime()) && freeUntil > new Date();
    const subscriptionActive =
      user.admin_tier === 'super_admin' ||
      accessGrantActive ||
      freeUntilActive ||
      !dashboardChargeEnabled ||
      (subscriptionExpiresAt && !Number.isNaN(subscriptionExpiresAt.getTime()) && subscriptionExpiresAt > new Date());

    return NextResponse.json({
      success: true,
      data: {
        token_system_enabled: tokenSystemEnabled,
        charge_enabled: tokenSystemEnabled,
        free_until: freeUntil ? freeUntil.toISOString() : null,
        is_free: Boolean(isFree),
        billing_state: !tokenSystemEnabled
          ? 'disabled'
          : isFreeBySetting
          ? 'free'
          : hasFree
          ? 'free'
          : hasPrepaid
          ? 'prepaid'
          : 'payg',
        razorpay_key_id: settings?.razorpay_key_id || '',
        razorpay_has_secret: Boolean(settings?.razorpay_key_secret),
        usage: {
          conversation_count: Number(usage?.conversation_count || 0),
          input_tokens: Number(usage?.input_tokens || 0),
          output_tokens: Number(usage?.output_tokens || 0),
          billable_input_tokens: Number(usage?.billable_input_tokens || 0),
          billable_output_tokens: Number(usage?.billable_output_tokens || 0),
          total_tokens: totalTokens,
          total_cost_usd: Number(usage?.total_cost_usd || 0),
          total_cost_inr: baseCostInr,
          first_used_at: usage?.first_used_at || null,
          last_used_at: usage?.last_used_at || null,
        },
        balances: {
          free_input_tokens: freeInputRemaining,
          free_output_tokens: freeOutputRemaining,
          paid_input_tokens: paidInputRemaining,
          paid_output_tokens: paidOutputRemaining,
          free_input_cap: FREE_INPUT_TOKEN_CAP,
          free_output_cap: FREE_OUTPUT_TOKEN_CAP,
          monthly_free_input: FREE_INPUT_TOKENS_PER_MONTH,
          monthly_free_output: FREE_OUTPUT_TOKENS_PER_MONTH,
          next_reset_at: nextReset.toISOString(),
        },
        payments: {
          total_paid_inr: totalPaid,
          last_paid_at: payments?.last_paid_at || null,
        },
        totals: {
          ...totals,
          total_due_inr: round(totalDue, 2),
          maintenance_fee_inr: maintenanceTotals.maintenanceFeeInr,
          total_due_with_maintenance_inr: maintenanceTotals.totalInr,
        },
        payg: {
          month_input_tokens: Number(monthUsage?.billable_input_tokens || 0),
          month_output_tokens: Number(monthUsage?.billable_output_tokens || 0),
          month_cost_usd: Number(monthUsage?.total_cost_usd || 0),
          month_cost_inr: Number(monthUsage?.total_cost_inr || 0),
          total_cost_inr: baseCostInr,
        },
        pricing,
        prepaid: {
          min_topup_inr: MIN_PREPAID_TOPUP_INR,
          split_ratio: 0.5,
        },
        dashboard: {
          charge_enabled: dashboardChargeEnabled,
          rates: dashboardRates,
          profile: {
            business_type: businessType,
            booking_enabled: bookingEnabled,
            service_label: adminProfile?.service_label || 'Service',
            product_label: adminProfile?.product_label || 'Product',
          },
          subscription: {
            expires_at: subscriptionExpiresAt && !Number.isNaN(subscriptionExpiresAt.getTime())
              ? subscriptionExpiresAt.toISOString()
              : null,
            active: Boolean(subscriptionActive),
          },
          amounts: {
            base_inr: round(baseDashboardCharge, 2),
            booking_inr: round(bookingCharge, 2),
            total_inr: round(dashboardTotal, 2),
          },
        },
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

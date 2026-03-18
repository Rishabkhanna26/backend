import { NextResponse } from 'next/server';
import { requireAuth, requireReadAuth } from '../../../../lib/auth-server';
import { getDashboardChargeRates, upsertAdminBillingSettings } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

const parseAmount = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return Number(num.toFixed(2));
};

export async function GET() {
  try {
    const user = await requireReadAuth();
    if (user.admin_tier !== 'super_admin') {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 403 });
    }
    const rates = await getDashboardChargeRates();
    return NextResponse.json({ success: true, data: rates });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function PUT(request) {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 403 });
    }
    const body = await request.json();
    const payload = {};
    if (Object.prototype.hasOwnProperty.call(body, 'service_inr')) {
      payload.dashboard_service_inr = parseAmount(body.service_inr);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'product_inr')) {
      payload.dashboard_product_inr = parseAmount(body.product_inr);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'both_inr')) {
      payload.dashboard_both_inr = parseAmount(body.both_inr);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'booking_inr')) {
      payload.dashboard_booking_inr = parseAmount(body.booking_inr);
    }
    const updated = await upsertAdminBillingSettings(user.id, payload);
    return NextResponse.json({
      success: true,
      data: {
        service_inr: Number(updated?.dashboard_service_inr || 0),
        product_inr: Number(updated?.dashboard_product_inr || 0),
        both_inr: Number(updated?.dashboard_both_inr || 0),
        booking_inr: Number(updated?.dashboard_booking_inr || 0),
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../../lib/auth-server';
import { upsertAdminBillingSettings } from '../../../../../lib/db-helpers';

export const runtime = 'nodejs';

const parseBoolean = (value) => {
  if (value === true || value === false) return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
  }
  return null;
};

const parseNumber = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return num;
};

export async function PUT(request, context) {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 403 });
    }
    const params = await context.params;
    const adminId = Number(params?.id);
    if (!Number.isFinite(adminId) || adminId <= 0) {
      return NextResponse.json({ success: false, error: 'Invalid admin id.' }, { status: 400 });
    }
    const body = await request.json();
    const payload = {};

    if (Object.prototype.hasOwnProperty.call(body, 'charge_enabled')) {
      const parsed = parseBoolean(body.charge_enabled);
      if (parsed !== null) payload.charge_enabled = parsed;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'dashboard_charge_enabled')) {
      const parsed = parseBoolean(body.dashboard_charge_enabled);
      if (parsed !== null) payload.dashboard_charge_enabled = parsed;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'free_until')) {
      payload.free_until = body.free_until || null;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'free_days')) {
      const days = parseNumber(body.free_days);
      if (days !== null) {
        const until = new Date(Date.now() + days * 24 * 60 * 60 * 1000);
        payload.free_until = until;
      }
    }
    if (Object.prototype.hasOwnProperty.call(body, 'input_price_usd_per_1m')) {
      payload.input_price_usd_per_1m = parseNumber(body.input_price_usd_per_1m);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'output_price_usd_per_1m')) {
      payload.output_price_usd_per_1m = parseNumber(body.output_price_usd_per_1m);
    }

    const updated = await upsertAdminBillingSettings(adminId, payload);
    return NextResponse.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

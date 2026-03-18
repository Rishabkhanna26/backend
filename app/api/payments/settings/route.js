import { NextResponse } from 'next/server';
import { requireAuth, requireReadAuth } from '../../../../lib/auth-server';
import { getAdminBillingSettings, upsertAdminBillingSettings } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

const normalizeKey = (value) => String(value || '').trim();

export async function GET() {
  try {
    const user = await requireReadAuth();
    const settings = await getAdminBillingSettings(user.id);
    return NextResponse.json({
      success: true,
      data: {
        razorpay_key_id: settings?.razorpay_key_id || '',
        razorpay_has_secret: Boolean(settings?.razorpay_key_secret),
      },
    });
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
    const body = await request.json();
    const payload = {};
    if (Object.prototype.hasOwnProperty.call(body, 'razorpay_key_id')) {
      payload.razorpay_key_id = normalizeKey(body.razorpay_key_id);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'razorpay_key_secret')) {
      payload.razorpay_key_secret = normalizeKey(body.razorpay_key_secret);
    }
    const updated = await upsertAdminBillingSettings(user.id, payload);
    return NextResponse.json({
      success: true,
      data: {
        razorpay_key_id: updated?.razorpay_key_id || '',
        razorpay_has_secret: Boolean(updated?.razorpay_key_secret),
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

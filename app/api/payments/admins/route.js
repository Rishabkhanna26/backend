import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import { getConnection } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 403 });
    }
    const connection = await getConnection();
    try {
      const [rows] = await connection.query(
        `
          SELECT
            a.id,
            a.name,
            a.email,
            a.phone,
            a.admin_tier,
            a.status,
            a.access_expires_at,
            a.business_type,
            a.booking_enabled,
            a.service_label,
            a.product_label,
            a.dashboard_subscription_expires_at,
            CASE
              WHEN a.admin_tier = 'super_admin' THEN TRUE
              ELSE COALESCE(s.charge_enabled, FALSE)
            END AS charge_enabled,
            s.free_until,
            s.input_price_usd_per_1m,
            s.output_price_usd_per_1m,
            s.dashboard_charge_enabled
          FROM admins a
          LEFT JOIN admin_billing_settings s ON s.admin_id = a.id
          ORDER BY a.created_at DESC
        `
      );
      return NextResponse.json({ success: true, data: rows || [] });
    } finally {
      connection.release();
    }
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

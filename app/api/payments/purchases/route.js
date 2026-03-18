import { NextResponse } from 'next/server';
import { requireReadAuth } from '../../../../lib/auth-server';
import { getConnection } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const user = await requireReadAuth();
    const connection = await getConnection();
    try {
      const params = [];
      const whereClause =
        user.admin_tier === 'super_admin' ? '' : 'WHERE p.admin_id = ?';
      if (user.admin_tier !== 'super_admin') {
        params.push(user.id);
      }
      const [rows] = await connection.query(
        `
          SELECT
            p.id,
            p.admin_id,
            a.name AS admin_name,
            a.email AS admin_email,
            a.phone AS admin_phone,
            p.payment_link_id,
            p.amount,
            p.base_amount,
            p.maintenance_fee,
            p.currency,
            p.purpose,
            p.input_tokens,
            p.output_tokens,
            p.subscription_months,
            p.discount_pct,
            p.dashboard_monthly_amount,
            p.status,
            p.paid_amount,
            p.paid_at,
            p.created_at
          FROM admin_payment_links p
          JOIN admins a ON a.id = p.admin_id
          ${whereClause}
          ORDER BY p.created_at DESC
        `,
        params
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

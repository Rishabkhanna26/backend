import { NextResponse } from 'next/server';
import { requireReadAuth } from '../../../../lib/auth-server';
import { getAdminBillingSettings, getConnection } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const user = await requireReadAuth();
    let tokenSystemEnabled = true;
    if (user.admin_tier !== 'super_admin') {
      const settings = await getAdminBillingSettings(user.id);
      tokenSystemEnabled = settings?.charge_enabled === true;
    }
    const connection = await getConnection();
    try {
      const params = [];
      const whereParts = [];
      if (user.admin_tier !== 'super_admin') {
        whereParts.push('p.admin_id = ?');
        params.push(user.id);
        if (!tokenSystemEnabled) {
          whereParts.push("(p.purpose = 'dashboard' OR p.purpose = 'business_type_change')");
        }
      }
      const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';
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

export async function DELETE(request) {
  try {
    const user = await requireReadAuth();
    const body = await request.json().catch(() => ({}));
    const purchaseId = Number(body?.purchase_id);
    if (!Number.isInteger(purchaseId) || purchaseId <= 0) {
      return NextResponse.json(
        { success: false, error: 'purchase_id is required.' },
        { status: 400 }
      );
    }

    const connection = await getConnection();
    try {
      const [rows] = await connection.query(
        `
          SELECT id, admin_id
          FROM admin_payment_links
          WHERE id = ?
          LIMIT 1
        `,
        [purchaseId]
      );
      const existing = rows?.[0] || null;
      if (!existing) {
        return NextResponse.json(
          { success: false, error: 'Payment history not found.' },
          { status: 404 }
        );
      }
      if (user.admin_tier !== 'super_admin' && Number(existing.admin_id) !== Number(user.id)) {
        return NextResponse.json(
          { success: false, error: 'Forbidden' },
          { status: 403 }
        );
      }

      await connection.query(
        `
          DELETE FROM admin_payment_links
          WHERE id = ?
        `,
        [purchaseId]
      );
      return NextResponse.json({ success: true });
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

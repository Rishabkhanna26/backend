import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import { getBusinessTypeChangeRequests } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function GET(request) {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 403 });
    }

    const { searchParams } = new URL(request.url);
    const status = String(searchParams.get('status') || '').trim().toLowerCase();
    const allowedStatus = new Set(['pending', 'approved', 'rejected', 'cancelled']);
    const normalizedStatus = allowedStatus.has(status) ? status : null;

    const rows = await getBusinessTypeChangeRequests({ status: normalizedStatus });
    return NextResponse.json({ success: true, data: rows || [] });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

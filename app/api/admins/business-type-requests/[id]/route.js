import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../../lib/auth-server';
import {
  approveBusinessTypeChangeRequest,
  rejectBusinessTypeChangeRequest,
} from '../../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function PATCH(request, context) {
  try {
    const user = await requireAuth();
    if (user.admin_tier !== 'super_admin') {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 403 });
    }

    const params = await context.params;
    const requestId = Number(params?.id);
    if (!Number.isFinite(requestId)) {
      return NextResponse.json({ success: false, error: 'Invalid request id' }, { status: 400 });
    }

    const body = await request.json().catch(() => ({}));
    const action = String(body?.action || '').trim().toLowerCase();
    if (!['approve', 'reject'].includes(action)) {
      return NextResponse.json({ success: false, error: 'Invalid action' }, { status: 400 });
    }

    const resolved =
      action === 'approve'
        ? await approveBusinessTypeChangeRequest({ requestId, resolvedBy: user.id })
        : await rejectBusinessTypeChangeRequest({ requestId, resolvedBy: user.id });

    if (!resolved) {
      return NextResponse.json({ success: false, error: 'Request not found or already resolved.' }, { status: 404 });
    }

    return NextResponse.json({ success: true, data: resolved });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

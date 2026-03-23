import { requireAuth, requireReadAuth } from '../../../lib/auth-server';
import { parsePagination } from '../../../lib/api-utils';
import { createOrder, getOrders } from '../../../lib/db-helpers';
import { hasProductAccess } from '../../../lib/business.js';

const ALLOWED_STATUSES = new Set([
  'new',
  'confirmed',
  'processing',
  'packed',
  'out_for_delivery',
  'fulfilled',
  'cancelled',
  'refunded',
]);
const ALLOWED_FULFILLMENT = new Set([
  'unfulfilled',
  'packed',
  'shipped',
  'delivered',
  'cancelled',
]);
const ALLOWED_PAYMENT = new Set(['pending', 'paid', 'failed', 'refunded']);
const ALLOWED_PAYMENT_METHODS = new Set(['cash', 'card', 'upi', 'bank', 'wallet', 'other', '']);

export async function GET(request) {
  try {
    const user = await requireReadAuth();
    if (!user.restricted_mode && !hasProductAccess(user)) {
      return Response.json(
        { success: false, error: 'Orders are disabled for this business type.' },
        { status: 403 }
      );
    }
    const { searchParams } = new URL(request.url);
    const { limit, offset } = parsePagination(searchParams, { defaultLimit: 200, maxLimit: 500 });
    const adminScopeId = user.admin_tier === 'super_admin' ? null : user.id;

    const orders = await getOrders(adminScopeId, { limit: limit + 1, offset });
    const hasMore = orders.length > limit;

    return Response.json({
      success: true,
      data: hasMore ? orders.slice(0, limit) : orders,
      meta: {
        limit,
        offset,
        hasMore,
        nextOffset: hasMore ? offset + limit : null,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function POST(request) {
  try {
    const user = await requireAuth();
    if (!hasProductAccess(user)) {
      return Response.json(
        { success: false, error: 'Orders are disabled for this business type.' },
        { status: 403 }
      );
    }

    const body = await request.json();
    const customerName = String(body?.customer_name || '').trim();
    const customerPhone = String(body?.customer_phone || '').trim();
    if (!customerName && !customerPhone) {
      return Response.json(
        { success: false, error: 'Customer name or phone is required.' },
        { status: 400 }
      );
    }

    const items = Array.isArray(body?.items) ? body.items : [];
    const hasItem = items.some((item) => String(item?.name || '').trim());
    if (!hasItem) {
      return Response.json({ success: false, error: 'Add at least one item.' }, { status: 400 });
    }

    const status = String(body?.status || 'new');
    if (!ALLOWED_STATUSES.has(status)) {
      return Response.json({ success: false, error: 'Invalid status.' }, { status: 400 });
    }

    const fulfillment = String(body?.fulfillment_status || 'unfulfilled');
    if (!ALLOWED_FULFILLMENT.has(fulfillment)) {
      return Response.json({ success: false, error: 'Invalid fulfillment status.' }, { status: 400 });
    }

    const paymentStatus = String(body?.payment_status || 'pending');
    if (!ALLOWED_PAYMENT.has(paymentStatus)) {
      return Response.json({ success: false, error: 'Invalid payment status.' }, { status: 400 });
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_method')) {
      const method = String(body?.payment_method || '');
      if (!ALLOWED_PAYMENT_METHODS.has(method)) {
        return Response.json({ success: false, error: 'Invalid payment method.' }, { status: 400 });
      }
    }

    const created = await createOrder({
      admin_id: user.id,
      order_number: body?.order_number,
      customer_name: customerName,
      customer_phone: customerPhone,
      customer_email: String(body?.customer_email || '').trim() || null,
      channel: body?.channel,
      status,
      fulfillment_status: fulfillment,
      payment_status: paymentStatus,
      payment_method: body?.payment_method,
      payment_total: body?.payment_total,
      payment_paid: body?.payment_paid,
      payment_currency: body?.payment_currency,
      payment_notes: body?.payment_notes,
      payment_transaction_id: body?.payment_transaction_id,
      payment_gateway_payment_id: body?.payment_gateway_payment_id,
      payment_link_id: body?.payment_link_id,
      delivery_method: body?.delivery_method,
      delivery_address: body?.delivery_address,
      items,
      notes: Array.isArray(body?.notes) ? body.notes : [],
      placed_at: body?.placed_at,
    });

    if (!created) {
      return Response.json({ success: false, error: 'Unable to create order.' }, { status: 500 });
    }

    return Response.json({ success: true, data: created });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

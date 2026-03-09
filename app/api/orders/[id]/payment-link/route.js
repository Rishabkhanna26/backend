import { requireAuth } from '../../../../../lib/auth-server';
import {
  getOrderById,
  getOrderPaymentLinkTimer,
  getUserByPhone,
  scheduleOrderPaymentLinkTimer,
  updateOrder,
} from '../../../../../lib/db-helpers';
import { hasProductAccess } from '../../../../../lib/business.js';
import { signAuthToken } from '../../../../../lib/auth';
import {
  createRazorpayPaymentLink,
  isRazorpayConfigured,
  normalizeRazorpayCurrency,
} from '../../../../../lib/razorpay.js';

export const runtime = 'nodejs';

const WHATSAPP_API_BASE =
  process.env.WHATSAPP_API_BASE ||
  process.env.NEXT_PUBLIC_WHATSAPP_API_BASE ||
  'http://localhost:3001';
const BACKEND_TOKEN_TTL_SECONDS = 10 * 60;

const toTrimmed = (value) => String(value || '').trim();
const normalizePhone = (value) => String(value || '').replace(/\D/g, '');

const parseScheduledFor = (value) => {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return date;
};

const parseTimerMinutes = (value) => {
  if (value === undefined || value === null || value === '') return null;
  const minutes = Number(value);
  if (!Number.isFinite(minutes)) return NaN;
  const rounded = Math.round(minutes);
  if (!Number.isFinite(rounded) || rounded <= 0 || rounded > 7 * 24 * 60) return NaN;
  return rounded;
};

const formatCurrency = (value = 0, currency = 'INR') => {
  const numeric = Number(value);
  const safeValue = Number.isFinite(numeric) ? numeric : 0;
  try {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency,
      maximumFractionDigits: 2,
    }).format(safeValue);
  } catch (_error) {
    return `${currency} ${safeValue.toFixed(2)}`;
  }
};

const getRemainingAmount = (order = {}) => {
  const total = Number(order?.payment_total);
  const paid = Number(order?.payment_paid);
  if (!Number.isFinite(total) || total <= 0) return 0;
  const safePaid = Number.isFinite(paid) && paid > 0 ? paid : 0;
  const due = total - safePaid;
  return Number.isFinite(due) && due > 0 ? Number(due.toFixed(2)) : 0;
};

const buildCallbackUrl = () => {
  const explicit = toTrimmed(process.env.RAZORPAY_CALLBACK_URL);
  if (explicit) return explicit;
  const frontendOrigin =
    toTrimmed(process.env.FRONTEND_ORIGIN) ||
    toTrimmed(process.env.PUBLIC_URL) ||
    toTrimmed(process.env.RENDER_EXTERNAL_URL) ||
    'http://localhost:3000';
  try {
    return new URL('/payment/success', frontendOrigin).toString();
  } catch (_error) {
    return '';
  }
};

const buildReferenceId = ({ adminId, orderId }) =>
  toTrimmed(`due_${adminId || 0}_${orderId || 0}_${Date.now()}`)
    .replace(/[^a-zA-Z0-9._-]/g, '')
    .slice(0, 40);

const appendPaymentLinkNote = ({
  currentNotes = '',
  linkId = '',
  shortUrl = '',
  amount = 0,
  currency = 'INR',
}) => {
  const notes = [];
  const existing = toTrimmed(currentNotes);
  if (existing) notes.push(existing);
  const stampedAt = new Date().toISOString();
  notes.push(
    `[${stampedAt}] Remaining payment link sent. Razorpay Link ${linkId}. Amount: ${formatCurrency(
      amount,
      currency
    )}. URL: ${shortUrl}`
  );
  return notes.join('\n');
};

const buildPaymentReminderMessage = ({ order = {}, dueAmount = 0, currency = 'INR', paymentLinkUrl = '' }) => {
  const orderRef = order?.order_number ? `Order ${order.order_number}` : `Order #${order?.id || ''}`;
  const customerName = toTrimmed(order?.customer_name) || 'Customer';
  return [
    `Hi ${customerName},`,
    `Please complete the remaining payment of ${formatCurrency(dueAmount, currency)} for ${orderRef}.`,
    `Payment link: ${paymentLinkUrl}`,
    'After payment, you will be redirected to the confirmation page.',
    'If you face any issue, reply with your transaction ID and screenshot.',
  ].join('\n');
};

export async function POST(request, context) {
  try {
    const authUser = await requireAuth();
    if (!hasProductAccess(authUser)) {
      return Response.json(
        { success: false, error: 'Orders are disabled for this business type.' },
        { status: 403 }
      );
    }

    const params = await context.params;
    const orderId = Number(params?.id);
    if (!Number.isFinite(orderId) || orderId <= 0) {
      return Response.json({ success: false, error: 'Invalid order id' }, { status: 400 });
    }

    if (!isRazorpayConfigured()) {
      return Response.json(
        { success: false, error: 'Razorpay is not configured. Add key id and key secret in env.' },
        { status: 400 }
      );
    }

    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const order = await getOrderById(orderId, adminScopeId);
    if (!order) {
      return Response.json({ success: false, error: 'Order not found' }, { status: 404 });
    }

    const customerPhone = normalizePhone(order?.customer_phone);
    if (!customerPhone) {
      return Response.json(
        { success: false, error: 'Order does not have a valid customer phone number.' },
        { status: 400 }
      );
    }

    let contact = await getUserByPhone(customerPhone, Number(order?.admin_id || 0));
    if (!contact?.id) {
      contact = await getUserByPhone(customerPhone);
    }
    const dueAmount = getRemainingAmount(order);
    if (!Number.isFinite(dueAmount) || dueAmount <= 0) {
      return Response.json({ success: false, error: 'No remaining amount to collect for this order.' }, { status: 400 });
    }

    const body = await request.json().catch(() => ({}));
    let scheduledFor = parseScheduledFor(body?.scheduled_for);
    if (body?.scheduled_for && !scheduledFor) {
      return Response.json(
        { success: false, error: 'Invalid scheduled time.' },
        { status: 400 }
      );
    }

    const timerMinutes = parseTimerMinutes(body?.timer_minutes);
    if (Number.isNaN(timerMinutes)) {
      return Response.json(
        { success: false, error: 'Invalid timer value. Use minutes between 1 and 10080.' },
        { status: 400 }
      );
    }
    if (!scheduledFor && Number.isFinite(timerMinutes)) {
      scheduledFor = new Date(Date.now() + timerMinutes * 60 * 1000);
    }

    if (scheduledFor) {
      const now = Date.now();
      if (scheduledFor.getTime() <= now + 15 * 1000) {
        return Response.json(
          { success: false, error: 'Scheduled time must be at least 15 seconds in the future.' },
          { status: 400 }
        );
      }
      const timer = await scheduleOrderPaymentLinkTimer({
        orderId: order.id,
        adminId: Number(order?.admin_id) || authUser.id,
        scheduledFor,
        createdBy: authUser.id,
        payload: {
          due_amount: dueAmount,
          customer_phone: customerPhone,
          ...(Number.isFinite(timerMinutes) ? { timer_minutes: timerMinutes } : {}),
        },
      });
      const currentTimer = timer || (await getOrderPaymentLinkTimer(order.id, Number(order?.admin_id) || authUser.id));
      return Response.json({
        success: true,
        data: {
          order,
          timer: currentTimer,
          message: `Payment link scheduled for ${scheduledFor.toISOString()}`,
        },
      });
    }

    const currency = normalizeRazorpayCurrency(order?.payment_currency || process.env.RAZORPAY_CURRENCY || 'INR');
    const callbackUrl = buildCallbackUrl();
    const callbackMethod = toTrimmed(process.env.RAZORPAY_CALLBACK_METHOD).toLowerCase() === 'post' ? 'post' : 'get';
    const orderRef = order?.order_number ? `Order ${order.order_number}` : `Order #${order.id}`;
    const baseDescription = toTrimmed(process.env.RAZORPAY_PAYMENT_DESCRIPTION) || 'WhatsApp order payment';
    const description = `${baseDescription} (${orderRef})`.slice(0, 255);

    let paymentLink = null;
    try {
      paymentLink = await createRazorpayPaymentLink({
        amount: dueAmount,
        currency,
        description,
        callbackUrl,
        callbackMethod,
        referenceId: buildReferenceId({ adminId: order?.admin_id, orderId: order.id }),
        customer: {
          name: toTrimmed(order?.customer_name),
          contact: customerPhone,
          email: toTrimmed(order?.customer_email),
        },
        notes: {
          order_id: String(order.id),
          order_number: toTrimmed(order?.order_number || `#${order.id}`),
          admin_id: String(order?.admin_id || ''),
          payment_type: 'remaining',
          amount_due: String(dueAmount),
        },
      });
    } catch (error) {
      return Response.json(
        { success: false, error: error?.message || 'Failed to create payment link.' },
        { status: 502 }
      );
    }

    const paymentLinkUrl = toTrimmed(paymentLink?.shortUrl);
    if (!paymentLinkUrl || !paymentLink?.id) {
      return Response.json(
        { success: false, error: 'Razorpay returned an invalid payment link response.' },
        { status: 502 }
      );
    }

    const message = buildPaymentReminderMessage({ order, dueAmount, currency, paymentLinkUrl });
    const backendToken = signAuthToken(
      {
        id: authUser.id,
        admin_tier: authUser.admin_tier,
        scope: 'backend',
      },
      { expiresIn: `${BACKEND_TOKEN_TTL_SECONDS}s` }
    );

    let whatsappPayload = null;
    try {
      const whatsappResponse = await fetch(`${WHATSAPP_API_BASE}/whatsapp/send`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${backendToken}`,
        },
        body: JSON.stringify({
          adminId: Number(order?.admin_id) || authUser.id,
          ...(contact?.id ? { userId: Number(contact.id) } : { phone: customerPhone }),
          message,
        }),
      });
      whatsappPayload = await whatsappResponse.json().catch(() => null);
      if (!whatsappResponse.ok || whatsappPayload?.success === false) {
        return Response.json(
          { success: false, error: whatsappPayload?.error || 'Failed to send payment link on WhatsApp.' },
          { status: whatsappResponse.status || 502 }
        );
      }
    } catch (_error) {
      return Response.json({ success: false, error: 'WhatsApp service unavailable.' }, { status: 502 });
    }

    const paymentNotes = appendPaymentLinkNote({
      currentNotes: order?.payment_notes,
      linkId: paymentLink.id,
      shortUrl: paymentLinkUrl,
      amount: dueAmount,
      currency,
    });
    const updatedOrder = await updateOrder(
      order.id,
      {
        payment_notes: paymentNotes,
        payment_currency: currency,
        payment_link_id: paymentLink.id,
      },
      adminScopeId
    );

    return Response.json({
      success: true,
      data: {
        order: updatedOrder || { ...order, payment_notes: paymentNotes, payment_currency: currency },
        payment_link: {
          id: paymentLink.id,
          short_url: paymentLinkUrl,
          amount: dueAmount,
          currency,
        },
        destination: {
          mode: contact?.id ? 'contact' : 'phone_fallback',
          phone: customerPhone,
          user_id: contact?.id || null,
        },
        whatsapp: whatsappPayload?.data || null,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

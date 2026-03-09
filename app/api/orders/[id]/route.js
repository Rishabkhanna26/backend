import { requireAuth } from '../../../../lib/auth-server';
import { deleteOrder, getOrderById, updateOrder } from '../../../../lib/db-helpers';
import { hasProductAccess } from '../../../../lib/business.js';
import { isRazorpayConfigured, verifyRazorpayPaymentLink } from '../../../../lib/razorpay.js';

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

const extractPaymentProofId = (value) => {
  const text = String(value || '').trim();
  if (!text) return '';
  const tagged = text.match(
    /(upi transaction id|transaction id|txn id|payment id|utr|rrn)\s*[:#-]?\s*([a-zA-Z0-9._-]{6,80})/i
  );
  if (tagged?.[2]) return tagged[2].trim();
  const generic = text.match(/\b[a-zA-Z0-9._-]{10,80}\b/g) || [];
  if (!generic.length) return '';
  generic.sort((a, b) => b.length - a.length);
  return String(generic[0] || '').trim();
};

const extractPaymentLinkId = (value) => {
  const text = String(value || '').trim();
  if (!text) return '';
  const matches = [];
  const pushMatches = (regex, extractor = (m) => m?.[0]) => {
    const all = text.matchAll(regex);
    for (const match of all) {
      const extracted = String(extractor(match) || '').trim();
      if (extracted) matches.push(extracted);
    }
  };

  pushMatches(/\bplink_[a-zA-Z0-9]+\b/gi);
  pushMatches(/Razorpay Link\s+([a-zA-Z0-9_]+)/gi, (m) => m?.[1]);
  pushMatches(/razorpay_payment_link_id[=:]([a-zA-Z0-9_]+)/gi, (m) => m?.[1]);

  if (!matches.length) return '';
  return matches[matches.length - 1];
};

const formatCurrency = (value = 0, currency = 'INR') => {
  const amount = Number(value);
  const safe = Number.isFinite(amount) ? amount : 0;
  try {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency,
      maximumFractionDigits: 2,
    }).format(safe);
  } catch (_error) {
    return `${currency} ${safe.toFixed(2)}`;
  }
};

const toTrimmed = (value) => {
  const text = String(value || '').trim();
  return text || '';
};

const buildVerifiedPaymentNotes = ({
  baseNotes = '',
  verification,
  proofId = '',
  linkId = '',
  accumulatedPaid = null,
}) => {
  const parts = [];
  const existing = String(baseNotes || '').trim();
  if (existing) parts.push(existing);
  parts.push('Manual payment verification completed.');
  if (linkId) parts.push(`Razorpay Link ${linkId}.`);
  if (proofId) parts.push(`Proof ID: ${proofId}.`);
  if (verification?.transactionId) parts.push(`Transaction ID: ${verification.transactionId}.`);
  if (verification?.paymentId) parts.push(`Payment ID: ${verification.paymentId}.`);
  if (Number.isFinite(Number(verification?.paidAmount)) && Number(verification.paidAmount) > 0) {
    parts.push(
      `Verified amount: ${formatCurrency(
        Number(verification.paidAmount),
        verification?.currency || 'INR'
      )}.`
    );
  }
  if (Number.isFinite(Number(accumulatedPaid)) && Number(accumulatedPaid) >= 0) {
    parts.push(
      `Recorded paid total: ${formatCurrency(
        Number(accumulatedPaid),
        verification?.currency || 'INR'
      )}.`
    );
  }
  if (verification?.paidAt) parts.push(`Paid at: ${verification.paidAt}.`);
  return parts.join(' ');
};

export async function PATCH(request, context) {
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
    if (!Number.isFinite(orderId)) {
      return Response.json({ success: false, error: 'Invalid order id' }, { status: 400 });
    }

    const body = await request.json();
    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const currentOrder = await getOrderById(orderId, adminScopeId);
    if (!currentOrder) {
      return Response.json({ success: false, error: 'Order not found' }, { status: 404 });
    }
    const updates = {};

    if (Object.prototype.hasOwnProperty.call(body, 'status')) {
      const status = String(body?.status || '');
      if (!ALLOWED_STATUSES.has(status)) {
        return Response.json({ success: false, error: 'Invalid status' }, { status: 400 });
      }
      updates.status = status;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'fulfillment_status')) {
      const fulfillment = String(body?.fulfillment_status || '');
      if (!ALLOWED_FULFILLMENT.has(fulfillment)) {
        return Response.json({ success: false, error: 'Invalid fulfillment status' }, { status: 400 });
      }
      updates.fulfillment_status = fulfillment;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_status')) {
      const paymentStatus = String(body?.payment_status || '');
      if (!ALLOWED_PAYMENT.has(paymentStatus)) {
        return Response.json({ success: false, error: 'Invalid payment status' }, { status: 400 });
      }
      updates.payment_status = paymentStatus;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_total')) {
      updates.payment_total = body?.payment_total;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_paid')) {
      updates.payment_paid = body?.payment_paid;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_method')) {
      const method = String(body?.payment_method || '');
      if (!ALLOWED_PAYMENT_METHODS.has(method)) {
        return Response.json({ success: false, error: 'Invalid payment method' }, { status: 400 });
      }
      updates.payment_method = method || null;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'payment_notes')) {
      updates.payment_notes = String(body?.payment_notes || '').trim() || null;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'payment_transaction_id')) {
      updates.payment_transaction_id = toTrimmed(body?.payment_transaction_id) || null;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'payment_gateway_payment_id')) {
      updates.payment_gateway_payment_id = toTrimmed(body?.payment_gateway_payment_id) || null;
    }
    if (Object.prototype.hasOwnProperty.call(body, 'payment_link_id')) {
      updates.payment_link_id = toTrimmed(body?.payment_link_id) || null;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'assigned_to')) {
      updates.assigned_to = String(body?.assigned_to || '').trim() || null;
    }

    if (Object.prototype.hasOwnProperty.call(body, 'notes')) {
      updates.notes = Array.isArray(body?.notes) ? body.notes : null;
    }

    if (updates.payment_status === 'paid') {
      if (!isRazorpayConfigured()) {
        return Response.json(
          {
            success: false,
            error:
              'Manual mark as paid is blocked. Razorpay verification is required but credentials are missing.',
          },
          { status: 400 }
        );
      }

      const proofId =
        extractPaymentProofId(body?.payment_proof_id) ||
        extractPaymentProofId(body?.payment_notes) ||
        extractPaymentProofId(currentOrder?.payment_notes);

      const linkId =
        toTrimmed(body?.payment_link_id) ||
        toTrimmed(currentOrder?.payment_link_id) ||
        extractPaymentLinkId(body?.payment_link_id) ||
        extractPaymentLinkId(body?.payment_notes) ||
        extractPaymentLinkId(currentOrder?.payment_notes);
      if (!linkId) {
        return Response.json(
          {
            success: false,
            error:
              'Manual mark as paid is blocked. Razorpay payment link ID is missing, so transaction cannot be verified.',
          },
          { status: 400 }
        );
      }

      let verification = null;
      const expectedTotal = Number.isFinite(Number(updates.payment_total))
        ? Number(updates.payment_total)
        : Number(currentOrder?.payment_total);
      const currentPaidBase =
        Number.isFinite(Number(currentOrder?.payment_paid)) && Number(currentOrder?.payment_paid) > 0
          ? Number(currentOrder.payment_paid)
          : 0;
      const expectedRemaining =
        Number.isFinite(expectedTotal) && expectedTotal > 0
          ? Number(Math.max(expectedTotal - currentPaidBase, 0).toFixed(2))
          : null;
      const expectedForVerification =
        Number.isFinite(expectedRemaining) && expectedRemaining > 0
          ? expectedRemaining
          : Number.isFinite(expectedTotal) && expectedTotal > 0
          ? expectedTotal
          : null;

      try {
        verification = await verifyRazorpayPaymentLink({
          paymentLinkId: linkId,
          expectedAmount: expectedForVerification,
          proofId,
        });
      } catch (error) {
        return Response.json(
          {
            success: false,
            error: `Payment verification failed: ${error?.message || 'Unknown error'}`,
          },
          { status: 400 }
        );
      }

      if (!verification?.verified) {
        return Response.json(
          {
            success: false,
            error:
              'Manual mark as paid is blocked. Payment could not be verified from Razorpay link.',
          },
          { status: 400 }
        );
      }

      const verifiedLinkAmount = Number(verification?.paidAmount || 0);
      const hasVerifiedAmount = Number.isFinite(verifiedLinkAmount) && verifiedLinkAmount > 0;
      const existingNotesLower = [
        currentOrder?.payment_notes,
        currentOrder?.payment_transaction_id,
        currentOrder?.payment_gateway_payment_id,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      const transactionId = String(verification?.transactionId || '').trim();
      const paymentId = String(verification?.paymentId || '').trim();
      const alreadyRecorded = [transactionId, paymentId]
        .filter(Boolean)
        .some((token) => existingNotesLower.includes(String(token).toLowerCase()));

      let accumulatedPaid = currentPaidBase;
      if (hasVerifiedAmount && !alreadyRecorded) {
        if (Number.isFinite(expectedRemaining) && expectedRemaining > 0) {
          accumulatedPaid = Number((currentPaidBase + verifiedLinkAmount).toFixed(2));
        } else {
          accumulatedPaid = Math.max(currentPaidBase, verifiedLinkAmount);
        }
      }
      if (Number.isFinite(expectedTotal) && expectedTotal > 0) {
        accumulatedPaid = Math.min(accumulatedPaid, Number(expectedTotal.toFixed(2)));
      }
      accumulatedPaid = Number.isFinite(accumulatedPaid) && accumulatedPaid >= 0 ? accumulatedPaid : 0;

      const isPartial =
        Number.isFinite(expectedTotal) &&
        expectedTotal > 0 &&
        accumulatedPaid + 0.01 < expectedTotal;

      if (isPartial) {
        updates.payment_status = 'pending';
      }
      updates.payment_paid = accumulatedPaid;
      updates.payment_currency = verification?.currency || currentOrder?.payment_currency || 'INR';
      updates.payment_notes = buildVerifiedPaymentNotes({
        baseNotes: updates.payment_notes || currentOrder?.payment_notes || '',
        verification,
        proofId,
        linkId,
        accumulatedPaid,
      });
      updates.payment_transaction_id =
        toTrimmed(verification?.transactionId) ||
        toTrimmed(proofId) ||
        toTrimmed(currentOrder?.payment_transaction_id) ||
        null;
      updates.payment_gateway_payment_id =
        toTrimmed(verification?.paymentId) ||
        toTrimmed(currentOrder?.payment_gateway_payment_id) ||
        null;
      updates.payment_link_id =
        toTrimmed(verification?.linkId) ||
        toTrimmed(linkId) ||
        toTrimmed(currentOrder?.payment_link_id) ||
        null;
      if (!updates.payment_method) {
        updates.payment_method = currentOrder?.payment_method || 'upi';
      }
    }

    if (Object.keys(updates).length === 0) {
      return Response.json({ success: false, error: 'No updates provided' }, { status: 400 });
    }

    const updated = await updateOrder(orderId, updates, adminScopeId);
    if (!updated) {
      return Response.json({ success: false, error: 'Order not found' }, { status: 404 });
    }

    return Response.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function DELETE(request, context) {
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
    if (!Number.isFinite(orderId)) {
      return Response.json({ success: false, error: 'Invalid order id' }, { status: 400 });
    }

    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const deleted = await deleteOrder(orderId, adminScopeId);
    if (!deleted) {
      return Response.json({ success: false, error: 'Order not found' }, { status: 404 });
    }

    return Response.json({ success: true, data: deleted });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

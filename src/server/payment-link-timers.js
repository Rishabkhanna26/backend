const toTrimmed = (value) => String(value || "").trim();
const normalizePhone = (value) => String(value || "").replace(/\D/g, "");

const formatCurrency = (value = 0, currency = "INR") => {
  const amount = Number(value);
  const safe = Number.isFinite(amount) ? amount : 0;
  try {
    return new Intl.NumberFormat("en-IN", {
      style: "currency",
      currency,
      maximumFractionDigits: 2,
    }).format(safe);
  } catch (_error) {
    return `${currency} ${safe.toFixed(2)}`;
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

const buildPaymentCallbackUrl = () => {
  const explicit = toTrimmed(process.env.RAZORPAY_CALLBACK_URL);
  if (explicit) return explicit;

  const frontendOrigin =
    toTrimmed(process.env.FRONTEND_ORIGIN) ||
    toTrimmed(process.env.PUBLIC_URL) ||
    toTrimmed(process.env.RENDER_EXTERNAL_URL) ||
    "http://localhost:3000";

  try {
    return new URL("/payment/success", frontendOrigin).toString();
  } catch (_error) {
    return "";
  }
};

const buildPaymentReferenceId = ({ adminId, orderId }) =>
  toTrimmed(`due_${adminId || 0}_${orderId || 0}_${Date.now()}`)
    .replace(/[^a-zA-Z0-9._-]/g, "")
    .slice(0, 40);

const appendPaymentLinkNote = ({
  currentNotes = "",
  linkId = "",
  shortUrl = "",
  amount = 0,
  currency = "INR",
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

  return notes.join("\n");
};

const buildPaymentReminderMessage = ({
  order = {},
  dueAmount = 0,
  currency = "INR",
  paymentLinkUrl = "",
}) => {
  const orderRef = order?.order_number ? `Order ${order.order_number}` : `Order #${order?.id || ""}`;
  const customerName = toTrimmed(order?.customer_name) || "Customer";

  return [
    `Hi ${customerName},`,
    `Please complete the remaining payment of ${formatCurrency(dueAmount, currency)} for ${orderRef}.`,
    `Payment link: ${paymentLinkUrl}`,
    "After payment, you will be redirected to the confirmation page.",
    "If you face any issue, reply with your transaction ID and screenshot.",
  ].join("\n");
};

export const createPaymentLinkTimerService = ({
  logger,
  claimDueOrderPaymentLinkTimers,
  completeOrderPaymentLinkTimer,
  failOrderPaymentLinkTimer,
  getOrderById,
  getUserByPhone,
  updateOrder,
  createRazorpayPaymentLink,
  isRazorpayConfigured,
  normalizeRazorpayCurrency,
  sendAdminMessage,
  enabled,
  pollMs,
  batchSize,
  retryMinutes,
}) => {
  let timerHandle = null;
  let processing = false;

  const processOnePaymentLinkTimer = async (timer) => {
    const orderId = Number(timer?.order_id);
    const adminId = Number(timer?.admin_id);
    if (!Number.isFinite(orderId) || orderId <= 0 || !Number.isFinite(adminId) || adminId <= 0) {
      throw new Error("Invalid timer payload");
    }

    const order = await getOrderById(orderId, adminId);
    if (!order) {
      throw new Error("Order not found for scheduled payment link");
    }

    const dueAmount = getRemainingAmount(order);
    if (!Number.isFinite(dueAmount) || dueAmount <= 0) {
      await completeOrderPaymentLinkTimer(order.id, { paymentLinkId: "" });
      return;
    }

    const customerPhone = normalizePhone(order?.customer_phone);
    if (!customerPhone) {
      throw new Error("Order customer phone is missing");
    }

    let contact = await getUserByPhone(customerPhone, adminId);
    if (!contact?.id) {
      contact = await getUserByPhone(customerPhone);
    }

    const currency = normalizeRazorpayCurrency(
      order?.payment_currency || process.env.RAZORPAY_CURRENCY || "INR"
    );
    const callbackUrl = buildPaymentCallbackUrl();
    const callbackMethod =
      toTrimmed(process.env.RAZORPAY_CALLBACK_METHOD).toLowerCase() === "post" ? "post" : "get";
    const orderRef = order?.order_number ? `Order ${order.order_number}` : `Order #${order.id}`;

    const baseDescription =
      toTrimmed(process.env.RAZORPAY_PAYMENT_DESCRIPTION) || "WhatsApp order payment";
    const description = `${baseDescription} (${orderRef})`.slice(0, 255);

    const paymentLink = await createRazorpayPaymentLink({
      amount: dueAmount,
      currency,
      description,
      callbackUrl,
      callbackMethod,
      referenceId: buildPaymentReferenceId({ adminId, orderId: order.id }),
      customer: {
        name: toTrimmed(order?.customer_name),
        contact: customerPhone,
        email: toTrimmed(order?.customer_email),
      },
      notes: {
        order_id: String(order.id),
        order_number: toTrimmed(order?.order_number || `#${order.id}`),
        admin_id: String(adminId),
        payment_type: "remaining",
        amount_due: String(dueAmount),
        timer: "auto",
      },
    });

    const paymentLinkUrl = toTrimmed(paymentLink?.shortUrl);
    const paymentLinkId = toTrimmed(paymentLink?.id);
    if (!paymentLinkUrl || !paymentLinkId) {
      throw new Error("Razorpay payment link response was invalid");
    }

    const message = buildPaymentReminderMessage({ order, dueAmount, currency, paymentLinkUrl });
    const sendResult = await sendAdminMessage({
      adminId,
      userId: contact?.id ? Number(contact.id) : undefined,
      phone: customerPhone,
      text: message,
    });
    if (sendResult?.error) {
      throw new Error(sendResult.error);
    }

    const paymentNotes = appendPaymentLinkNote({
      currentNotes: order?.payment_notes,
      linkId: paymentLinkId,
      shortUrl: paymentLinkUrl,
      amount: dueAmount,
      currency,
    });

    await updateOrder(
      order.id,
      {
        payment_notes: paymentNotes,
        payment_currency: currency,
      },
      adminId
    );

    await completeOrderPaymentLinkTimer(order.id, { paymentLinkId });
  };

  const processScheduledPaymentLinkTimers = async () => {
    if (!enabled || processing) return;
    if (!isRazorpayConfigured()) return;

    processing = true;
    try {
      const timers = await claimDueOrderPaymentLinkTimers(batchSize);
      if (!timers.length) return;

      for (const timer of timers) {
        try {
          await processOnePaymentLinkTimer(timer);
        } catch (error) {
          logger.warn("Scheduled payment link send failed", {
            orderId: Number(timer?.order_id) || null,
            adminId: Number(timer?.admin_id) || null,
            error: error?.message || String(error),
          });

          await failOrderPaymentLinkTimer(
            timer?.order_id,
            error?.message || "Timer processing failed",
            {
              retryDelayMinutes: retryMinutes,
            }
          );
        }
      }
    } finally {
      processing = false;
    }
  };

  const start = () => {
    if (!enabled || timerHandle) return;

    timerHandle = setInterval(() => {
      processScheduledPaymentLinkTimers().catch((error) => {
        logger.error("Scheduled payment link timer crashed", {
          error: error?.message || String(error),
        });
      });
    }, pollMs);

    processScheduledPaymentLinkTimers().catch((error) => {
      logger.error("Initial scheduled payment link run failed", {
        error: error?.message || String(error),
      });
    });

    logger.info("Scheduled payment link timer enabled", {
      pollMs,
      batchSize,
      retryMinutes,
    });
  };

  const stop = () => {
    if (!timerHandle) return;
    clearInterval(timerHandle);
    timerHandle = null;
  };

  return {
    start,
    stop,
    processScheduledPaymentLinkTimers,
  };
};

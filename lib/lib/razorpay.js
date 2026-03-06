const DEFAULT_CURRENCY = "INR";
const RAZORPAY_API_BASE = "https://api.razorpay.com/v1";
const RAZORPAY_PAYMENT_LINK_ENDPOINT = `${RAZORPAY_API_BASE}/payment_links`;

const toTrimmedString = (value) => String(value || "").trim();

const toPaise = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return Math.round(amount * 100);
};

const toAmountFromPaise = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return 0;
  return Number((amount / 100).toFixed(2));
};

const sanitizeReferenceId = (value) =>
  toTrimmedString(value)
    .replace(/[^a-zA-Z0-9._-]/g, "")
    .slice(0, 40);

const sanitizeNotes = (notes) => {
  if (!notes || typeof notes !== "object") return undefined;
  const entries = Object.entries(notes)
    .filter(([key, val]) => toTrimmedString(key) && val != null && toTrimmedString(val))
    .slice(0, 12)
    .map(([key, val]) => [toTrimmedString(key).slice(0, 40), toTrimmedString(val).slice(0, 256)]);
  if (entries.length === 0) return undefined;
  return Object.fromEntries(entries);
};

const getRazorpayCredentials = () => ({
  keyId: toTrimmedString(process.env.RAZORPAY_KEY_ID),
  keySecret: toTrimmedString(process.env.RAZORPAY_KEY_SECRET),
});

const assertRazorpayCredentials = () => {
  const { keyId, keySecret } = getRazorpayCredentials();
  if (!keyId || !keySecret) {
    throw new Error("Razorpay credentials are not configured.");
  }
  return { keyId, keySecret };
};

const getRazorpayAuthHeader = () => {
  const { keyId, keySecret } = assertRazorpayCredentials();
  return `Basic ${Buffer.from(`${keyId}:${keySecret}`).toString("base64")}`;
};

const parseRazorpayResponse = async (response) => {
  const raw = await response.text();
  let parsed = null;
  try {
    parsed = raw ? JSON.parse(raw) : null;
  } catch (_error) {
    parsed = null;
  }
  return { raw, parsed };
};

const buildRazorpayApiError = (response, parsed, raw, action) => {
  const apiError = parsed?.error?.description || parsed?.error?.reason || raw || response.statusText;
  const error = new Error(
    `Razorpay ${action} failed (${response.status}): ${toTrimmedString(apiError) || "Unknown error"}`
  );
  error.status = response.status;
  error.payload = parsed || raw;
  return error;
};

const getPaymentLinkByIdEndpoint = (paymentLinkId) =>
  `${RAZORPAY_PAYMENT_LINK_ENDPOINT}/${encodeURIComponent(toTrimmedString(paymentLinkId))}`;

const getPaymentLinkPaymentsEndpoint = (paymentLinkId, { count = 20, skip = 0 } = {}) => {
  const safeCount = Math.min(Math.max(Number(count) || 20, 1), 100);
  const safeSkip = Math.max(Number(skip) || 0, 0);
  const query = new URLSearchParams({
    count: String(safeCount),
    skip: String(safeSkip),
  });
  return `${getPaymentLinkByIdEndpoint(paymentLinkId)}/payments?${query.toString()}`;
};

const normalizeComparable = (value) => toTrimmedString(value).toLowerCase();

const extractPaymentTransactionId = (payment) =>
  toTrimmedString(
    payment?.acquirer_data?.upi_transaction_id ||
      payment?.acquirer_data?.utr ||
      payment?.acquirer_data?.rrn ||
      payment?.reference_id ||
      payment?.id
  );

const doesPaymentMatchProofId = (payment, proofId) => {
  const target = normalizeComparable(proofId);
  if (!target) return false;
  const candidates = [
    payment?.id,
    payment?.reference_id,
    payment?.acquirer_data?.upi_transaction_id,
    payment?.acquirer_data?.utr,
    payment?.acquirer_data?.rrn,
    payment?.notes?.transaction_id,
    payment?.notes?.utr,
  ]
    .map((value) => normalizeComparable(value))
    .filter(Boolean);
  return candidates.includes(target);
};

const normalizePaymentTimestamp = (value) => {
  const epochSeconds = Number(value);
  if (!Number.isFinite(epochSeconds) || epochSeconds <= 0) return null;
  return new Date(epochSeconds * 1000).toISOString();
};

const normalizePaymentMode = ({ paidAmount, expectedAmount }) => {
  if (!Number.isFinite(paidAmount) || paidAmount <= 0) return "none";
  if (!Number.isFinite(expectedAmount) || expectedAmount <= 0) return "paid";
  if (paidAmount + 0.01 < expectedAmount) return "partial";
  return "paid";
};

export const normalizeRazorpayCurrency = (value) => {
  const normalized = toTrimmedString(value).toUpperCase();
  return normalized || DEFAULT_CURRENCY;
};

export const normalizeRazorpayAmount = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return Number(amount.toFixed(2));
};

export const isRazorpayConfigured = () => {
  const { keyId, keySecret } = getRazorpayCredentials();
  return Boolean(keyId && keySecret);
};

export const createRazorpayPaymentLink = async ({
  amount,
  currency = DEFAULT_CURRENCY,
  description = "WhatsApp order payment",
  customer = null,
  notes = null,
  callbackUrl = "",
  callbackMethod = "get",
  referenceId = "",
}) => {
  const normalizedAmount = normalizeRazorpayAmount(amount);
  const amountPaise = toPaise(normalizedAmount);
  if (!amountPaise) {
    throw new Error("Invalid amount for Razorpay payment link.");
  }

  const payload = {
    amount: amountPaise,
    currency: normalizeRazorpayCurrency(currency),
    description: toTrimmedString(description).slice(0, 255) || "WhatsApp order payment",
  };

  const sanitizedRef = sanitizeReferenceId(referenceId);
  if (sanitizedRef) {
    payload.reference_id = sanitizedRef;
  }

  const customerName = toTrimmedString(customer?.name);
  const customerContact = String(customer?.contact || "").replace(/\D/g, "");
  const customerEmail = toTrimmedString(customer?.email);
  if (customerName || customerContact || customerEmail) {
    payload.customer = {};
    if (customerName) payload.customer.name = customerName.slice(0, 80);
    if (customerContact) payload.customer.contact = customerContact.slice(0, 15);
    if (customerEmail) payload.customer.email = customerEmail.slice(0, 120);
  }

  const normalizedNotes = sanitizeNotes(notes);
  if (normalizedNotes) {
    payload.notes = normalizedNotes;
  }

  const normalizedCallbackUrl = toTrimmedString(callbackUrl);
  if (normalizedCallbackUrl) {
    payload.callback_url = normalizedCallbackUrl;
    payload.callback_method = toTrimmedString(callbackMethod).toLowerCase() === "post" ? "post" : "get";
  }

  const response = await fetch(RAZORPAY_PAYMENT_LINK_ENDPOINT, {
    method: "POST",
    headers: {
      Authorization: getRazorpayAuthHeader(),
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const { raw, parsed } = await parseRazorpayResponse(response);
  if (!response.ok) {
    throw buildRazorpayApiError(response, parsed, raw, "payment link creation");
  }

  return {
    id: toTrimmedString(parsed?.id),
    shortUrl: toTrimmedString(parsed?.short_url),
    amount: normalizedAmount,
    currency: normalizeRazorpayCurrency(parsed?.currency || currency),
    raw: parsed,
  };
};

export const fetchRazorpayPaymentLink = async (paymentLinkId) => {
  const id = toTrimmedString(paymentLinkId);
  if (!id) {
    throw new Error("paymentLinkId is required.");
  }
  const response = await fetch(getPaymentLinkByIdEndpoint(id), {
    method: "GET",
    headers: {
      Authorization: getRazorpayAuthHeader(),
      "Content-Type": "application/json",
    },
  });
  const { raw, parsed } = await parseRazorpayResponse(response);
  if (!response.ok) {
    throw buildRazorpayApiError(response, parsed, raw, "payment link lookup");
  }
  return parsed || {};
};

export const fetchRazorpayPaymentLinkPayments = async (paymentLinkId, options = {}) => {
  const id = toTrimmedString(paymentLinkId);
  if (!id) {
    throw new Error("paymentLinkId is required.");
  }
  const response = await fetch(getPaymentLinkPaymentsEndpoint(id, options), {
    method: "GET",
    headers: {
      Authorization: getRazorpayAuthHeader(),
      "Content-Type": "application/json",
    },
  });
  const { raw, parsed } = await parseRazorpayResponse(response);
  if (!response.ok) {
    throw buildRazorpayApiError(response, parsed, raw, "payment link payments lookup");
  }
  return parsed || { items: [] };
};

export const verifyRazorpayPaymentLink = async ({
  paymentLinkId,
  expectedAmount = null,
  proofId = "",
} = {}) => {
  const normalizedLinkId = toTrimmedString(paymentLinkId);
  if (!normalizedLinkId) {
    return {
      verified: false,
      reason: "payment_link_missing",
      mode: "none",
      paidAmount: 0,
      expectedAmount: normalizeRazorpayAmount(expectedAmount) || null,
      paymentId: "",
      transactionId: "",
      linkStatus: "unknown",
      proofMatched: null,
      paidAt: null,
      currency: DEFAULT_CURRENCY,
      linkId: "",
      raw: null,
    };
  }

  const normalizedExpectedAmount = normalizeRazorpayAmount(expectedAmount);
  const normalizedProofId = toTrimmedString(proofId);
  const link = await fetchRazorpayPaymentLink(normalizedLinkId);

  let payments = [];
  let paymentsLookupError = null;
  try {
    const paymentList = await fetchRazorpayPaymentLinkPayments(normalizedLinkId, { count: 20, skip: 0 });
    payments = Array.isArray(paymentList?.items) ? paymentList.items : [];
  } catch (error) {
    paymentsLookupError = error;
  }

  const capturedPayments = payments
    .filter((payment) => normalizeComparable(payment?.status) === "captured")
    .sort((a, b) => Number(b?.created_at || 0) - Number(a?.created_at || 0));
  const latestCaptured = capturedPayments[0] || null;
  const proofMatchPayment = normalizedProofId
    ? capturedPayments.find((payment) => doesPaymentMatchProofId(payment, normalizedProofId)) || null
    : null;
  const selectedPayment = proofMatchPayment || latestCaptured;

  const paidAmountFromPayments = capturedPayments.reduce(
    (sum, payment) => sum + toAmountFromPaise(payment?.amount),
    0
  );
  const paidAmountFromLink = toAmountFromPaise(link?.amount_paid);
  const paidAmount = Number(Math.max(paidAmountFromPayments, paidAmountFromLink).toFixed(2));
  const linkAmount = toAmountFromPaise(link?.amount);
  const expected = normalizedExpectedAmount ?? (linkAmount > 0 ? linkAmount : null);
  const linkStatus = normalizeComparable(link?.status) || "unknown";

  let verified = paidAmount > 0 && !["cancelled", "expired"].includes(linkStatus);
  let reason = verified ? "verified" : "not_paid";
  if (normalizedProofId && !proofMatchPayment) {
    verified = false;
    reason = "proof_id_not_found";
  } else if (["cancelled", "expired"].includes(linkStatus)) {
    verified = false;
    reason = `link_${linkStatus}`;
  } else if (!verified && paymentsLookupError) {
    reason = "payments_lookup_failed";
  }

  return {
    verified,
    reason,
    mode: normalizePaymentMode({ paidAmount, expectedAmount: expected }),
    paidAmount,
    expectedAmount: expected,
    paymentId: toTrimmedString(selectedPayment?.id),
    transactionId: extractPaymentTransactionId(selectedPayment),
    linkStatus,
    proofMatched: normalizedProofId ? Boolean(proofMatchPayment) : null,
    paidAt: normalizePaymentTimestamp(selectedPayment?.created_at),
    currency: normalizeRazorpayCurrency(link?.currency || DEFAULT_CURRENCY),
    linkId: toTrimmedString(link?.id || normalizedLinkId),
    raw: {
      link,
      payment: selectedPayment || null,
    },
  };
};

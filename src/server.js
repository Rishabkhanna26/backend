import "dotenv/config";
import express from "express";
import http from "node:http";
import fs from "node:fs/promises";
import compression from "compression";
import { Server } from "socket.io";
import { verifyAuthToken } from "../lib/auth.js";
import {
  claimDueOrderPaymentLinkTimers,
  completeOrderPaymentLinkTimer,
  failOrderPaymentLinkTimer,
  getEffectiveRazorpayCredentials,
  getOrderById,
  getUserByPhone,
  initializeDbHelpers,
  refreshAllAdminTokenBalances,
  updateOrder,
} from "../lib/db-helpers.js";
import {
  createRazorpayPaymentLink,
  isRazorpayConfigured,
  normalizeRazorpayCurrency,
} from "../lib/razorpay.js";
import {
  startWhatsApp,
  stopWhatsApp,
  getWhatsAppState,
  whatsappEvents,
  sendAdminMessage,
} from "./whatsapp.js";
import { initSentry, sentryErrorHandler } from "../config/sentry.js";
import logger from "../config/logger.js";
import {
  apiRateLimiter,
  authRateLimiter,
  whatsappRateLimiter,
  securityHeaders,
  requestLogger,
} from "../middleware/security.js";
// Import graceful shutdown handler (Task 10.4)
import "./shutdown.js";

const app = express();

// Initialize Sentry (must be first)
initSentry(app);

// Security headers
app.use(securityHeaders);

// Compression
app.use(compression());

// Body parsing with size limits
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Request logging
app.use(requestLogger);

const DEFAULT_PORT = 3001;
const BASE_PORT = Number(process.env.PORT) || DEFAULT_PORT;
const FRONTEND_ORIGIN =
  process.env.FRONTEND_ORIGIN || "http://localhost:3000";
const FRONTEND_ORIGINS = new Set(
  (process.env.FRONTEND_ORIGINS || `${FRONTEND_ORIGIN},http://localhost:3001`)
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean)
);
const BACKEND_SCOPE = "backend";
const PAYMENT_LINK_TIMER_ENABLED = String(
  process.env.PAYMENT_LINK_TIMER_ENABLED || "true"
).toLowerCase() !== "false";
const PAYMENT_LINK_TIMER_POLL_MS = Math.min(
  Math.max(Number(process.env.PAYMENT_LINK_TIMER_POLL_SECONDS || 30) * 1000, 5000),
  5 * 60 * 1000
);
const PAYMENT_LINK_TIMER_BATCH_SIZE = Math.min(
  Math.max(Number(process.env.PAYMENT_LINK_TIMER_BATCH_SIZE || 10), 1),
  100
);
const FREE_TOKEN_REFRESH_ENABLED = String(
  process.env.FREE_TOKEN_REFRESH_ENABLED || "true"
).toLowerCase() !== "false";
const FREE_TOKEN_REFRESH_INTERVAL_MS = Math.min(
  Math.max(Number(process.env.FREE_TOKEN_REFRESH_INTERVAL_HOURS || 6) * 60 * 60 * 1000, 60 * 60 * 1000),
  24 * 60 * 60 * 1000
);
const PAYMENT_LINK_TIMER_RETRY_MINUTES = Math.min(
  Math.max(Number(process.env.PAYMENT_LINK_TIMER_RETRY_MINUTES || 10), 1),
  24 * 60
);
let paymentLinkTimerHandle = null;
let paymentLinkTimerRunning = false;

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

const buildPaymentReminderMessage = ({ order = {}, dueAmount = 0, currency = "INR", paymentLinkUrl = "" }) => {
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

const parseAdminId = (value) => {
  const normalized = String(value ?? "").trim();
  if (!normalized) return null;
  const adminId = Number(normalized);
  return Number.isInteger(adminId) && adminId > 0 ? adminId : null;
};

const extractBearerToken = (value) => {
  const header = String(value || "");
  if (!header.toLowerCase().startsWith("bearer ")) return "";
  return header.slice(7).trim();
};

const getScopedAdminIdFromRequest = (req) => {
  const queryAdminId = parseAdminId(req.query?.adminId);
  if (queryAdminId != null) return queryAdminId;
  const bodyAdminId = parseAdminId(req.body?.adminId);
  if (bodyAdminId != null) return bodyAdminId;
  const authAdminId = parseAdminId(req.backendAuth?.id);
  return authAdminId != null ? authAdminId : undefined;
};

const verifyBackendAuthPayload = (payload) => {
  const adminId = parseAdminId(payload?.id);
  if (!payload || payload?.scope !== BACKEND_SCOPE || adminId == null) {
    return null;
  }
  return payload;
};

const requireBackendAuth = (req, res, next) => {
  const token = extractBearerToken(req.headers.authorization);
  const payload = verifyBackendAuthPayload(verifyAuthToken(token));
  if (!payload) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  const requestedAdminId = parseAdminId(req.query?.adminId ?? req.body?.adminId);
  if (
    requestedAdminId != null &&
    payload.admin_tier !== "super_admin" &&
    requestedAdminId !== parseAdminId(payload.id)
  ) {
    res.status(403).json({ error: "Forbidden" });
    return;
  }

  req.backendAuth = payload;
  next();
};

const isLocalhostOrigin = (origin) =>
  /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin);

const resolveOrigin = (origin) => {
  if (!origin) return FRONTEND_ORIGIN;
  if (FRONTEND_ORIGINS.has(origin) || isLocalhostOrigin(origin)) return origin;
  return FRONTEND_ORIGIN;
};

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

  const credentials = (await getEffectiveRazorpayCredentials(adminId)) || {};
  if (!isRazorpayConfigured(credentials)) {
    throw new Error("Razorpay credentials are missing for this admin");
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

  const currency = normalizeRazorpayCurrency(order?.payment_currency || process.env.RAZORPAY_CURRENCY || "INR");
  const callbackUrl = buildPaymentCallbackUrl();
  const callbackMethod = toTrimmed(process.env.RAZORPAY_CALLBACK_METHOD).toLowerCase() === "post" ? "post" : "get";
  const orderRef = order?.order_number ? `Order ${order.order_number}` : `Order #${order.id}`;
  const baseDescription = toTrimmed(process.env.RAZORPAY_PAYMENT_DESCRIPTION) || "WhatsApp order payment";
  const description = `${baseDescription} (${orderRef})`.slice(0, 255);

  const paymentLink = await createRazorpayPaymentLink({
    amount: dueAmount,
    currency,
    description,
    callbackUrl,
    callbackMethod,
    referenceId: buildPaymentReferenceId({ adminId, orderId: order.id }),
    credentials,
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
  if (!PAYMENT_LINK_TIMER_ENABLED || paymentLinkTimerRunning) return;
  paymentLinkTimerRunning = true;
  try {
    const timers = await claimDueOrderPaymentLinkTimers(PAYMENT_LINK_TIMER_BATCH_SIZE);
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
        await failOrderPaymentLinkTimer(timer?.order_id, error?.message || "Timer processing failed", {
          retryDelayMinutes: PAYMENT_LINK_TIMER_RETRY_MINUTES,
        });
      }
    }
  } finally {
    paymentLinkTimerRunning = false;
  }
};

app.use((req, res, next) => {
  const origin = resolveOrigin(req.headers.origin);
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  if (req.method === "OPTIONS") {
    res.status(204).end();
    return;
  }
  next();
});

app.get("/", (req, res) => {
  res.json({ 
    status: 'ok',
    message: 'Backend running',
    version: '1.0.0',
    timestamp: new Date().toISOString()
  });
});

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: 'healthy',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    memory: process.memoryUsage(),
  });
});

app.get("/health/storage", async (req, res) => {
  if (process.env.DEBUG_STORAGE_CHECK !== "true") {
    res.status(404).json({ error: "Not found" });
    return;
  }
  const authPath = process.env.WHATSAPP_AUTH_PATH || ".wwebjs_auth";
  try {
    const stats = await fs.stat(authPath);
    const entries = await fs.readdir(authPath).catch(() => []);
    res.json({
      ok: true,
      authPath,
      exists: true,
      isDirectory: stats.isDirectory(),
      entryCount: entries.length,
      sampleEntries: entries.slice(0, 10),
    });
  } catch (err) {
    res.json({
      ok: false,
      authPath,
      exists: false,
      error: err?.message || "Unknown error",
    });
  }
});

app.use("/whatsapp", requireBackendAuth, whatsappRateLimiter);

app.get("/whatsapp/status", async (req, res) => {
  try {
    const state = await getWhatsAppState(getScopedAdminIdFromRequest(req));
    res.json(state);
  } catch (err) {
    logger.error("Failed to get WhatsApp status", { error: err.message });
    res.status(500).json({ error: "Failed to get status" });
  }
});

app.post("/whatsapp/start", async (req, res) => {
  try {
    const adminId = getScopedAdminIdFromRequest(req);
    const result = await startWhatsApp(adminId, {
      authMethod: req.body?.authMethod ?? req.body?.authMode,
      phoneNumber: req.body?.phoneNumber ?? req.body?.pairingPhoneNumber,
    });
    if (result?.error) {
      logger.warn("WhatsApp start failed", { error: result.error, adminId });
      res.status(400).json(result);
      return;
    }
    logger.info("WhatsApp started successfully", { adminId });
    res.json(result);
  } catch (err) {
    const message = String(err?.message || "").trim() || "Failed to start WhatsApp";
    logger.error("Failed to start WhatsApp", { error: message, stack: err?.stack, adminId: getScopedAdminIdFromRequest(req) });
    // This route is backend-auth protected; returning a reason here is acceptable and helps debug deployments.
    res.status(500).json({ error: message });
  }
});

app.post("/whatsapp/disconnect", async (req, res) => {
  try {
    const result = await stopWhatsApp(getScopedAdminIdFromRequest(req));
    if (result?.error) {
      logger.warn("WhatsApp disconnect failed", { error: result.error, adminId: getScopedAdminIdFromRequest(req) });
      res.status(400).json(result);
      return;
    }
    logger.info("WhatsApp disconnected successfully", { adminId: getScopedAdminIdFromRequest(req) });
    res.json(result);
  } catch (err) {
    const message = String(err?.message || "").trim() || "Failed to disconnect WhatsApp";
    logger.error("Failed to disconnect WhatsApp", { error: message, stack: err?.stack, adminId: getScopedAdminIdFromRequest(req) });
    res.status(500).json({ error: message });
  }
});

app.post("/whatsapp/send", async (req, res) => {
  try {
    const userId = Number(req.body?.userId);
    const phone = String(req.body?.phone || "").trim();
    const message = String(req.body?.message || "").trim();
    const result = await sendAdminMessage({
      adminId: getScopedAdminIdFromRequest(req),
      userId: Number.isFinite(userId) ? userId : undefined,
      phone,
      text: message,
    });
    if (result?.error) {
      res.status(result.status || 400).json({ success: false, error: result.error, code: result.code });
      return;
    }
    res.json({ success: true, data: result?.data || null });
  } catch (err) {
    console.error("❌ Failed to send WhatsApp message:", err);
    res.status(500).json({ success: false, error: "Failed to send WhatsApp message" });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: (origin, callback) => {
      if (!origin || FRONTEND_ORIGINS.has(origin) || isLocalhostOrigin(origin)) {
        callback(null, true);
        return;
      }
      callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST"],
  },
});

io.use((socket, next) => {
  const authHeaderToken = extractBearerToken(socket.handshake.headers?.authorization);
  const handshakeToken = String(socket.handshake.auth?.token || "").trim();
  const token = handshakeToken || authHeaderToken;
  const payload = verifyBackendAuthPayload(verifyAuthToken(token));
  if (!payload) {
    next(new Error("Unauthorized"));
    return;
  }

  const requestedAdminId = parseAdminId(socket.handshake.query?.adminId);
  const tokenAdminId = parseAdminId(payload.id);
  if (tokenAdminId == null) {
    next(new Error("Unauthorized"));
    return;
  }

  if (
    requestedAdminId != null &&
    payload.admin_tier !== "super_admin" &&
    requestedAdminId !== tokenAdminId
  ) {
    next(new Error("Forbidden"));
    return;
  }

  socket.data.backendAuth = payload;
  socket.data.adminId = requestedAdminId != null ? requestedAdminId : tokenAdminId;
  next();
});

const enableRedisAdapter = async (ioServer) => {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) return;
  try {
    const { createClient } = await import("redis");
    const { createAdapter } = await import("@socket.io/redis-adapter");
    const pubClient = createClient({ url: redisUrl });
    const subClient = pubClient.duplicate();
    await Promise.all([pubClient.connect(), subClient.connect()]);
    ioServer.adapter(createAdapter(pubClient, subClient));
    console.log("✅ Socket.IO Redis adapter enabled");
  } catch (err) {
    console.warn("⚠️ Redis adapter not enabled:", err?.message || err);
  }
};

try {
  await initializeDbHelpers();
  logger.info("Database helpers initialized");
} catch (err) {
  logger.warn("Database helpers initialization skipped at startup (will retry later)", {
    error: err?.message || err
  });
}

await enableRedisAdapter(io);

if (PAYMENT_LINK_TIMER_ENABLED) {
  paymentLinkTimerHandle = setInterval(() => {
    processScheduledPaymentLinkTimers().catch((error) => {
      logger.error("Scheduled payment link timer crashed", {
        error: error?.message || String(error),
      });
    });
  }, PAYMENT_LINK_TIMER_POLL_MS);
  processScheduledPaymentLinkTimers().catch((error) => {
    logger.error("Initial scheduled payment link run failed", {
      error: error?.message || String(error),
    });
  });
  logger.info("Scheduled payment link timer enabled", {
    pollMs: PAYMENT_LINK_TIMER_POLL_MS,
    batchSize: PAYMENT_LINK_TIMER_BATCH_SIZE,
    retryMinutes: PAYMENT_LINK_TIMER_RETRY_MINUTES,
  });
}

if (FREE_TOKEN_REFRESH_ENABLED) {
  const runTokenRefresh = () => {
    refreshAllAdminTokenBalances().catch((error) => {
      logger.error("Free token refresh job failed", {
        error: error?.message || String(error),
      });
    });
  };
  runTokenRefresh();
  setInterval(runTokenRefresh, FREE_TOKEN_REFRESH_INTERVAL_MS);
  logger.info("Free token refresh job enabled", {
    intervalMs: FREE_TOKEN_REFRESH_INTERVAL_MS,
  });
}

// Error handling middleware (must be after all routes)
app.use(sentryErrorHandler());

// 404 handler
app.use((req, res) => {
  logger.warn("Route not found", { path: req.path, method: req.method });
  res.status(404).json({
    error: "Not Found",
    message: "The requested resource was not found",
    path: req.path,
  });
});

// Global error handler
app.use((err, req, res, next) => {
  logger.error("Unhandled error", {
    error: err.message,
    stack: err.stack,
    path: req.path,
    method: req.method,
  });
  
  res.status(err.status || 500).json({
    error: "Internal Server Error",
    message: process.env.NODE_ENV === 'development' ? err.message : "An error occurred",
  });
});

io.on("connection", async (socket) => {
  const adminId = parseAdminId(socket.data?.adminId);
  if (adminId != null) {
    const room = `admin:${adminId}`;
    socket.join(room);
    const state = await getWhatsAppState(adminId);
    socket.emit("whatsapp:status", state);
    if (state.qrImage) {
      socket.emit("whatsapp:qr", { qrImage: state.qrImage });
    }
  } else {
    socket.emit("whatsapp:status", await getWhatsAppState());
  }
});

whatsappEvents.on("status", (payload) => {
  const adminId = Number(payload?.adminId);
  if (Number.isFinite(adminId)) {
    io.to(`admin:${adminId}`).emit("whatsapp:status", payload);
    return;
  }
  io.emit("whatsapp:status", payload);
});

whatsappEvents.on("qr", (payload) => {
  const adminId = Number(payload?.adminId);
  const qrPayload =
    payload && typeof payload === "object"
      ? { qr: payload.qr, qrImage: payload.qrImage }
      : payload;
  if (Number.isFinite(adminId)) {
    io.to(`admin:${adminId}`).emit("whatsapp:qr", qrPayload);
    return;
  }
  io.emit("whatsapp:qr", qrPayload);
});

let currentPort = BASE_PORT;

const startServer = (port) => {
  server.listen(port, () => {
    const logUrl = `http://localhost:${port}`;
    logger.info(`Backend running on ${logUrl}`, {
      port,
      environment: process.env.NODE_ENV || 'development',
    });
  });
};

server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    const nextPort = currentPort + 1;
    logger.warn(`Port ${currentPort} in use, trying ${nextPort}...`);
    currentPort = nextPort;
    setTimeout(() => startServer(currentPort), 200);
    return;
  }
  logger.error("Server error", { error: err.message, code: err.code });
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully...');
  if (paymentLinkTimerHandle) {
    clearInterval(paymentLinkTimerHandle);
    paymentLinkTimerHandle = null;
  }
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully...');
  if (paymentLinkTimerHandle) {
    clearInterval(paymentLinkTimerHandle);
    paymentLinkTimerHandle = null;
  }
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

startServer(currentPort);

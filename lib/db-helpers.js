import pg from "pg";
import {
  DEFAULT_INPUT_USD_PER_1M,
  DEFAULT_OUTPUT_USD_PER_1M,
  DEFAULT_USD_TO_INR_RATE,
  FREE_INPUT_TOKEN_CAP,
  FREE_INPUT_TOKENS_PER_MONTH,
  FREE_OUTPUT_TOKEN_CAP,
  FREE_OUTPUT_TOKENS_PER_MONTH,
  computeUsageCosts,
  estimateTokens,
} from "./billing.js";
import { sanitizeEmail, sanitizeNameUpper, sanitizePhone, sanitizeText } from "./sanitize.js";

const { Pool } = pg;

let pool;
let adminBusinessColumnsReadyPromise = null;
let adminBusinessColumnsInitStarted = false;

const ALLOWED_BUSINESS_TYPES = new Set(['product', 'service', 'both']);
const ALLOWED_APPOINTMENT_KINDS = new Set(['service', 'booking']);
const ALLOWED_CATALOG_SECTIONS = new Set(['catalog', 'booking', 'all']);
const ALLOWED_FREE_DELIVERY_SCOPES = new Set(['combined', 'eligible_only']);
const APPOINTMENT_SETTING_DEFAULTS = Object.freeze({
  startHour: 9,
  endHour: 20,
  slotMinutes: 60,
  windowMonths: 3,
});

const toFiniteNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const normalizeInteger = (value) => {
  const num = toFiniteNumber(value);
  if (num === null) return null;
  return Math.trunc(num);
};

const normalizeAppointmentStartHour = (value) => {
  const num = normalizeInteger(value);
  if (num === null || num < 0 || num > 23) return null;
  return num;
};

const normalizeAppointmentEndHour = (value) => {
  const num = normalizeInteger(value);
  if (num === null || num < 1 || num > 24) return null;
  return num;
};

const normalizeAppointmentSlotMinutes = (value) => {
  const num = normalizeInteger(value);
  if (num === null || num < 15 || num > 240) return null;
  return num;
};

const normalizeAppointmentWindowMonths = (value) => {
  const num = normalizeInteger(value);
  if (num === null || num < 1 || num > 24) return null;
  return num;
};

const normalizeBusinessUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (!['http:', 'https:'].includes(parsed.protocol)) return null;
    return parsed.toString();
  } catch {
    return null;
  }
};

const normalizeFreeDeliveryScope = (value, fallback = 'combined') => {
  const normalized = String(value || '').trim().toLowerCase();
  if (ALLOWED_FREE_DELIVERY_SCOPES.has(normalized)) return normalized;
  return fallback;
};

const normalizeFreeDeliveryProductRules = (value) => {
  const rawRules = Array.isArray(value)
    ? value
    : typeof value === 'string'
    ? (() => {
        try {
          const parsed = JSON.parse(value);
          return Array.isArray(parsed) ? parsed : [];
        } catch {
          return [];
        }
      })()
    : [];

  const seenIds = new Set();
  const normalized = [];
  rawRules.forEach((entry) => {
    const productIdRaw =
      entry?.catalog_item_id ?? entry?.catalogItemId ?? entry?.product_id ?? entry?.productId;
    const minAmountRaw = entry?.min_amount ?? entry?.minAmount;
    const productNameRaw =
      entry?.product_name ?? entry?.productName ?? entry?.name ?? '';

    const productId = Math.trunc(Number(productIdRaw));
    const minAmount = normalizeCurrencyAmount(minAmountRaw);
    if (!Number.isFinite(productId) || productId <= 0) return;
    if (!(Number.isFinite(minAmount) && minAmount > 0)) return;
    if (seenIds.has(productId)) return;
    seenIds.add(productId);
    normalized.push({
      catalog_item_id: productId,
      min_amount: minAmount,
      product_name: sanitizeText(productNameRaw, 160).trim() || null,
    });
  });

  return normalized.slice(0, 100);
};

const normalizeCurrencyAmount = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return Number(num.toFixed(2));
};

const normalizeWhatsappLimit = (value, fallback = 3) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  const normalized = Math.trunc(num);
  if (normalized < 0) return 0;
  if (normalized > 25) return 25;
  return normalized;
};

const isMissingColumnError = (error) =>
  Boolean(error) &&
  (error.code === '42703' || String(error.message || '').toLowerCase().includes('column'));

const DASHBOARD_TREND_DAYS = 14;
const MONTH_SHORT_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const toUtcDateKey = (date) => {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const toDashboardTrendLabel = (date) =>
  `${String(date.getUTCDate()).padStart(2, '0')} ${MONTH_SHORT_NAMES[date.getUTCMonth()]}`;

const buildDashboardGrowthTrend = async (
  connection,
  adminId = null,
  days = DASHBOARD_TREND_DAYS
) => {
  const safeDays = Number.isFinite(Number(days)) ? Math.max(2, Math.min(90, Number(days))) : DASHBOARD_TREND_DAYS;
  const startUtc = new Date();
  startUtc.setUTCHours(0, 0, 0, 0);
  startUtc.setUTCDate(startUtc.getUTCDate() - (safeDays - 1));

  const params = [startUtc.toISOString()];
  let query = `
    SELECT TO_CHAR(DATE(created_at), 'YYYY-MM-DD') AS day_key, COUNT(*)::int AS total
    FROM messages
    WHERE message_type = 'incoming'
      AND created_at >= ?
  `;
  const scopedAdminId = toScopedAdminId(adminId);
  if (scopedAdminId) {
    query += ' AND admin_id = ?';
    params.push(scopedAdminId);
  }
  query += `
    GROUP BY DATE(created_at)
    ORDER BY DATE(created_at) ASC
  `;

  const [rows] = await connection.query(query, params);
  const totalsByDay = new Map(
    (rows || []).map((row) => [String(row.day_key || ''), Number(row.total) || 0])
  );

  const trend = [];
  for (let i = 0; i < safeDays; i += 1) {
    const current = new Date(startUtc);
    current.setUTCDate(startUtc.getUTCDate() + i);
    const key = toUtcDateKey(current);
    trend.push({
      date: key,
      label: toDashboardTrendLabel(current),
      value: totalsByDay.get(key) || 0,
    });
  }
  return trend;
};

const toAmountValue = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Number(num.toFixed(2));
};

const toScopedAdminId = (adminId) => {
  const numeric = Number(adminId);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  return numeric;
};

const buildWhatsAppChannelPredicate = (columnRef) =>
  `LOWER(COALESCE(NULLIF(BTRIM(${columnRef}), ''), 'whatsapp')) LIKE '%whatsapp%'`;

const buildDashboardRevenueTrend = async (
  connection,
  adminId = null,
  days = DASHBOARD_TREND_DAYS
) => {
  const safeDays = Number.isFinite(Number(days)) ? Math.max(2, Math.min(90, Number(days))) : DASHBOARD_TREND_DAYS;
  const startUtc = new Date();
  startUtc.setUTCHours(0, 0, 0, 0);
  startUtc.setUTCDate(startUtc.getUTCDate() - (safeDays - 1));

  const params = [startUtc.toISOString()];
  const whatsappPredicate = buildWhatsAppChannelPredicate('channel');
  let query = `
    SELECT
      TO_CHAR(revenue_date, 'YYYY-MM-DD') AS day_key,
      SUM(
        CASE
          WHEN payment_status = 'refunded' THEN 0
          ELSE GREATEST(COALESCE(collected_amount, 0), 0)
        END
      )::numeric(12,2) AS earned_total,
      SUM(GREATEST(COALESCE(booked_amount, 0), 0))::numeric(12,2) AS booked_total
    FROM order_revenue
    WHERE revenue_date >= DATE(?)
      AND ${whatsappPredicate}
  `;
  const scopedAdminId = toScopedAdminId(adminId);
  if (scopedAdminId) {
    query += ' AND admin_id = ?';
    params.push(scopedAdminId);
  }
  query += `
    GROUP BY revenue_date
    ORDER BY revenue_date ASC
  `;

  const [rows] = await connection.query(query, params);
  const totalsByDay = new Map(
    (rows || []).map((row) => [
      String(row.day_key || ''),
      {
        earned: toAmountValue(row.earned_total),
        booked: toAmountValue(row.booked_total),
      },
    ])
  );

  const trend = [];
  for (let i = 0; i < safeDays; i += 1) {
    const current = new Date(startUtc);
    current.setUTCDate(startUtc.getUTCDate() + i);
    const key = toUtcDateKey(current);
    const dayTotals = totalsByDay.get(key) || { earned: 0, booked: 0 };
    trend.push({
      date: key,
      label: toDashboardTrendLabel(current),
      earned: toAmountValue(dayTotals.earned),
      booked: toAmountValue(dayTotals.booked),
    });
  }
  return trend;
};

const getDashboardSubscriptionRevenueSnapshot = async (connection, adminId = null) => {
  const params = [];
  let query = `
    SELECT
      COALESCE(
        SUM(
          CASE
            WHEN purpose = 'dashboard' AND status IN ('created', 'pending', 'paid')
            THEN GREATEST(COALESCE(amount, 0), 0)
            ELSE 0
          END
        ),
        0
      )::numeric(12,2) AS booked_total,
      COALESCE(
        SUM(
          CASE
            WHEN purpose = 'dashboard' AND status = 'paid'
            THEN GREATEST(COALESCE(NULLIF(paid_amount, 0), amount, 0), 0)
            ELSE 0
          END
        ),
        0
      )::numeric(12,2) AS paid_total
    FROM admin_payment_links
  `;
  const scopedAdminId = toScopedAdminId(adminId);
  if (scopedAdminId) {
    query += ' WHERE admin_id = ?';
    params.push(scopedAdminId);
  }
  const [rows] = await connection.query(query, params);
  const row = rows?.[0] || {};
  const booked = toAmountValue(row?.booked_total || 0);
  const paid = toAmountValue(row?.paid_total || 0);
  const outstanding = toAmountValue(Math.max(booked - paid, 0));
  return {
    booked,
    paid,
    outstanding,
  };
};

const buildDashboardSubscriptionTrend = async (
  connection,
  adminId = null,
  days = DASHBOARD_TREND_DAYS
) => {
  const safeDays = Number.isFinite(Number(days)) ? Math.max(2, Math.min(90, Number(days))) : DASHBOARD_TREND_DAYS;
  const startUtc = new Date();
  startUtc.setUTCHours(0, 0, 0, 0);
  startUtc.setUTCDate(startUtc.getUTCDate() - (safeDays - 1));

  const params = [startUtc.toISOString()];
  let query = `
    SELECT
      TO_CHAR(DATE(created_at), 'YYYY-MM-DD') AS day_key,
      SUM(
        CASE
          WHEN purpose = 'dashboard' AND status IN ('created', 'pending', 'paid')
          THEN GREATEST(COALESCE(amount, 0), 0)
          ELSE 0
        END
      )::numeric(12,2) AS booked_total,
      SUM(
        CASE
          WHEN purpose = 'dashboard' AND status = 'paid'
          THEN GREATEST(COALESCE(NULLIF(paid_amount, 0), amount, 0), 0)
          ELSE 0
        END
      )::numeric(12,2) AS earned_total
    FROM admin_payment_links
    WHERE created_at >= ?
  `;
  const scopedAdminId = toScopedAdminId(adminId);
  if (scopedAdminId) {
    query += ' AND admin_id = ?';
    params.push(scopedAdminId);
  }
  query += `
    GROUP BY DATE(created_at)
    ORDER BY DATE(created_at) ASC
  `;

  const [rows] = await connection.query(query, params);
  const totalsByDay = new Map(
    (rows || []).map((row) => [
      String(row.day_key || ''),
      {
        earned: toAmountValue(row.earned_total),
        booked: toAmountValue(row.booked_total),
      },
    ])
  );

  const trend = [];
  for (let i = 0; i < safeDays; i += 1) {
    const current = new Date(startUtc);
    current.setUTCDate(startUtc.getUTCDate() + i);
    const key = toUtcDateKey(current);
    const dayTotals = totalsByDay.get(key) || { earned: 0, booked: 0 };
    trend.push({
      date: key,
      label: toDashboardTrendLabel(current),
      earned: toAmountValue(dayTotals.earned),
      booked: toAmountValue(dayTotals.booked),
    });
  }
  return trend;
};

const getDashboardOrderRevenueSnapshotFromOrders = async (connection, adminId = null) => {
  const params = [];
  const whatsappPredicate = buildWhatsAppChannelPredicate('o.channel');
  let query = `
    SELECT
      SUM(
        CASE
          WHEN ${whatsappPredicate}
          THEN 1
          ELSE 0
        END
      )::int AS whatsapp_orders,
      SUM(
        CASE
          WHEN ${whatsappPredicate}
           AND COALESCE(NULLIF(BTRIM(o.payment_status), ''), 'pending') <> 'refunded'
           AND GREATEST(COALESCE(o.payment_paid, 0), 0) > 0
          THEN 1
          ELSE 0
        END
      )::int AS whatsapp_paid_orders,
      COALESCE(
        SUM(
          CASE
            WHEN ${whatsappPredicate}
            THEN GREATEST(COALESCE(o.payment_total, 0), 0)
            ELSE 0
          END
        ),
        0
      )::numeric(12,2) AS whatsapp_revenue_booked,
      COALESCE(
        SUM(
          CASE
            WHEN ${whatsappPredicate}
             AND COALESCE(NULLIF(BTRIM(o.payment_status), ''), 'pending') <> 'refunded'
            THEN LEAST(
              GREATEST(COALESCE(o.payment_paid, 0), 0),
              GREATEST(COALESCE(o.payment_total, 0), 0)
            )
            ELSE 0
          END
        ),
        0
      )::numeric(12,2) AS whatsapp_revenue_paid
    FROM orders o
  `;
  const scopedAdminId = toScopedAdminId(adminId);
  if (scopedAdminId) {
    query += ' WHERE o.admin_id = ?';
    params.push(scopedAdminId);
  }
  const [rows] = await connection.query(query, params);
  const row = rows?.[0] || {};
  return {
    whatsapp_orders: Number(row?.whatsapp_orders || 0),
    whatsapp_paid_orders: Number(row?.whatsapp_paid_orders || 0),
    whatsapp_revenue_booked: toAmountValue(row?.whatsapp_revenue_booked || 0),
    whatsapp_revenue_paid: toAmountValue(row?.whatsapp_revenue_paid || 0),
  };
};

const buildDashboardRevenueTrendFromOrders = async (
  connection,
  adminId = null,
  days = DASHBOARD_TREND_DAYS
) => {
  const safeDays = Number.isFinite(Number(days)) ? Math.max(2, Math.min(90, Number(days))) : DASHBOARD_TREND_DAYS;
  const startUtc = new Date();
  startUtc.setUTCHours(0, 0, 0, 0);
  startUtc.setUTCDate(startUtc.getUTCDate() - (safeDays - 1));

  const params = [startUtc.toISOString()];
  const whatsappPredicate = buildWhatsAppChannelPredicate('o.channel');
  let query = `
    SELECT
      TO_CHAR(DATE(COALESCE(o.placed_at, o.created_at)), 'YYYY-MM-DD') AS day_key,
      SUM(
        CASE
          WHEN ${whatsappPredicate}
           AND COALESCE(NULLIF(BTRIM(o.payment_status), ''), 'pending') <> 'refunded'
          THEN LEAST(
            GREATEST(COALESCE(o.payment_paid, 0), 0),
            GREATEST(COALESCE(o.payment_total, 0), 0)
          )
          ELSE 0
        END
      )::numeric(12,2) AS earned_total,
      SUM(
        CASE
          WHEN ${whatsappPredicate}
          THEN GREATEST(COALESCE(o.payment_total, 0), 0)
          ELSE 0
        END
      )::numeric(12,2) AS booked_total
    FROM orders o
    WHERE COALESCE(o.placed_at, o.created_at) >= ?
  `;
  const scopedAdminId = toScopedAdminId(adminId);
  if (scopedAdminId) {
    query += ' AND o.admin_id = ?';
    params.push(scopedAdminId);
  }
  query += `
    GROUP BY DATE(COALESCE(o.placed_at, o.created_at))
    ORDER BY DATE(COALESCE(o.placed_at, o.created_at)) ASC
  `;

  const [rows] = await connection.query(query, params);
  const totalsByDay = new Map(
    (rows || []).map((row) => [
      String(row.day_key || ''),
      {
        earned: toAmountValue(row.earned_total),
        booked: toAmountValue(row.booked_total),
      },
    ])
  );

  const trend = [];
  for (let i = 0; i < safeDays; i += 1) {
    const current = new Date(startUtc);
    current.setUTCDate(startUtc.getUTCDate() + i);
    const key = toUtcDateKey(current);
    const dayTotals = totalsByDay.get(key) || { earned: 0, booked: 0 };
    trend.push({
      date: key,
      label: toDashboardTrendLabel(current),
      earned: toAmountValue(dayTotals.earned),
      booked: toAmountValue(dayTotals.booked),
    });
  }
  return trend;
};

const buildDashboardRevenueAnalysis = (trend = []) => {
  if (!Array.isArray(trend) || trend.length === 0) {
    return {
      trend_direction: 'flat',
      growth_percent: 0,
      slowdown_percent: 0,
      compare_window_days: 0,
      recent_total: 0,
      previous_total: 0,
      recent_daily_avg: 0,
      previous_daily_avg: 0,
      total_earned: 0,
      total_booked: 0,
      outstanding_total: 0,
      top_day: null,
      insight: 'No WhatsApp revenue yet.',
    };
  }

  const totalEarned = toAmountValue(
    trend.reduce((sum, point) => sum + toAmountValue(point?.earned), 0)
  );
  const totalBooked = toAmountValue(
    trend.reduce((sum, point) => sum + toAmountValue(point?.booked), 0)
  );
  const outstanding = toAmountValue(Math.max(totalBooked - totalEarned, 0));
  const compareWindowDays = Math.max(3, Math.floor(trend.length / 2));
  if (totalEarned <= 0 && totalBooked <= 0) {
    return {
      trend_direction: 'flat',
      growth_percent: 0,
      slowdown_percent: 0,
      compare_window_days: compareWindowDays,
      recent_total: 0,
      previous_total: 0,
      recent_daily_avg: 0,
      previous_daily_avg: 0,
      total_earned: 0,
      total_booked: 0,
      outstanding_total: 0,
      top_day: null,
      insight: `No WhatsApp revenue data available for the last ${trend.length} days.`,
    };
  }
  const recentSlice = trend.slice(-compareWindowDays);
  const previousSlice = trend.slice(-compareWindowDays * 2, -compareWindowDays);
  const recentTotal = toAmountValue(
    recentSlice.reduce((sum, point) => sum + toAmountValue(point?.earned), 0)
  );
  const previousTotal = toAmountValue(
    previousSlice.reduce((sum, point) => sum + toAmountValue(point?.earned), 0)
  );
  const recentAvg = toAmountValue(recentTotal / Math.max(1, recentSlice.length));
  const previousAvg = toAmountValue(
    previousSlice.length ? previousTotal / previousSlice.length : 0
  );

  let trendDirection = 'flat';
  let growthPercent = 0;
  let slowdownPercent = 0;

  if (previousTotal > 0) {
    const deltaPct = ((recentTotal - previousTotal) / previousTotal) * 100;
    if (deltaPct > 0.5) {
      trendDirection = 'up';
      growthPercent = Number(deltaPct.toFixed(1));
    } else if (deltaPct < -0.5) {
      trendDirection = 'down';
      slowdownPercent = Number(Math.abs(deltaPct).toFixed(1));
    }
  } else if (recentTotal > 0) {
    trendDirection = 'up';
    growthPercent = 100;
  }

  const topDay = trend.reduce((best, point) => {
    const earned = toAmountValue(point?.earned);
    if (!best || earned > best.earned) {
      return {
        date: point?.date || '',
        label: point?.label || point?.date || '',
        earned,
      };
    }
    return best;
  }, null);

  const insight =
    trendDirection === 'up'
      ? `WhatsApp revenue is growing ${growthPercent}% vs previous ${compareWindowDays} days.`
      : trendDirection === 'down'
      ? `WhatsApp revenue slowed ${slowdownPercent}% vs previous ${compareWindowDays} days.`
      : `WhatsApp revenue is stable vs previous ${compareWindowDays} days.`;

  return {
    trend_direction: trendDirection,
    growth_percent: growthPercent,
    slowdown_percent: slowdownPercent,
    compare_window_days: compareWindowDays,
    recent_total: recentTotal,
    previous_total: previousTotal,
    recent_daily_avg: recentAvg,
    previous_daily_avg: previousAvg,
    total_earned: totalEarned,
    total_booked: totalBooked,
    outstanding_total: outstanding,
    top_day: topDay,
    insight,
  };
};

const formatQuery = (text, params = []) => {
  if (!params.length) return text;
  let index = 0;
  return text.replace(/\?/g, () => `$${++index}`);
};

export function getPool() {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });
  }
  return pool;
}

const toTrimmedString = (value) => String(value || '').trim();

const normalizeBillingPrice = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return Number(num.toFixed(4));
};

const normalizeBillingBoolean = (value) => {
  if (value === true || value === false) return value;
  return null;
};

const normalizeBillingDate = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const date = value instanceof Date ? value : new Date(String(value));
  if (Number.isNaN(date.getTime())) return null;
  return date;
};

export async function getAdminBillingSettings(adminId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT admin_id, razorpay_key_id, razorpay_key_secret,
               charge_enabled, free_until,
               input_price_usd_per_1m, output_price_usd_per_1m,
               dashboard_charge_enabled,
               dashboard_service_inr,
               dashboard_product_inr,
               dashboard_both_inr,
               dashboard_booking_inr,
               created_at, updated_at
        FROM admin_billing_settings
        WHERE admin_id = ?
        LIMIT 1
      `,
      [adminId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function upsertAdminBillingSettings(adminId, updates = {}) {
  const connection = await getConnection();
  try {
    const [existingRows] = await connection.query(
      `SELECT admin_id FROM admin_billing_settings WHERE admin_id = ? LIMIT 1`,
      [adminId]
    );
    const exists = Boolean(existingRows?.[0]);
    const fields = [];
    const values = [];

    if (Object.prototype.hasOwnProperty.call(updates, 'razorpay_key_id')) {
      const normalized = toTrimmedString(updates.razorpay_key_id);
      fields.push('razorpay_key_id = ?');
      values.push(normalized || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'razorpay_key_secret')) {
      const normalized = toTrimmedString(updates.razorpay_key_secret);
      fields.push('razorpay_key_secret = ?');
      values.push(normalized || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'charge_enabled')) {
      const normalized = normalizeBillingBoolean(updates.charge_enabled);
      if (normalized !== null) {
        fields.push('charge_enabled = ?');
        values.push(normalized);
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'dashboard_charge_enabled')) {
      const normalized = normalizeBillingBoolean(updates.dashboard_charge_enabled);
      if (normalized !== null) {
        fields.push('dashboard_charge_enabled = ?');
        values.push(normalized);
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'free_until')) {
      const normalized = normalizeBillingDate(updates.free_until);
      fields.push('free_until = ?');
      values.push(normalized);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'input_price_usd_per_1m')) {
      const normalized = normalizeBillingPrice(updates.input_price_usd_per_1m);
      fields.push('input_price_usd_per_1m = ?');
      values.push(normalized);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'output_price_usd_per_1m')) {
      const normalized = normalizeBillingPrice(updates.output_price_usd_per_1m);
      fields.push('output_price_usd_per_1m = ?');
      values.push(normalized);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'dashboard_service_inr')) {
      const normalized = normalizeCurrencyAmount(updates.dashboard_service_inr);
      fields.push('dashboard_service_inr = ?');
      values.push(normalized);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'dashboard_product_inr')) {
      const normalized = normalizeCurrencyAmount(updates.dashboard_product_inr);
      fields.push('dashboard_product_inr = ?');
      values.push(normalized);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'dashboard_both_inr')) {
      const normalized = normalizeCurrencyAmount(updates.dashboard_both_inr);
      fields.push('dashboard_both_inr = ?');
      values.push(normalized);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'dashboard_booking_inr')) {
      const normalized = normalizeCurrencyAmount(updates.dashboard_booking_inr);
      fields.push('dashboard_booking_inr = ?');
      values.push(normalized);
    }

    if (fields.length === 0) {
      return await getAdminBillingSettings(adminId);
    }

    if (exists) {
      values.push(adminId);
      await connection.query(
        `UPDATE admin_billing_settings SET ${fields.join(', ')} WHERE admin_id = ?`,
        values
      );
    } else {
      const insertColumns = ['admin_id', ...fields.map((field) => field.split('=')[0].trim())];
      const insertValues = [adminId, ...values];
      if (!insertColumns.includes('charge_enabled')) {
        insertColumns.push('charge_enabled');
        insertValues.push(false);
      }
      const placeholders = insertColumns.map(() => '?').join(', ');
      await connection.query(
        `INSERT INTO admin_billing_settings (${insertColumns.join(', ')}) VALUES (${placeholders})`,
        insertValues
      );
    }

    return await getAdminBillingSettings(adminId);
  } finally {
    connection.release();
  }
}

export async function getAdminRazorpayCredentials(adminId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT razorpay_key_id, razorpay_key_secret
        FROM admin_billing_settings
        WHERE admin_id = ?
        LIMIT 1
      `,
      [adminId]
    );
    const row = rows[0];
    const keyId = toTrimmedString(row?.razorpay_key_id);
    const keySecret = toTrimmedString(row?.razorpay_key_secret);
    if (!keyId || !keySecret) return null;
    return { keyId, keySecret };
  } finally {
    connection.release();
  }
}

export async function getSuperAdminRazorpayCredentials() {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT a.id, s.razorpay_key_id, s.razorpay_key_secret
        FROM admins a
        JOIN admin_billing_settings s ON s.admin_id = a.id
        WHERE a.admin_tier = 'super_admin'
          AND COALESCE(NULLIF(btrim(s.razorpay_key_id), ''), '') <> ''
          AND COALESCE(NULLIF(btrim(s.razorpay_key_secret), ''), '') <> ''
        ORDER BY a.id ASC
        LIMIT 1
      `
    );
    const row = rows[0];
    const keyId = toTrimmedString(row?.razorpay_key_id);
    const keySecret = toTrimmedString(row?.razorpay_key_secret);
    if (!keyId || !keySecret) return null;
    return { keyId, keySecret, adminId: row?.id };
  } finally {
    connection.release();
  }
}

export async function getEffectiveRazorpayCredentials(adminId) {
  const adminCreds = await getAdminRazorpayCredentials(adminId);
  if (adminCreds?.keyId && adminCreds?.keySecret) {
    return adminCreds;
  }
  const superCreds = await getSuperAdminRazorpayCredentials();
  if (superCreds?.keyId && superCreds?.keySecret) {
    return superCreds;
  }
  return null;
}

const toDashboardAmount = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return 0;
  return Number(num.toFixed(2));
};

export async function getDashboardChargeRates() {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT
          s.dashboard_service_inr,
          s.dashboard_product_inr,
          s.dashboard_both_inr,
          s.dashboard_booking_inr
        FROM admins a
        JOIN admin_billing_settings s ON s.admin_id = a.id
        WHERE a.admin_tier = 'super_admin'
        ORDER BY a.id ASC
        LIMIT 1
      `
    );
    const row = rows[0] || {};
    return {
      service_inr: toDashboardAmount(row.dashboard_service_inr),
      product_inr: toDashboardAmount(row.dashboard_product_inr),
      both_inr: toDashboardAmount(row.dashboard_both_inr),
      booking_inr: toDashboardAmount(row.dashboard_booking_inr),
    };
  } finally {
    connection.release();
  }
}

const normalizeTokenBalance = (value, fallback = 0) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.max(0, Math.trunc(num));
};

const getMonthStartUtc = (date = new Date()) =>
  new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));

const getMonthDiff = (fromDate, toDate) => {
  const from = fromDate instanceof Date ? fromDate : new Date(fromDate);
  const to = toDate instanceof Date ? toDate : new Date(toDate);
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return 0;
  return (to.getUTCFullYear() - from.getUTCFullYear()) * 12 + (to.getUTCMonth() - from.getUTCMonth());
};

const applyMonthlyFreeRefresh = ({
  freeInput = 0,
  freeOutput = 0,
  paidInput = 0,
  paidOutput = 0,
  lastResetAt = null,
  now = new Date(),
} = {}) => {
  const safeFreeInput = normalizeTokenBalance(freeInput, 0);
  const safeFreeOutput = normalizeTokenBalance(freeOutput, 0);
  const safePaidInput = normalizeTokenBalance(paidInput, 0);
  const safePaidOutput = normalizeTokenBalance(paidOutput, 0);
  const monthStart = getMonthStartUtc(now);
  const lastReset = lastResetAt ? new Date(lastResetAt) : null;
  const monthsElapsed = lastReset ? Math.max(getMonthDiff(lastReset, monthStart), 0) : 0;

  if (!lastReset || Number.isNaN(lastReset.getTime())) {
    return {
      freeInput: Math.min(
        Math.max(safeFreeInput || FREE_INPUT_TOKENS_PER_MONTH, 0),
        FREE_INPUT_TOKEN_CAP
      ),
      freeOutput: Math.min(
        Math.max(safeFreeOutput || FREE_OUTPUT_TOKENS_PER_MONTH, 0),
        FREE_OUTPUT_TOKEN_CAP
      ),
      paidInput: safePaidInput,
      paidOutput: safePaidOutput,
      lastResetAt: monthStart,
      refreshedMonths: 0,
      changed: true,
    };
  }

  if (monthsElapsed <= 0) {
    return {
      freeInput: safeFreeInput,
      freeOutput: safeFreeOutput,
      paidInput: safePaidInput,
      paidOutput: safePaidOutput,
      lastResetAt: lastReset,
      refreshedMonths: 0,
      changed: false,
    };
  }

  const inputTopup = FREE_INPUT_TOKENS_PER_MONTH * monthsElapsed;
  const outputTopup = FREE_OUTPUT_TOKENS_PER_MONTH * monthsElapsed;
  return {
    freeInput: Math.min(safeFreeInput + inputTopup, FREE_INPUT_TOKEN_CAP),
    freeOutput: Math.min(safeFreeOutput + outputTopup, FREE_OUTPUT_TOKEN_CAP),
    paidInput: safePaidInput,
    paidOutput: safePaidOutput,
    lastResetAt: monthStart,
    refreshedMonths: monthsElapsed,
    changed: true,
  };
};

export async function recordAdminAiUsage({
  adminId,
  contactId = null,
  model = '',
  prompt = '',
  response = '',
} = {}) {
  if (!Number.isFinite(Number(adminId)) || !adminId) return null;
  const connection = await getConnection();
  try {
    const settings = await getAdminBillingSettings(adminId);
    const inputPrice = settings?.input_price_usd_per_1m ?? DEFAULT_INPUT_USD_PER_1M;
    const outputPrice = settings?.output_price_usd_per_1m ?? DEFAULT_OUTPUT_USD_PER_1M;
    const now = new Date();
    const freeUntil = settings?.free_until ? new Date(settings.free_until) : null;
    const freeUntilActive =
      freeUntil && !Number.isNaN(freeUntil.getTime()) && freeUntil > now;
    const inputTokens = estimateTokens(prompt);
    const outputTokens = estimateTokens(response);

    const consumeTokens = (needed, freeBalance, paidBalance) => {
      const fromFree = Math.min(needed, freeBalance);
      const remainingAfterFree = Math.max(needed - fromFree, 0);
      const fromPaid = Math.min(remainingAfterFree, paidBalance);
      const payg = Math.max(remainingAfterFree - fromPaid, 0);
      return {
        newFree: freeBalance - fromFree,
        newPaid: paidBalance - fromPaid,
        payg,
      };
    };

    await connection.query('BEGIN');
    const [adminRows] = await connection.query(
      `
        SELECT
          admin_tier,
          free_input_tokens,
          free_output_tokens,
          paid_input_tokens,
          paid_output_tokens,
          free_tokens_reset_at
        FROM admins
        WHERE id = ?
        FOR UPDATE
      `,
      [adminId]
    );
    const adminRow = adminRows?.[0];
    if (!adminRow) {
      await connection.query('ROLLBACK');
      return null;
    }

    const tokenSystemEnabled =
      adminRow.admin_tier === 'super_admin' || settings?.charge_enabled === true;
    const isFree = !tokenSystemEnabled || Boolean(freeUntilActive);

    const refreshed = applyMonthlyFreeRefresh({
      freeInput: adminRow.free_input_tokens,
      freeOutput: adminRow.free_output_tokens,
      paidInput: adminRow.paid_input_tokens,
      paidOutput: adminRow.paid_output_tokens,
      lastResetAt: adminRow.free_tokens_reset_at,
      now,
    });

    let freeInput = refreshed.freeInput;
    let freeOutput = refreshed.freeOutput;
    let paidInput = refreshed.paidInput;
    let paidOutput = refreshed.paidOutput;

    let billableInputTokens = 0;
    let billableOutputTokens = 0;

    if (!isFree) {
      const inputConsumption = consumeTokens(inputTokens, freeInput, paidInput);
      const outputConsumption = consumeTokens(outputTokens, freeOutput, paidOutput);
      freeInput = inputConsumption.newFree;
      paidInput = inputConsumption.newPaid;
      freeOutput = outputConsumption.newFree;
      paidOutput = outputConsumption.newPaid;
      billableInputTokens = inputConsumption.payg;
      billableOutputTokens = outputConsumption.payg;
    }

    await connection.query(
      `
        UPDATE admins
        SET free_input_tokens = ?,
            free_output_tokens = ?,
            paid_input_tokens = ?,
            paid_output_tokens = ?,
            free_tokens_reset_at = ?
        WHERE id = ?
      `,
      [
        freeInput,
        freeOutput,
        paidInput,
        paidOutput,
        refreshed.lastResetAt,
        adminId,
      ]
    );

    const isBillable = !isFree && (billableInputTokens + billableOutputTokens > 0);
    const costs = computeUsageCosts({
      inputTokens: billableInputTokens,
      outputTokens: billableOutputTokens,
      inputUsdPer1M: inputPrice,
      outputUsdPer1M: outputPrice,
      usdToInrRate: DEFAULT_USD_TO_INR_RATE,
    });
    const billedUsd = isBillable ? costs.totalCostUsd : 0;
    const billedInr = isBillable ? costs.totalCostInr : 0;

    await connection.query(
      `
        INSERT INTO admin_ai_usage (
          admin_id, contact_id, model,
          input_tokens, output_tokens,
          billable_input_tokens, billable_output_tokens,
          input_cost_usd, output_cost_usd, total_cost_usd,
          usd_to_inr_rate, total_cost_inr,
          is_billable
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        adminId,
        contactId,
        toTrimmedString(model) || null,
        inputTokens,
        outputTokens,
        billableInputTokens,
        billableOutputTokens,
        isBillable ? costs.inputCostUsd : 0,
        isBillable ? costs.outputCostUsd : 0,
        billedUsd,
        DEFAULT_USD_TO_INR_RATE,
        billedInr,
        isBillable,
      ]
    );
    await connection.query('COMMIT');
    return {
      inputTokens,
      outputTokens,
      billableInputTokens,
      billableOutputTokens,
      totalCostUsd: billedUsd,
      totalCostInr: billedInr,
      isBillable,
    };
  } catch (error) {
    try {
      await connection.query('ROLLBACK');
    } catch (_rollbackError) {
      // Ignore rollback errors.
    }
    throw error;
  } finally {
    connection.release();
  }
}

export async function getAdminAiUsageSummary(adminId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT
          COUNT(*)::int AS conversation_count,
          COALESCE(SUM(input_tokens), 0)::int AS input_tokens,
          COALESCE(SUM(output_tokens), 0)::int AS output_tokens,
          COALESCE(SUM(billable_input_tokens), 0)::int AS billable_input_tokens,
          COALESCE(SUM(billable_output_tokens), 0)::int AS billable_output_tokens,
          COALESCE(SUM(input_tokens + output_tokens), 0)::int AS total_tokens,
          COALESCE(SUM(CASE WHEN is_billable THEN total_cost_usd ELSE 0 END), 0) AS total_cost_usd,
          COALESCE(SUM(CASE WHEN is_billable THEN total_cost_inr ELSE 0 END), 0) AS total_cost_inr,
          MIN(created_at) AS first_used_at,
          MAX(created_at) AS last_used_at
        FROM admin_ai_usage
        WHERE admin_id = ?
      `,
      [adminId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function getAdminTokenBalances(adminId, { refresh = true } = {}) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT
          free_input_tokens,
          free_output_tokens,
          paid_input_tokens,
          paid_output_tokens,
          free_tokens_reset_at
        FROM admins
        WHERE id = ?
        LIMIT 1
      `,
      [adminId]
    );
    const current = rows?.[0];
    if (!current) return null;
    if (!refresh) {
      return {
        free_input_tokens: normalizeTokenBalance(current.free_input_tokens, 0),
        free_output_tokens: normalizeTokenBalance(current.free_output_tokens, 0),
        paid_input_tokens: normalizeTokenBalance(current.paid_input_tokens, 0),
        paid_output_tokens: normalizeTokenBalance(current.paid_output_tokens, 0),
        free_tokens_reset_at: current.free_tokens_reset_at,
      };
    }

    const refreshed = applyMonthlyFreeRefresh({
      freeInput: current.free_input_tokens,
      freeOutput: current.free_output_tokens,
      paidInput: current.paid_input_tokens,
      paidOutput: current.paid_output_tokens,
      lastResetAt: current.free_tokens_reset_at,
      now: new Date(),
    });

    if (refreshed.changed) {
      await connection.query(
        `
          UPDATE admins
          SET free_input_tokens = ?,
              free_output_tokens = ?,
              paid_input_tokens = ?,
              paid_output_tokens = ?,
              free_tokens_reset_at = ?
          WHERE id = ?
        `,
        [
          refreshed.freeInput,
          refreshed.freeOutput,
          refreshed.paidInput,
          refreshed.paidOutput,
          refreshed.lastResetAt,
          adminId,
        ]
      );
    }

    return {
      free_input_tokens: refreshed.freeInput,
      free_output_tokens: refreshed.freeOutput,
      paid_input_tokens: refreshed.paidInput,
      paid_output_tokens: refreshed.paidOutput,
      free_tokens_reset_at: refreshed.lastResetAt,
    };
  } finally {
    connection.release();
  }
}

export async function getAdminPaygUsageSummary(adminId, { since = null } = {}) {
  const connection = await getConnection();
  try {
    const params = [adminId];
    let sinceClause = '';
    if (since) {
      sinceClause = ' AND created_at >= ?';
      params.push(since);
    }
    const [rows] = await connection.query(
      `
        SELECT
          COALESCE(SUM(billable_input_tokens), 0)::int AS billable_input_tokens,
          COALESCE(SUM(billable_output_tokens), 0)::int AS billable_output_tokens,
          COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
          COALESCE(SUM(total_cost_inr), 0) AS total_cost_inr
        FROM admin_ai_usage
        WHERE admin_id = ?
          AND is_billable = TRUE
          ${sinceClause}
      `,
      params
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function refreshAllAdminTokenBalances(now = new Date()) {
  const connection = await getConnection();
  try {
    const monthStart = getMonthStartUtc(now);
    await connection.query(
      `
        WITH reset AS (
          SELECT
            id,
            GREATEST(
              (
                EXTRACT(
                  YEAR FROM AGE(date_trunc('month', ?::timestamptz), date_trunc('month', COALESCE(free_tokens_reset_at, ?::timestamptz)))
                ) * 12
                + EXTRACT(
                  MONTH FROM AGE(date_trunc('month', ?::timestamptz), date_trunc('month', COALESCE(free_tokens_reset_at, ?::timestamptz)))
                )
              ),
              0
            )::int AS months_elapsed
          FROM admins
        )
        UPDATE admins a
        SET free_input_tokens = LEAST(a.free_input_tokens + (r.months_elapsed * ?), ?),
            free_output_tokens = LEAST(a.free_output_tokens + (r.months_elapsed * ?), ?),
            free_tokens_reset_at = CASE
              WHEN r.months_elapsed > 0 THEN ?
              ELSE a.free_tokens_reset_at
            END
        FROM reset r
        WHERE a.id = r.id
          AND r.months_elapsed > 0
      `,
      [
        monthStart,
        monthStart,
        monthStart,
        monthStart,
        FREE_INPUT_TOKENS_PER_MONTH,
        FREE_INPUT_TOKEN_CAP,
        FREE_OUTPUT_TOKENS_PER_MONTH,
        FREE_OUTPUT_TOKEN_CAP,
        monthStart,
      ]
    );
    return true;
  } finally {
    connection.release();
  }
}

export async function getAdminPaymentTotals(adminId, { purpose = null } = {}) {
  const connection = await getConnection();
  try {
    const params = [adminId];
    const purposeClause = purpose ? ' AND purpose = ?' : '';
    if (purpose) params.push(purpose);
    const [rows] = await connection.query(
      `
        SELECT
          COALESCE(SUM(paid_amount), 0) AS total_paid,
          MAX(paid_at) AS last_paid_at
        FROM admin_payment_links
        WHERE admin_id = ?
          AND status = 'paid'
          ${purposeClause}
      `,
      params
    );
    return rows[0] || { total_paid: 0, last_paid_at: null };
  } finally {
    connection.release();
  }
}

export async function createAdminPaymentLinkRecord({
  adminId,
  paymentLinkId,
  amount = 0,
  baseAmount = null,
  maintenanceFee = null,
  currency = 'INR',
  rawJson = null,
  purpose = 'payg',
  inputTokens = 0,
  outputTokens = 0,
  subscriptionMonths = 0,
  discountPct = 0,
  dashboardMonthlyAmount = 0,
} = {}) {
  const connection = await getConnection();
  try {
    await connection.query(
      `
        INSERT INTO admin_payment_links (
          admin_id, payment_link_id, amount, base_amount, maintenance_fee, currency, purpose, input_tokens, output_tokens,
          subscription_months, discount_pct, dashboard_monthly_amount, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (payment_link_id) DO NOTHING
      `,
      [
        adminId,
        paymentLinkId,
        normalizeCurrencyAmount(amount) || 0,
        baseAmount !== null && baseAmount !== undefined
          ? normalizeCurrencyAmount(baseAmount) || 0
          : normalizeCurrencyAmount(amount) || 0,
        maintenanceFee !== null && maintenanceFee !== undefined
          ? normalizeCurrencyAmount(maintenanceFee) || 0
          : 0,
        currency,
        purpose,
        Math.max(0, Math.trunc(Number(inputTokens) || 0)),
        Math.max(0, Math.trunc(Number(outputTokens) || 0)),
        Math.max(0, Math.trunc(Number(subscriptionMonths) || 0)),
        normalizeCurrencyAmount(discountPct) || 0,
        normalizeCurrencyAmount(dashboardMonthlyAmount) || 0,
        rawJson,
      ]
    );
    return true;
  } finally {
    connection.release();
  }
}

export async function updateAdminPaymentLinkStatus({
  paymentLinkId,
  status,
  paidAmount = 0,
  paidAt = null,
  rawJson = null,
  onlyIfNotPaid = false,
} = {}) {
  const connection = await getConnection();
  try {
    const updates = [];
    const values = [];
    if (typeof status === 'string' && status.trim()) {
      updates.push('status = ?');
      values.push(status.trim());
    }
    if (paidAmount !== undefined) {
      const normalized = normalizeCurrencyAmount(paidAmount) || 0;
      updates.push('paid_amount = ?');
      values.push(normalized);
    }
    if (paidAt !== undefined) {
      updates.push('paid_at = ?');
      values.push(paidAt ? new Date(paidAt) : null);
    }
    if (rawJson !== undefined) {
      updates.push('raw_json = ?');
      values.push(rawJson);
    }
    if (!updates.length) return false;
    values.push(paymentLinkId);
    const whereClause = onlyIfNotPaid
      ? `WHERE payment_link_id = ? AND status <> 'paid'`
      : `WHERE payment_link_id = ?`;
    const [, result] = await connection.query(
      `UPDATE admin_payment_links SET ${updates.join(', ')} ${whereClause}`,
      values
    );
    return Number(result?.rowCount ?? result?.affectedRows ?? 0);
  } finally {
    connection.release();
  }
}

export async function getAdminPaymentLink(paymentLinkId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT
          id,
          admin_id,
          payment_link_id,
          amount,
          base_amount,
          maintenance_fee,
          currency,
          purpose,
          input_tokens,
          output_tokens,
          subscription_months,
          discount_pct,
          dashboard_monthly_amount,
          status,
          paid_amount,
          paid_at
        FROM admin_payment_links
        WHERE payment_link_id = ?
        LIMIT 1
      `,
      [paymentLinkId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function creditAdminPaidTokens({
  adminId,
  inputTokens = 0,
  outputTokens = 0,
} = {}) {
  const connection = await getConnection();
  try {
    const safeInput = Math.max(0, Math.trunc(Number(inputTokens) || 0));
    const safeOutput = Math.max(0, Math.trunc(Number(outputTokens) || 0));
    if (!adminId || (!safeInput && !safeOutput)) return false;
    await connection.query(
      `
        UPDATE admins
        SET paid_input_tokens = paid_input_tokens + ?,
            paid_output_tokens = paid_output_tokens + ?
        WHERE id = ?
      `,
      [safeInput, safeOutput, adminId]
    );
    return true;
  } finally {
    connection.release();
  }
}

export async function extendAdminDashboardSubscription({ adminId, months = 1 } = {}) {
  const connection = await getConnection();
  try {
    const safeMonths = Math.max(1, Math.trunc(Number(months) || 1));
    await connection.query(
      `
        UPDATE admins
        SET dashboard_subscription_expires_at = CASE
          WHEN dashboard_subscription_expires_at IS NULL OR dashboard_subscription_expires_at < NOW()
            THEN NOW() + (INTERVAL '1 month' * ?)
          ELSE dashboard_subscription_expires_at + (INTERVAL '1 month' * ?)
        END
        WHERE id = ?
      `,
      [safeMonths, safeMonths, adminId]
    );
    return true;
  } finally {
    connection.release();
  }
}

async function ensureAdminBusinessColumns() {
  adminBusinessColumnsInitStarted = true;
  if (adminBusinessColumnsReadyPromise) {
    return adminBusinessColumnsReadyPromise;
  }

  const poolRef = getPool();
  adminBusinessColumnsReadyPromise = (async () => {
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS business_name VARCHAR(140)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS business_category VARCHAR(120)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS business_type VARCHAR(20)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS service_label VARCHAR(60)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS product_label VARCHAR(60)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS dashboard_subscription_expires_at TIMESTAMPTZ`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS booking_enabled BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS business_address TEXT`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS business_hours VARCHAR(160)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS business_map_url TEXT`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS access_expires_at TIMESTAMPTZ`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS automation_enabled BOOLEAN NOT NULL DEFAULT TRUE`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS automation_trigger_mode VARCHAR(20) NOT NULL DEFAULT 'any'`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS automation_trigger_keyword VARCHAR(40)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS appointment_start_hour SMALLINT NOT NULL DEFAULT 9`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS appointment_end_hour SMALLINT NOT NULL DEFAULT 20`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS appointment_slot_minutes SMALLINT NOT NULL DEFAULT 60`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS appointment_window_months SMALLINT NOT NULL DEFAULT 3`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_enabled BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_min_amount NUMERIC(10,2)`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_scope VARCHAR(30) NOT NULL DEFAULT 'combined'`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_product_rules JSONB NOT NULL DEFAULT '[]'::jsonb`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS whatsapp_service_limit SMALLINT NOT NULL DEFAULT 3`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS whatsapp_product_limit SMALLINT NOT NULL DEFAULT 3`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_input_tokens INT NOT NULL DEFAULT 100000`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_output_tokens INT NOT NULL DEFAULT 100000`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS paid_input_tokens INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS paid_output_tokens INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_tokens_reset_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS two_factor_enabled BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS login_otp_hash TEXT`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS login_otp_expires_at TIMESTAMPTZ`
    );
    await poolRef.query(
      `ALTER TABLE admins ADD COLUMN IF NOT EXISTS login_otp_attempts SMALLINT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE contacts ADD COLUMN IF NOT EXISTS automation_disabled BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE contacts ADD COLUMN IF NOT EXISTS automation_activated BOOLEAN NOT NULL DEFAULT TRUE`
    );
    await poolRef.query(
      `ALTER TABLE contacts ADD COLUMN IF NOT EXISTS automation_activated_at TIMESTAMPTZ`
    );
    await poolRef.query(
      `ALTER TABLE contacts ADD COLUMN IF NOT EXISTS previous_owner_admin VARCHAR(180)`
    );
    await poolRef.query(
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS reason_of_contacting TEXT`
    );
    await poolRef.query(
      `
        UPDATE leads
        SET reason_of_contacting = LEFT(
          regexp_replace(COALESCE(requirement_text, ''), E'[\\n\\r\\t]+', ' ', 'g'),
          220
        )
        WHERE (reason_of_contacting IS NULL OR btrim(reason_of_contacting) = '')
          AND requirement_text IS NOT NULL
          AND btrim(requirement_text) <> ''
      `
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS duration_value NUMERIC(10,2)`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS duration_unit VARCHAR(20)`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS quantity_value NUMERIC(10,3)`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(40)`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS is_booking_item BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS payment_required BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS free_delivery_eligible BOOLEAN NOT NULL DEFAULT FALSE`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS show_on_whatsapp BOOLEAN NOT NULL DEFAULT TRUE`
    );
    await poolRef.query(
      `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS whatsapp_sort_order INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_ai_usage ADD COLUMN IF NOT EXISTS billable_input_tokens INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_ai_usage ADD COLUMN IF NOT EXISTS billable_output_tokens INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE appointments ADD COLUMN IF NOT EXISTS appointment_kind VARCHAR(20) NOT NULL DEFAULT 'service'`
    );
    await poolRef.query(
      `ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_transaction_id VARCHAR(120)`
    );
    await poolRef.query(
      `ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_gateway_payment_id VARCHAR(120)`
    );
    await poolRef.query(
      `ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_link_id VARCHAR(120)`
    );
    await poolRef.query(
      `UPDATE catalog_items
       SET duration_value = duration_minutes,
           duration_unit = COALESCE(NULLIF(duration_unit, ''), 'minutes')
       WHERE duration_minutes IS NOT NULL
         AND duration_value IS NULL`
    );
    await poolRef.query(
      `
        UPDATE catalog_items
        SET is_booking_item = TRUE
        WHERE item_type = 'service'
          AND is_bookable = TRUE
          AND LOWER(COALESCE(category, '')) = 'booking'
      `
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS signup_verifications (
          id SERIAL PRIMARY KEY,
          email VARCHAR(150) UNIQUE NOT NULL,
          code_hash TEXT NOT NULL,
          payload_json JSONB NOT NULL,
          attempts INT NOT NULL DEFAULT 0,
          expires_at TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS signup_verifications_expires_idx ON signup_verifications (expires_at)`
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS order_revenue (
          id SERIAL PRIMARY KEY,
          order_id INT NOT NULL UNIQUE REFERENCES orders(id) ON DELETE CASCADE,
          admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
          channel VARCHAR(50) DEFAULT 'WhatsApp',
          payment_currency VARCHAR(10) DEFAULT 'INR',
          booked_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
          collected_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
          outstanding_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
          payment_status VARCHAR(20) NOT NULL DEFAULT 'pending'
            CHECK (payment_status IN ('pending', 'paid', 'failed', 'refunded')),
          payment_method VARCHAR(30),
          revenue_date DATE NOT NULL DEFAULT CURRENT_DATE,
          placed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT order_revenue_booked_nonneg CHECK (booked_amount >= 0),
          CONSTRAINT order_revenue_collected_nonneg CHECK (collected_amount >= 0),
          CONSTRAINT order_revenue_outstanding_nonneg CHECK (outstanding_amount >= 0)
        )
      `
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS order_revenue_admin_date_idx ON order_revenue (admin_id, revenue_date DESC)`
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS order_revenue_channel_idx ON order_revenue (channel)`
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS order_revenue_payment_status_idx ON order_revenue (payment_status)`
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS order_payment_link_timers (
          order_id INT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
          admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
          scheduled_for TIMESTAMPTZ NOT NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'scheduled'
            CHECK (status IN ('scheduled', 'processing', 'sent', 'failed', 'cancelled')),
          attempts INT NOT NULL DEFAULT 0,
          max_attempts INT NOT NULL DEFAULT 3,
          last_error TEXT,
          payload_json JSONB,
          last_payment_link_id VARCHAR(120),
          created_by INT REFERENCES admins(id) ON DELETE SET NULL,
          processing_started_at TIMESTAMPTZ,
          sent_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT order_payment_link_timers_attempts_nonneg CHECK (attempts >= 0),
          CONSTRAINT order_payment_link_timers_max_attempts_positive CHECK (max_attempts >= 1)
        )
      `
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS order_payment_link_timers_status_due_idx ON order_payment_link_timers (status, scheduled_for ASC)`
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS order_payment_link_timers_admin_idx ON order_payment_link_timers (admin_id, status)`
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS admin_billing_settings (
          admin_id INT PRIMARY KEY REFERENCES admins(id) ON DELETE CASCADE,
          razorpay_key_id VARCHAR(120),
          razorpay_key_secret VARCHAR(160),
          charge_enabled BOOLEAN NOT NULL DEFAULT TRUE,
          free_until TIMESTAMPTZ,
          input_price_usd_per_1m NUMERIC(12,4),
          output_price_usd_per_1m NUMERIC(12,4),
          dashboard_charge_enabled BOOLEAN NOT NULL DEFAULT TRUE,
          dashboard_service_inr NUMERIC(12,2),
          dashboard_product_inr NUMERIC(12,2),
          dashboard_both_inr NUMERIC(12,2),
          dashboard_booking_inr NUMERIC(12,2),
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS admin_billing_settings_charge_idx ON admin_billing_settings (charge_enabled)`
    );
    await poolRef.query(
      `ALTER TABLE admin_billing_settings ADD COLUMN IF NOT EXISTS dashboard_charge_enabled BOOLEAN NOT NULL DEFAULT TRUE`
    );
    await poolRef.query(
      `ALTER TABLE admin_billing_settings ADD COLUMN IF NOT EXISTS dashboard_service_inr NUMERIC(12,2)`
    );
    await poolRef.query(
      `ALTER TABLE admin_billing_settings ADD COLUMN IF NOT EXISTS dashboard_product_inr NUMERIC(12,2)`
    );
    await poolRef.query(
      `ALTER TABLE admin_billing_settings ADD COLUMN IF NOT EXISTS dashboard_both_inr NUMERIC(12,2)`
    );
    await poolRef.query(
      `ALTER TABLE admin_billing_settings ADD COLUMN IF NOT EXISTS dashboard_booking_inr NUMERIC(12,2)`
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS admin_ai_usage (
          id SERIAL PRIMARY KEY,
          admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
          contact_id INT REFERENCES contacts(id) ON DELETE SET NULL,
          model VARCHAR(80),
          input_tokens INT NOT NULL DEFAULT 0,
          output_tokens INT NOT NULL DEFAULT 0,
          billable_input_tokens INT NOT NULL DEFAULT 0,
          billable_output_tokens INT NOT NULL DEFAULT 0,
          input_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
          output_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
          total_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
          usd_to_inr_rate NUMERIC(10,4),
          total_cost_inr NUMERIC(12,4) NOT NULL DEFAULT 0,
          is_billable BOOLEAN NOT NULL DEFAULT TRUE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS admin_ai_usage_admin_created_idx ON admin_ai_usage (admin_id, created_at DESC)`
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS admin_payment_links (
          id SERIAL PRIMARY KEY,
          admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
          payment_link_id VARCHAR(120) UNIQUE NOT NULL,
          amount NUMERIC(12,2) NOT NULL DEFAULT 0,
          currency VARCHAR(10) NOT NULL DEFAULT 'INR',
          purpose VARCHAR(30) NOT NULL DEFAULT 'payg',
          input_tokens INT NOT NULL DEFAULT 0,
          output_tokens INT NOT NULL DEFAULT 0,
          status VARCHAR(20) NOT NULL DEFAULT 'created'
            CHECK (status IN ('created', 'paid', 'failed', 'cancelled', 'expired', 'pending')),
          paid_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
          paid_at TIMESTAMPTZ,
          raw_json JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS purpose VARCHAR(30) NOT NULL DEFAULT 'payg'`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS base_amount NUMERIC(12,2) NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS maintenance_fee NUMERIC(12,2) NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS subscription_months INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS discount_pct NUMERIC(5,2) NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS dashboard_monthly_amount NUMERIC(12,2) NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS input_tokens INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `ALTER TABLE admin_payment_links ADD COLUMN IF NOT EXISTS output_tokens INT NOT NULL DEFAULT 0`
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS admin_payment_links_admin_idx ON admin_payment_links (admin_id, created_at DESC)`
    );
    await poolRef.query(
      `
        CREATE TABLE IF NOT EXISTS business_type_change_requests (
          id SERIAL PRIMARY KEY,
          admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
          current_business_type VARCHAR(20) NOT NULL
            CHECK (current_business_type IN ('product', 'service', 'both')),
          requested_business_type VARCHAR(20) NOT NULL
            CHECK (requested_business_type IN ('product', 'service', 'both')),
          reason TEXT,
          status VARCHAR(20) NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending', 'approved', 'rejected', 'cancelled')),
          monthly_current_inr NUMERIC(12,2),
          monthly_requested_inr NUMERIC(12,2),
          monthly_delta_inr NUMERIC(12,2),
          payment_required BOOLEAN NOT NULL DEFAULT FALSE,
          payment_status VARCHAR(20) NOT NULL DEFAULT 'unpaid'
            CHECK (payment_status IN ('unpaid', 'paid', 'waived')),
          payment_link_id VARCHAR(120),
          payment_link_url TEXT,
          payment_paid_amount NUMERIC(12,2),
          payment_paid_at TIMESTAMPTZ,
          resolved_by INT REFERENCES admins(id) ON DELETE SET NULL,
          resolved_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS business_type_change_requests_admin_idx ON business_type_change_requests (admin_id, created_at DESC)`
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS business_type_change_requests_status_idx ON business_type_change_requests (status, created_at DESC)`
    );
    await poolRef.query(
      `CREATE INDEX IF NOT EXISTS business_type_change_requests_payment_idx ON business_type_change_requests (payment_link_id)`
    );
    await poolRef.query(
      `
        INSERT INTO order_revenue (
          order_id,
          admin_id,
          channel,
          payment_currency,
          booked_amount,
          collected_amount,
          outstanding_amount,
          payment_status,
          payment_method,
          revenue_date,
          placed_at
        )
        SELECT
          o.id AS order_id,
          o.admin_id,
          COALESCE(NULLIF(btrim(o.channel), ''), 'WhatsApp') AS channel,
          COALESCE(NULLIF(btrim(o.payment_currency), ''), 'INR') AS payment_currency,
          GREATEST(COALESCE(o.payment_total, 0), 0) AS booked_amount,
          CASE
            WHEN o.payment_status = 'refunded' THEN 0
            ELSE LEAST(
              GREATEST(COALESCE(o.payment_paid, 0), 0),
              GREATEST(COALESCE(o.payment_total, 0), 0)
            )
          END AS collected_amount,
          GREATEST(
            GREATEST(COALESCE(o.payment_total, 0), 0) -
              CASE
                WHEN o.payment_status = 'refunded' THEN 0
                ELSE LEAST(
                  GREATEST(COALESCE(o.payment_paid, 0), 0),
                  GREATEST(COALESCE(o.payment_total, 0), 0)
                )
              END,
            0
          ) AS outstanding_amount,
          COALESCE(NULLIF(btrim(o.payment_status), ''), 'pending') AS payment_status,
          NULLIF(btrim(o.payment_method), '') AS payment_method,
          COALESCE(DATE(COALESCE(o.placed_at, o.created_at)), CURRENT_DATE) AS revenue_date,
          COALESCE(o.placed_at, o.created_at) AS placed_at
        FROM orders o
        ON CONFLICT (order_id) DO UPDATE
        SET
          admin_id = EXCLUDED.admin_id,
          channel = EXCLUDED.channel,
          payment_currency = EXCLUDED.payment_currency,
          booked_amount = EXCLUDED.booked_amount,
          collected_amount = EXCLUDED.collected_amount,
          outstanding_amount = EXCLUDED.outstanding_amount,
          payment_status = EXCLUDED.payment_status,
          payment_method = EXCLUDED.payment_method,
          revenue_date = EXCLUDED.revenue_date,
          placed_at = EXCLUDED.placed_at,
          updated_at = NOW()
      `
    );

    const adminProfessionColumn = await poolRef.query(
      `
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'admins'
          AND column_name = 'profession'
        LIMIT 1
      `
    );

    if (adminProfessionColumn.rows.length > 0) {
      await poolRef.query(
        `
          UPDATE admins
          SET business_type = CASE
            WHEN LOWER(COALESCE(profession, '')) IN ('clinic', 'salon', 'gym', 'spa', 'doctor', 'consultant') THEN 'service'
            WHEN LOWER(COALESCE(profession, '')) IN ('warehouse', 'inventory') THEN 'product'
            ELSE 'both'
          END
          WHERE business_type IS NULL OR btrim(business_type) = ''
        `
      );

      await poolRef.query(
        `
          UPDATE admins
          SET business_category = COALESCE(
            NULLIF(btrim(business_category), ''),
            NULLIF(btrim(whatsapp_name), ''),
            NULLIF(btrim(profession), ''),
            'General'
          )
          WHERE business_category IS NULL OR btrim(business_category) = ''
        `
      );
    } else {
      await poolRef.query(
        `
          UPDATE admins
          SET business_type = COALESCE(NULLIF(btrim(business_type), ''), 'both')
          WHERE business_type IS NULL OR btrim(business_type) = ''
        `
      );
      await poolRef.query(
        `
          UPDATE admins
          SET business_category = COALESCE(
            NULLIF(btrim(business_category), ''),
            NULLIF(btrim(whatsapp_name), ''),
            'General'
          )
          WHERE business_category IS NULL OR btrim(business_category) = ''
        `
      );
    }
    await poolRef.query(
      `
        UPDATE admins
        SET booking_enabled = FALSE
        WHERE booking_enabled IS NULL
      `
    );
    await poolRef.query(
      `
        UPDATE appointments
        SET appointment_kind = CASE
          WHEN LOWER(COALESCE(appointment_kind, '')) IN ('service', 'booking') THEN LOWER(appointment_kind)
          ELSE 'service'
        END
      `
    );

    await poolRef.query(`ALTER TABLE admins DROP COLUMN IF EXISTS profession_request`);
    await poolRef.query(`ALTER TABLE admins DROP COLUMN IF EXISTS profession_requested_at`);
    await poolRef.query(`ALTER TABLE admins DROP COLUMN IF EXISTS profession`);
    await poolRef.query(`ALTER TABLE appointments DROP COLUMN IF EXISTS profession`);
    await poolRef.query(
      `
        UPDATE admins
        SET
          appointment_start_hour = CASE
            WHEN appointment_start_hour BETWEEN 0 AND 23 THEN appointment_start_hour
            ELSE ${APPOINTMENT_SETTING_DEFAULTS.startHour}
          END,
          appointment_end_hour = CASE
            WHEN appointment_end_hour BETWEEN 1 AND 24 THEN appointment_end_hour
            ELSE ${APPOINTMENT_SETTING_DEFAULTS.endHour}
          END,
          appointment_slot_minutes = CASE
            WHEN appointment_slot_minutes BETWEEN 15 AND 240 THEN appointment_slot_minutes
            ELSE ${APPOINTMENT_SETTING_DEFAULTS.slotMinutes}
          END,
          appointment_window_months = CASE
            WHEN appointment_window_months BETWEEN 1 AND 24 THEN appointment_window_months
            ELSE ${APPOINTMENT_SETTING_DEFAULTS.windowMonths}
          END
      `
    );
    await poolRef.query(
      `
        UPDATE admins
        SET appointment_end_hour = LEAST(24, appointment_start_hour + 1)
        WHERE appointment_end_hour <= appointment_start_hour
      `
    );
  })().catch((error) => {
    adminBusinessColumnsInitStarted = false;
    adminBusinessColumnsReadyPromise = null;
    throw error;
  });

  return adminBusinessColumnsReadyPromise;
}

export async function initializeDbHelpers() {
  await ensureAdminBusinessColumns();
}

export async function getConnection() {
  if (!adminBusinessColumnsInitStarted) {
    void ensureAdminBusinessColumns().catch((error) => {
      console.warn(
        "⚠️ Database helper initialization will retry on next startup/request:",
        error?.message || error
      );
    });
  }
  const client = await getPool().connect();
  const query = async (text, params = []) => {
    const sql = formatQuery(text, params);
    const result = await client.query(sql, params);
    return [result.rows, result];
  };
  return {
    query,
    execute: query,
    release: () => client.release(),
  };
}

// Get all users with their admin info
export async function getAllUsers(adminId = null, { search = '', limit = 50, offset = 0 } = {}) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('u.assigned_admin_id = ?');
      params.push(adminId);
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push('(LOWER(u.name) LIKE ? OR u.phone LIKE ? OR LOWER(u.email) LIKE ?)');
      params.push(q, q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [users] = await connection.query(
      `
        SELECT u.*, a.name as admin_name
        FROM contacts u
        LEFT JOIN admins a ON u.assigned_admin_id = a.id
        ${whereClause}
        ORDER BY u.created_at DESC, u.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return users;
  } finally {
    connection.release();
  }
}

export async function countUsersSince(adminId = null, since = null) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('u.assigned_admin_id = ?');
      params.push(adminId);
    }
    if (since) {
      whereParts.push('u.created_at > ?');
      params.push(since);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        SELECT COUNT(*) as count
        FROM contacts u
        ${whereClause}
      `,
      params
    );
    return Number(rows?.[0]?.count || 0);
  } finally {
    connection.release();
  }
}

// Get user by ID
export async function getUserById(userId, adminId = null) {
  const connection = await getConnection();
  try {
    const params = [userId];
    let whereClause = 'WHERE u.id = ?';
    if (adminId) {
      whereClause += ' AND u.assigned_admin_id = ?';
      params.push(adminId);
    }
    const [user] = await connection.query(
      `
        SELECT u.*, a.name as admin_name
        FROM contacts u
        LEFT JOIN admins a ON u.assigned_admin_id = a.id
        ${whereClause}
      `,
      params
    );
    return user[0];
  } finally {
    connection.release();
  }
}

export async function updateUserAutomation(userId, automationDisabled, adminId = null) {
  const connection = await getConnection();
  try {
    const params = [Boolean(automationDisabled), userId];
    let whereClause = 'WHERE id = ?';
    if (adminId) {
      whereClause += ' AND assigned_admin_id = ?';
      params.push(adminId);
    }

    await connection.query(
      `
        UPDATE contacts
        SET automation_disabled = ?, updated_at = NOW()
        ${whereClause}
      `,
      params
    );

    return await getUserById(userId, adminId);
  } finally {
    connection.release();
  }
}

export async function getUserByPhone(phone, adminId = null) {
  const connection = await getConnection();
  try {
    const normalizedPhone = sanitizePhone(phone);
    if (!normalizedPhone) return null;
    const params = [normalizedPhone, normalizedPhone];
    const whereParts = ["(u.phone = ? OR regexp_replace(u.phone, '\\D', '', 'g') = ?)"];
    if (Number.isFinite(Number(adminId)) && Number(adminId) > 0) {
      whereParts.push('u.assigned_admin_id = ?');
      params.push(Number(adminId));
    }
    const whereClause = whereParts.join(' AND ');
    const [rows] = await connection.query(
      `
        SELECT u.*, a.name as admin_name
        FROM contacts u
        LEFT JOIN admins a ON u.assigned_admin_id = a.id
        WHERE ${whereClause}
        LIMIT 1
      `,
      params
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

// Get all messages with user and admin details
export async function getAllMessages(adminId = null, { search = '', limit = 50, offset = 0 } = {}) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('m.admin_id = ?');
      params.push(adminId);
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push('(LOWER(u.name) LIKE ? OR u.phone LIKE ? OR LOWER(m.message_text) LIKE ?)');
      params.push(q, q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [messages] = await connection.query(
      `
        SELECT m.*, u.name as user_name, u.phone, a.name as admin_name
        FROM messages m
        LEFT JOIN contacts u ON m.user_id = u.id
        LEFT JOIN admins a ON m.admin_id = a.id
        ${whereClause}
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return messages;
  } finally {
    connection.release();
  }
}

export async function deleteMessagesOlderThan(days = 15) {
  const connection = await getConnection();
  try {
    const safeDays = Number.isFinite(Number(days)) ? Number(days) : 15;
    const interval = `${safeDays} days`;
    await connection.query(
      `DELETE FROM messages WHERE created_at < NOW() - ($1::interval)`,
      [interval]
    );
  } finally {
    connection.release();
  }
}

// Get messages for a specific user
export async function getMessagesForUser(
  userId,
  adminId = null,
  { limit = 50, offset = 0, before = null } = {}
) {
  const connection = await getConnection();
  try {
    const params = [userId];
    const whereParts = ['m.user_id = ?'];
    if (adminId) {
      whereParts.push('m.admin_id = ?');
      params.push(adminId);
    }
    if (before) {
      whereParts.push('m.created_at < ?');
      params.push(before);
    }
    const whereClause = `WHERE ${whereParts.join(' AND ')}`;
    const [messages] = await connection.query(
      `
        SELECT m.*, u.name as user_name, a.name as admin_name
        FROM messages m
        LEFT JOIN contacts u ON m.user_id = u.id
        LEFT JOIN admins a ON m.admin_id = a.id
        ${whereClause}
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return messages;
  } finally {
    connection.release();
  }
}

export async function markMessagesRead(userId, adminId = null) {
  const connection = await getConnection();
  try {
    const params = [userId];
    const whereParts = ['user_id = ?', "message_type = 'incoming'", "status <> 'read'"];
    if (adminId) {
      whereParts.push('admin_id = ?');
      params.push(adminId);
    }
    const whereClause = `WHERE ${whereParts.join(' AND ')}`;
    const [, result] = await connection.query(
      `
        UPDATE messages
        SET status = 'read'
        ${whereClause}
      `,
      params
    );
    return Number(result?.rowCount || 0);
  } finally {
    connection.release();
  }
}

// Get all leads with user info
export async function getAllRequirements(
  adminId = null,
  { search = '', status = 'all', limit = 50, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('u.assigned_admin_id = ?');
      params.push(adminId);
    }
    if (status && status !== 'all') {
      whereParts.push('r.status = ?');
      params.push(status);
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push('(LOWER(u.name) LIKE ? OR LOWER(r.requirement_text) LIKE ?)');
      params.push(q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [leads] = await connection.query(
      `
        SELECT r.*, u.name, u.phone
        FROM leads r
        LEFT JOIN contacts u ON r.user_id = u.id
        ${whereClause}
        ORDER BY r.created_at DESC, r.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return leads;
  } finally {
    connection.release();
  }
}

export async function updateRequirementStatus(requirementId, status, adminId = null) {
  const connection = await getConnection();
  try {
    if (adminId) {
      await connection.query(
        `UPDATE leads r
         SET status = ?
         FROM contacts u
         WHERE r.user_id = u.id AND r.id = ? AND u.assigned_admin_id = ?`,
        [status, requirementId, adminId]
      );
    } else {
      await connection.query(
        `UPDATE leads SET status = ? WHERE id = ?`,
        [status, requirementId]
      );
    }

    const params = [requirementId];
    let whereClause = 'WHERE r.id = ?';
    if (adminId) {
      whereClause += ' AND u.assigned_admin_id = ?';
      params.push(adminId);
    }
    const [rows] = await connection.query(
      `SELECT r.*, u.name, u.phone
       FROM leads r
       LEFT JOIN contacts u ON r.user_id = u.id
       ${whereClause}
       LIMIT 1`,
      params
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function getAppointments(
  adminId = null,
  { search = '', status = 'all', kind = 'all', limit = 50, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('a.admin_id = ?');
      params.push(adminId);
    }
    if (status && status !== 'all') {
      whereParts.push('a.status = ?');
      params.push(status);
    }
    if (kind && kind !== 'all') {
      whereParts.push('COALESCE(a.appointment_kind, \'service\') = ?');
      params.push(normalizeAppointmentKind(kind));
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push(
        '(LOWER(u.name) LIKE ? OR u.phone LIKE ? OR LOWER(a.appointment_type) LIKE ? OR LOWER(COALESCE(a.appointment_kind, \'service\')) LIKE ?)'
      );
      params.push(q, q, q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        ${appointmentSelectWithPayments}
        ${whereClause}
        ORDER BY a.start_time DESC, a.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return rows;
  } finally {
    connection.release();
  }
}

export async function getAppointmentsForUser(
  userId,
  adminId = null,
  { status = 'all', kind = 'all', limit = 10, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [userId];
    const whereParts = ['a.user_id = ?'];
    if (adminId) {
      whereParts.push('a.admin_id = ?');
      params.push(adminId);
    }
    if (status && status !== 'all') {
      whereParts.push('a.status = ?');
      params.push(status);
    }
    if (kind && kind !== 'all') {
      whereParts.push('COALESCE(a.appointment_kind, \'service\') = ?');
      params.push(normalizeAppointmentKind(kind));
    }
    const whereClause = `WHERE ${whereParts.join(' AND ')}`;
    const [rows] = await connection.query(
      `
        ${appointmentSelectWithPayments}
        ${whereClause}
        ORDER BY a.start_time DESC, a.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return rows;
  } finally {
    connection.release();
  }
}

const normalizeAmount = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const normalizeAppointmentKind = (value, fallback = 'service') => {
  const normalized = String(value || '').trim().toLowerCase();
  if (ALLOWED_APPOINTMENT_KINDS.has(normalized)) return normalized;
  return fallback;
};

const appointmentSelectWithPayments = `
  SELECT
    a.*,
    u.name as user_name,
    u.phone,
    u.email,
    a.payment_total,
    COALESCE(a.payment_paid, 0) as payment_paid,
    GREATEST(COALESCE(a.payment_total, 0) - COALESCE(a.payment_paid, 0), 0) as payment_due,
    CASE
      WHEN a.payment_total IS NULL OR a.payment_total <= 0 THEN 'unpaid'
      WHEN COALESCE(a.payment_paid, 0) <= 0 THEN 'unpaid'
      WHEN COALESCE(a.payment_paid, 0) < a.payment_total THEN 'partial'
      ELSE 'paid'
    END as payment_status,
    a.payment_method,
    a.payment_notes
  FROM appointments a
  LEFT JOIN contacts u ON a.user_id = u.id
`;

export async function updateAppointment(appointmentId, updates = {}, adminId = null) {
  const connection = await getConnection();
  try {
    const fields = [];
    const params = [];

    if (Object.prototype.hasOwnProperty.call(updates, 'status')) {
      fields.push('status = ?');
      params.push(updates.status);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'appointment_type')) {
      fields.push('appointment_type = ?');
      params.push(updates.appointment_type || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'appointment_kind')) {
      fields.push('appointment_kind = ?');
      params.push(normalizeAppointmentKind(updates.appointment_kind));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'start_time')) {
      fields.push('start_time = ?');
      params.push(updates.start_time);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'end_time')) {
      fields.push('end_time = ?');
      params.push(updates.end_time);
    }

    if (Object.prototype.hasOwnProperty.call(updates, 'payment_total')) {
      fields.push('payment_total = ?');
      params.push(normalizeAmount(updates.payment_total));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_paid')) {
      fields.push('payment_paid = ?');
      params.push(normalizeAmount(updates.payment_paid));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_method')) {
      fields.push('payment_method = ?');
      params.push(updates.payment_method || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_notes')) {
      fields.push('payment_notes = ?');
      params.push(updates.payment_notes || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_currency')) {
      fields.push('payment_currency = ?');
      params.push(updates.payment_currency || null);
    }

    if (fields.length === 0) {
      return null;
    }

    fields.push('updated_at = NOW()');
    params.push(appointmentId);
    if (adminId) {
      params.push(adminId);
    }

    if (adminId) {
      await connection.query(
        `UPDATE appointments a
         SET ${fields.join(', ')}
         FROM contacts u
         WHERE a.user_id = u.id AND a.id = ? AND a.admin_id = ?`,
        params
      );
    } else {
      await connection.query(
        `UPDATE appointments
         SET ${fields.join(', ')}
         WHERE id = ?`,
        params
      );
    }

    const fetchParams = [appointmentId];
    let whereClause = 'WHERE a.id = ?';
    if (adminId) {
      whereClause += ' AND a.admin_id = ?';
      fetchParams.push(adminId);
    }
    const [rows] = await connection.query(
      `
        ${appointmentSelectWithPayments}
        ${whereClause}
        LIMIT 1
      `,
      fetchParams
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function updateAppointmentStatus(appointmentId, status, adminId = null) {
  return updateAppointment(appointmentId, { status }, adminId);
}

export async function deleteAppointment(appointmentId, adminId = null) {
  const normalizedAppointmentId = Number(appointmentId);
  if (!Number.isFinite(normalizedAppointmentId) || normalizedAppointmentId <= 0) {
    return { success: false, error: 'Invalid appointment ID' };
  }

  const connection = await getConnection();
  try {
    const params = [normalizedAppointmentId];
    let whereClause = 'WHERE id = ?';
    if (adminId) {
      whereClause += ' AND admin_id = ?';
      params.push(adminId);
    }

    const [existingRows] = await connection.query(
      `SELECT id FROM appointments ${whereClause} LIMIT 1`,
      params
    );

    if (!existingRows || existingRows.length === 0) {
      return { success: false, error: 'Appointment not found' };
    }

    await connection.query(`DELETE FROM appointments ${whereClause}`, params);

    return { success: true };
  } catch (error) {
    console.error('Error deleting appointment:', error);
    return { success: false, error: error.message || 'Failed to delete appointment' };
  } finally {
    connection.release();
  }
}

export async function createAppointment(
  {
    user_id,
    admin_id,
    appointment_type,
    appointment_kind,
    start_time,
    end_time,
    status = 'booked',
    payment_total,
    payment_paid,
    payment_method,
    payment_notes,
  } = {}
) {
  const connection = await getConnection();
  try {
    const normalizedTotal = normalizeAmount(payment_total);
    const normalizedPaid = normalizeAmount(payment_paid);

    const [rows] = await connection.query(
      `
        INSERT INTO appointments
          (user_id, admin_id, appointment_type, appointment_kind, start_time, end_time, status, payment_total, payment_paid, payment_method, payment_notes, payment_currency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
      `,
      [
        user_id,
        admin_id,
        appointment_type || null,
        normalizeAppointmentKind(appointment_kind),
        start_time,
        end_time,
        status,
        normalizedTotal,
        normalizedPaid,
        payment_method || null,
        payment_notes || null,
        null,
      ]
    );
    const appointmentId = rows?.[0]?.id;
    if (!appointmentId) return null;

    const [created] = await connection.query(
      `
        ${appointmentSelectWithPayments}
        WHERE a.id = ?
        LIMIT 1
      `,
      [appointmentId]
    );
    return created[0] || null;
  } finally {
    connection.release();
  }
}

const orderSelectWithPayments = `
  SELECT
    o.*,
    o.payment_total as total_amount,
    o.payment_total as payment_total,
    COALESCE(o.payment_paid, 0) as payment_paid,
    GREATEST(COALESCE(o.payment_total, 0) - COALESCE(o.payment_paid, 0), 0) as payment_due,
    CASE
      WHEN o.payment_status IN ('failed', 'refunded') THEN o.payment_status
      WHEN o.payment_total IS NULL OR o.payment_total <= 0 THEN 'pending'
      WHEN COALESCE(o.payment_paid, 0) >= o.payment_total THEN 'paid'
      ELSE 'pending'
    END as payment_status,
    o.payment_method as payment_method,
    o.payment_notes as payment_notes,
    o.payment_transaction_id as payment_transaction_id,
    o.payment_gateway_payment_id as payment_gateway_payment_id,
    o.payment_link_id as payment_link_id
  FROM orders o
`;

const normalizeRevenueText = (value, fallback = '') => {
  const raw = String(value || '').trim();
  return raw || fallback;
};

const buildOrderRevenuePayload = (order = {}) => {
  const booked = toAmountValue(Math.max(Number(order?.payment_total || 0), 0));
  const paymentStatus = normalizeRevenueText(order?.payment_status, 'pending').toLowerCase();
  const rawCollected = toAmountValue(Math.max(Number(order?.payment_paid || 0), 0));
  const collected = paymentStatus === 'refunded'
    ? 0
    : toAmountValue(Math.min(rawCollected, booked));
  const outstanding = toAmountValue(Math.max(booked - collected, 0));
  const placedAt = order?.placed_at || order?.created_at || null;
  const dateValue = placedAt ? new Date(placedAt) : new Date();
  const revenueDate =
    Number.isNaN(dateValue.getTime()) ? new Date().toISOString().slice(0, 10) : toUtcDateKey(dateValue);
  return {
    orderId: Number(order?.id),
    adminId: Number(order?.admin_id),
    channel: normalizeRevenueText(order?.channel, 'WhatsApp'),
    paymentCurrency: normalizeRevenueText(order?.payment_currency, 'INR').toUpperCase(),
    paymentStatus:
      ['pending', 'paid', 'failed', 'refunded'].includes(paymentStatus) ? paymentStatus : 'pending',
    paymentMethod: normalizeRevenueText(order?.payment_method, '') || null,
    bookedAmount: booked,
    collectedAmount: collected,
    outstandingAmount: outstanding,
    revenueDate,
    placedAt: placedAt || null,
  };
};

const upsertOrderRevenueRecord = async (connection, order = {}) => {
  const payload = buildOrderRevenuePayload(order);
  if (!Number.isFinite(payload.orderId) || payload.orderId <= 0) return false;
  if (!Number.isFinite(payload.adminId) || payload.adminId <= 0) return false;
  await connection.query(
    `
      INSERT INTO order_revenue (
        order_id,
        admin_id,
        channel,
        payment_currency,
        booked_amount,
        collected_amount,
        outstanding_amount,
        payment_status,
        payment_method,
        revenue_date,
        placed_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (order_id) DO UPDATE
      SET
        admin_id = EXCLUDED.admin_id,
        channel = EXCLUDED.channel,
        payment_currency = EXCLUDED.payment_currency,
        booked_amount = EXCLUDED.booked_amount,
        collected_amount = EXCLUDED.collected_amount,
        outstanding_amount = EXCLUDED.outstanding_amount,
        payment_status = EXCLUDED.payment_status,
        payment_method = EXCLUDED.payment_method,
        revenue_date = EXCLUDED.revenue_date,
        placed_at = EXCLUDED.placed_at,
        updated_at = NOW()
    `,
    [
      payload.orderId,
      payload.adminId,
      payload.channel,
      payload.paymentCurrency,
      payload.bookedAmount,
      payload.collectedAmount,
      payload.outstandingAmount,
      payload.paymentStatus,
      payload.paymentMethod,
      payload.revenueDate,
      payload.placedAt,
    ]
  );
  return true;
};

export async function syncOrderRevenueByOrderId(orderId, adminId = null) {
  const normalizedOrderId = Number(orderId);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) return false;

  const connection = await getConnection();
  try {
    const params = [normalizedOrderId];
    let whereClause = 'WHERE o.id = ?';
    if (Number.isFinite(Number(adminId)) && Number(adminId) > 0) {
      whereClause += ' AND o.admin_id = ?';
      params.push(Number(adminId));
    }
    const [rows] = await connection.query(
      `
        ${orderSelectWithPayments}
        ${whereClause}
        LIMIT 1
      `,
      params
    );
    const order = rows?.[0];
    if (!order) return false;
    return await upsertOrderRevenueRecord(connection, order);
  } finally {
    connection.release();
  }
};

export async function getOrders(
  adminId = null,
  { limit = 200, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('o.admin_id = ?');
      params.push(adminId);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        ${orderSelectWithPayments}
        ${whereClause}
        ORDER BY COALESCE(o.placed_at, o.created_at) DESC, o.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return rows;
  } finally {
    connection.release();
  }
}

export async function getOrderById(orderId, adminId = null) {
  const normalizedOrderId = Number(orderId);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) return null;
  const connection = await getConnection();
  try {
    const params = [normalizedOrderId];
    let whereClause = 'WHERE o.id = ?';
    if (adminId) {
      whereClause += ' AND o.admin_id = ?';
      params.push(adminId);
    }
    const [rows] = await connection.query(
      `
        ${orderSelectWithPayments}
        ${whereClause}
        LIMIT 1
      `,
      params
    );
    return rows?.[0] || null;
  } finally {
    connection.release();
  }
}

export async function createOrder(payload = {}) {
  const connection = await getConnection();
  try {
    const adminId = Number(payload?.admin_id);
    if (!Number.isFinite(adminId) || adminId <= 0) return null;

    const orderNumber = toTrimmedString(payload?.order_number) || null;
    const customerName = toTrimmedString(payload?.customer_name) || null;
    const customerPhone = toTrimmedString(payload?.customer_phone) || null;
    const customerEmail = toTrimmedString(payload?.customer_email) || null;

    const normalizeChannel = (value, fallback = 'Manual') => {
      const raw = toTrimmedString(value);
      if (!raw) return fallback;
      const normalized = raw.toLowerCase();
      if (normalized === 'whatsapp') return 'WhatsApp';
      if (normalized === 'instagram') return 'Instagram';
      if (normalized === 'website') return 'Website';
      if (normalized === 'manual') return 'Manual';
      return raw;
    };

    const channel = normalizeChannel(payload?.channel);
    const status = toTrimmedString(payload?.status) || 'new';
    const fulfillmentStatus = toTrimmedString(payload?.fulfillment_status) || 'unfulfilled';

    const rawItems = Array.isArray(payload?.items) ? payload.items : [];
    const items = rawItems
      .map((item) => ({
        name: toTrimmedString(item?.name),
        quantity: Math.max(1, Math.round(Number(item?.quantity || 1))),
        price: Math.max(0, Number(item?.price || 0)),
      }))
      .filter((item) => item.name);

    const notes = Array.isArray(payload?.notes) ? payload.notes : [];
    const itemsTotal = items.reduce(
      (sum, item) => sum + Number(item.price || 0) * Number(item.quantity || 0),
      0
    );

    const paymentTotal = normalizeAmount(payload?.payment_total);
    const paymentPaid = normalizeAmount(payload?.payment_paid);
    const normalizedPaymentTotal = paymentTotal !== null ? paymentTotal : itemsTotal || null;
    const normalizedPaymentPaid = paymentPaid !== null ? paymentPaid : 0;
    const rawPaymentStatus = toTrimmedString(payload?.payment_status || '').toLowerCase();
    const normalizedPaymentStatus = ['pending', 'paid', 'failed', 'refunded'].includes(rawPaymentStatus)
      ? rawPaymentStatus
      : normalizedPaymentTotal && normalizedPaymentPaid >= normalizedPaymentTotal
      ? 'paid'
      : 'pending';
    const paymentMethod = toTrimmedString(payload?.payment_method) || null;
    const paymentCurrency = toTrimmedString(payload?.payment_currency) || 'INR';
    const paymentNotes = toTrimmedString(payload?.payment_notes) || null;
    const paymentTransactionId = toTrimmedString(payload?.payment_transaction_id) || null;
    const paymentGatewayPaymentId = toTrimmedString(payload?.payment_gateway_payment_id) || null;
    const paymentLinkId = toTrimmedString(payload?.payment_link_id) || null;

    const deliveryMethod = toTrimmedString(payload?.delivery_method) || null;
    const deliveryAddress = toTrimmedString(payload?.delivery_address) || null;

    const placedAtRaw = payload?.placed_at ? new Date(payload.placed_at) : new Date();
    const placedAt = Number.isNaN(placedAtRaw.getTime()) ? new Date() : placedAtRaw;

    const [rows] = await connection.query(
      `
        INSERT INTO orders (
          admin_id,
          order_number,
          customer_name,
          customer_phone,
          customer_email,
          channel,
          status,
          fulfillment_status,
          delivery_method,
          delivery_address,
          items,
          notes,
          placed_at,
          payment_total,
          payment_paid,
          payment_status,
          payment_method,
          payment_currency,
          payment_notes,
          payment_transaction_id,
          payment_gateway_payment_id,
          payment_link_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
      `,
      [
        adminId,
        orderNumber,
        customerName,
        customerPhone,
        customerEmail,
        channel,
        status,
        fulfillmentStatus,
        deliveryMethod,
        deliveryAddress,
        JSON.stringify(items),
        JSON.stringify(notes),
        placedAt.toISOString(),
        normalizedPaymentTotal,
        normalizedPaymentPaid,
        normalizedPaymentStatus,
        paymentMethod,
        paymentCurrency,
        paymentNotes,
        paymentTransactionId,
        paymentGatewayPaymentId,
        paymentLinkId,
      ]
    );

    const createdId = rows?.[0]?.id;
    if (!createdId) return null;

    const [createdRows] = await connection.query(
      `
        ${orderSelectWithPayments}
        WHERE o.id = ?
        LIMIT 1
      `,
      [createdId]
    );
    const created = createdRows?.[0] || null;
    if (created) {
      await upsertOrderRevenueRecord(connection, created);
    }
    return created;
  } finally {
    connection.release();
  }
}

export async function updateOrder(orderId, updates = {}, adminId = null) {
  const connection = await getConnection();
  try {
    const fields = [];
    const params = [];

    if (Object.prototype.hasOwnProperty.call(updates, 'status')) {
      fields.push('status = ?');
      params.push(updates.status);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'fulfillment_status')) {
      fields.push('fulfillment_status = ?');
      params.push(updates.fulfillment_status);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'assigned_to')) {
      fields.push('assigned_to = ?');
      params.push(updates.assigned_to || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'notes')) {
      fields.push('notes = ?');
      params.push(
        Array.isArray(updates.notes) || updates.notes === null
          ? updates.notes
          : null
      );
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_total')) {
      fields.push('payment_total = ?');
      params.push(normalizeAmount(updates.payment_total));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_paid')) {
      fields.push('payment_paid = ?');
      params.push(normalizeAmount(updates.payment_paid));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_method')) {
      fields.push('payment_method = ?');
      params.push(updates.payment_method || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_status')) {
      fields.push('payment_status = ?');
      params.push(updates.payment_status || 'pending');
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_notes')) {
      fields.push('payment_notes = ?');
      params.push(updates.payment_notes || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_currency')) {
      fields.push('payment_currency = ?');
      params.push(updates.payment_currency || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_transaction_id')) {
      fields.push('payment_transaction_id = ?');
      params.push(updates.payment_transaction_id || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_gateway_payment_id')) {
      fields.push('payment_gateway_payment_id = ?');
      params.push(updates.payment_gateway_payment_id || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_link_id')) {
      fields.push('payment_link_id = ?');
      params.push(updates.payment_link_id || null);
    }

    if (fields.length === 0) {
      return null;
    }

    fields.push('updated_at = NOW()');
    params.push(orderId);
    if (adminId) {
      params.push(adminId);
    }

    if (adminId) {
      await connection.query(
        `UPDATE orders o
         SET ${fields.join(', ')}
         WHERE o.id = ? AND o.admin_id = ?`,
        params
      );
    } else {
      await connection.query(
        `UPDATE orders
         SET ${fields.join(', ')}
         WHERE id = ?`,
        params
      );
    }

    const fetchParams = [orderId];
    let whereClause = 'WHERE o.id = ?';
    if (adminId) {
      whereClause += ' AND o.admin_id = ?';
      fetchParams.push(adminId);
    }
    const [rows] = await connection.query(
      `
        ${orderSelectWithPayments}
        ${whereClause}
        LIMIT 1
      `,
      fetchParams
    );
    const updated = rows[0] || null;
    if (updated) {
      await upsertOrderRevenueRecord(connection, updated);
    }
    return updated;
  } finally {
    connection.release();
  }
}

export async function deleteOrder(orderId, adminId = null) {
  const normalizedOrderId = Number(orderId);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) {
    return { success: false, error: 'Invalid order ID' };
  }

  const connection = await getConnection();
  try {
    // First, get the order to verify it exists and belongs to the admin
    const params = [normalizedOrderId];
    let whereClause = 'WHERE id = ?';
    if (adminId) {
      whereClause += ' AND admin_id = ?';
      params.push(adminId);
    }

    const [existingRows] = await connection.query(
      `SELECT id FROM orders ${whereClause} LIMIT 1`,
      params
    );

    if (!existingRows || existingRows.length === 0) {
      return { success: false, error: 'Order not found' };
    }

    // Delete the order (CASCADE will handle related records like order_revenue, order_payment_link_timers)
    await connection.query(
      `DELETE FROM orders ${whereClause}`,
      params
    );

    return { success: true };
  } catch (error) {
    console.error('Error deleting order:', error);
    return { success: false, error: error.message || 'Failed to delete order' };
  } finally {
    connection.release();
  }
}

export async function scheduleOrderPaymentLinkTimer({
  orderId,
  adminId,
  scheduledFor,
  createdBy = null,
  maxAttempts = 3,
  payload = null,
} = {}) {
  const normalizedOrderId = Number(orderId);
  const normalizedAdminId = Number(adminId);
  const scheduledDate = scheduledFor instanceof Date ? scheduledFor : new Date(scheduledFor);
  const normalizedMaxAttempts = Math.max(1, Number(maxAttempts) || 3);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) return null;
  if (!Number.isFinite(normalizedAdminId) || normalizedAdminId <= 0) return null;
  if (Number.isNaN(scheduledDate.getTime())) return null;

  const payloadJson =
    payload && typeof payload === 'object' ? JSON.stringify(payload) : null;
  const normalizedCreatedBy =
    Number.isFinite(Number(createdBy)) && Number(createdBy) > 0 ? Number(createdBy) : null;

  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        INSERT INTO order_payment_link_timers (
          order_id,
          admin_id,
          scheduled_for,
          status,
          attempts,
          max_attempts,
          last_error,
          payload_json,
          created_by,
          processing_started_at,
          sent_at
        )
        VALUES (?, ?, ?, 'scheduled', 0, ?, NULL, ?::jsonb, ?, NULL, NULL)
        ON CONFLICT (order_id) DO UPDATE
        SET
          admin_id = EXCLUDED.admin_id,
          scheduled_for = EXCLUDED.scheduled_for,
          status = 'scheduled',
          attempts = 0,
          max_attempts = EXCLUDED.max_attempts,
          last_error = NULL,
          payload_json = EXCLUDED.payload_json,
          created_by = EXCLUDED.created_by,
          processing_started_at = NULL,
          sent_at = NULL,
          updated_at = NOW()
        RETURNING *
      `,
      [
        normalizedOrderId,
        normalizedAdminId,
        scheduledDate.toISOString(),
        normalizedMaxAttempts,
        payloadJson,
        normalizedCreatedBy,
      ]
    );
    return rows?.[0] || null;
  } finally {
    connection.release();
  }
}

export async function getOrderPaymentLinkTimer(orderId, adminId = null) {
  const normalizedOrderId = Number(orderId);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) return null;
  const normalizedAdminId =
    Number.isFinite(Number(adminId)) && Number(adminId) > 0 ? Number(adminId) : null;
  const connection = await getConnection();
  try {
    const params = [normalizedOrderId];
    let whereClause = 'WHERE t.order_id = ?';
    if (normalizedAdminId) {
      whereClause += ' AND t.admin_id = ?';
      params.push(normalizedAdminId);
    }
    const [rows] = await connection.query(
      `
        SELECT t.*
        FROM order_payment_link_timers t
        ${whereClause}
        LIMIT 1
      `,
      params
    );
    return rows?.[0] || null;
  } finally {
    connection.release();
  }
}

export async function claimDueOrderPaymentLinkTimers(limit = 10) {
  const connection = await getConnection();
  try {
    const safeLimit = Math.min(Math.max(Number(limit) || 10, 1), 100);
    const [rows] = await connection.query(
      `
        WITH due AS (
          SELECT t.order_id
          FROM order_payment_link_timers t
          WHERE t.status = 'scheduled'
            AND t.scheduled_for <= NOW()
          ORDER BY t.scheduled_for ASC
          LIMIT ?
          FOR UPDATE SKIP LOCKED
        )
        UPDATE order_payment_link_timers t
        SET
          status = 'processing',
          attempts = t.attempts + 1,
          processing_started_at = NOW(),
          updated_at = NOW()
        FROM due
        WHERE t.order_id = due.order_id
        RETURNING t.*
      `,
      [safeLimit]
    );
    return rows || [];
  } finally {
    connection.release();
  }
}

export async function completeOrderPaymentLinkTimer(orderId, { paymentLinkId = '' } = {}) {
  const normalizedOrderId = Number(orderId);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) return null;
  const normalizedLinkId = String(paymentLinkId || '').trim() || null;
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        UPDATE order_payment_link_timers
        SET
          status = 'sent',
          sent_at = NOW(),
          last_error = NULL,
          last_payment_link_id = COALESCE(?, last_payment_link_id),
          processing_started_at = NULL,
          updated_at = NOW()
        WHERE order_id = ?
        RETURNING *
      `,
      [normalizedLinkId, normalizedOrderId]
    );
    return rows?.[0] || null;
  } finally {
    connection.release();
  }
}

export async function failOrderPaymentLinkTimer(
  orderId,
  errorMessage,
  { retryDelayMinutes = 10 } = {}
) {
  const normalizedOrderId = Number(orderId);
  if (!Number.isFinite(normalizedOrderId) || normalizedOrderId <= 0) return null;
  const safeRetryMinutes = Math.min(Math.max(Number(retryDelayMinutes) || 10, 1), 1440);
  const retryInterval = `${safeRetryMinutes} minutes`;
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        UPDATE order_payment_link_timers
        SET
          status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'scheduled' END,
          scheduled_for = CASE
            WHEN attempts >= max_attempts THEN scheduled_for
            ELSE NOW() + (?::interval)
          END,
          last_error = ?,
          processing_started_at = NULL,
          updated_at = NOW()
        WHERE order_id = ?
        RETURNING *
      `,
      [retryInterval, String(errorMessage || 'Unknown timer failure').slice(0, 1200), normalizedOrderId]
    );
    return rows?.[0] || null;
  } finally {
    connection.release();
  }
}

export async function countOrdersSince(adminId = null, since = null) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('o.admin_id = ?');
      params.push(adminId);
    }
    if (since) {
      whereParts.push('COALESCE(o.placed_at, o.created_at) > ?');
      params.push(since);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        SELECT COUNT(*) as count
        FROM orders o
        ${whereClause}
      `,
      params
    );
    return Number(rows?.[0]?.count || 0);
  } finally {
    connection.release();
  }
}

// Get all needs with user and admin info
export async function getAllNeeds(
  adminId = null,
  { search = '', status = 'all', limit = 50, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('u.assigned_admin_id = ?');
      params.push(adminId);
    }
    if (status && status !== 'all') {
      whereParts.push('n.status = ?');
      params.push(status);
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push('(LOWER(u.name) LIKE ? OR LOWER(n.need_text) LIKE ?)');
      params.push(q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [needs] = await connection.query(
      `
        SELECT n.*, u.name, u.phone, a.name as assigned_admin_name
        FROM tasks n
        LEFT JOIN contacts u ON n.user_id = u.id
        LEFT JOIN admins a ON n.assigned_to = a.id
        ${whereClause}
        ORDER BY n.created_at DESC, n.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return needs;
  } finally {
    connection.release();
  }
}

// Add new user
export async function addUser(phone, name, email, assigned_admin_id) {
  const connection = await getConnection();
  try {
    const normalizedPhone = sanitizePhone(phone);
    if (!normalizedPhone) {
      throw new Error('Invalid phone number');
    }
    const normalizedName = sanitizeNameUpper(name);
    const normalizedEmail = sanitizeEmail(email);
    const [rows] = await connection.query(
      `
        INSERT INTO contacts (phone, name, email, assigned_admin_id)
        VALUES (?, ?, ?, ?)
        RETURNING id
      `,
      [normalizedPhone, normalizedName, normalizedEmail, assigned_admin_id]
    );
    return rows[0]?.id || null;
  } finally {
    connection.release();
  }
}

// Add new message
export async function addMessage(user_id, admin_id, message_text, message_type) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        INSERT INTO messages (user_id, admin_id, message_text, message_type, status)
        VALUES (?, ?, ?, ?, 'sent')
        RETURNING id
      `,
      [user_id, admin_id, message_text, message_type]
    );
    return rows[0]?.id || null;
  } finally {
    connection.release();
  }
}

// Get dashboard stats
const DASHBOARD_LEAD_STATUS_ORDER = [
  { key: 'pending', label: 'Pending' },
  { key: 'in_progress', label: 'In Progress' },
  { key: 'completed', label: 'Completed' },
];

async function buildDashboardLeadStatusBreakdown(connection, adminId = null) {
  const baseCounts = DASHBOARD_LEAD_STATUS_ORDER.reduce((accumulator, item) => {
    accumulator[item.key] = 0;
    return accumulator;
  }, {});

  const [rows] = adminId
    ? await connection.query(
        `
          SELECT r.status, COUNT(*) AS count
          FROM leads r
          JOIN contacts u ON r.user_id = u.id
          WHERE u.assigned_admin_id = ?
          GROUP BY r.status
        `,
        [adminId]
      )
    : await connection.query(
        `
          SELECT status, COUNT(*) AS count
          FROM leads
          GROUP BY status
        `
      );

  for (const row of rows || []) {
    const key = String(row?.status || '').trim().toLowerCase();
    if (!Object.prototype.hasOwnProperty.call(baseCounts, key)) continue;
    baseCounts[key] = Number(row?.count || 0);
  }

  return DASHBOARD_LEAD_STATUS_ORDER.map((item) => ({
    status: item.key,
    label: item.label,
    value: baseCounts[item.key] || 0,
  }));
}

export async function getDashboardStats(adminId = null) {
  const connection = await getConnection();
  try {
    if (!adminId) {
      const [stats] = await connection.query(`
        SELECT 
          (SELECT COUNT(*) FROM contacts) as total_users,
          (SELECT COUNT(*) FROM messages WHERE message_type = 'incoming') as incoming_messages,
          (SELECT COUNT(*) FROM leads WHERE status = 'in_progress') as active_requirements,
          (SELECT COUNT(*) FROM tasks WHERE status = 'open') as open_needs,
          (SELECT COUNT(*) FROM orders) as total_orders,
          (SELECT COUNT(*) FROM appointments) as total_appointments,
          (SELECT COUNT(*)
           FROM order_revenue
           WHERE LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp') as whatsapp_orders,
          (SELECT COUNT(*)
           FROM order_revenue
           WHERE LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp'
             AND COALESCE(collected_amount, 0) > 0
             AND payment_status <> 'refunded') as whatsapp_paid_orders,
          (SELECT COALESCE(
             SUM(
               CASE
                 WHEN LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp'
                 THEN GREATEST(COALESCE(booked_amount, 0), 0)
                 ELSE 0
               END
             ),
             0
           )
           FROM order_revenue) as whatsapp_revenue_booked,
          (SELECT COALESCE(
             SUM(
               CASE
                 WHEN LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp'
                  AND payment_status <> 'refunded'
                 THEN GREATEST(COALESCE(collected_amount, 0), 0)
                 ELSE 0
               END
             ),
             0
           )
           FROM order_revenue) as whatsapp_revenue_paid
      `);
      const data = stats[0] || {};
      const [aiSpendRows] = await connection.query(
        `
          SELECT COALESCE(SUM(total_cost_inr), 0) AS ai_spend_inr
          FROM admin_ai_usage
          WHERE is_billable = TRUE
        `
      );
      const needsRevenueFallback =
        Number(data?.total_orders || 0) > 0 &&
        Number(data?.whatsapp_orders || 0) === 0 &&
        toAmountValue(data?.whatsapp_revenue_booked || 0) === 0 &&
        toAmountValue(data?.whatsapp_revenue_paid || 0) === 0;
      if (needsRevenueFallback) {
        const fallback = await getDashboardOrderRevenueSnapshotFromOrders(connection, null);
        data.whatsapp_orders = fallback.whatsapp_orders;
        data.whatsapp_paid_orders = fallback.whatsapp_paid_orders;
        data.whatsapp_revenue_booked = fallback.whatsapp_revenue_booked;
        data.whatsapp_revenue_paid = fallback.whatsapp_revenue_paid;
      }
      const subscriptionSnapshot = await getDashboardSubscriptionRevenueSnapshot(connection, null);
      data.growth_trend = await buildDashboardGrowthTrend(connection, null);
      data.lead_status_breakdown = await buildDashboardLeadStatusBreakdown(connection, null);
      const orderTrend = needsRevenueFallback
        ? await buildDashboardRevenueTrendFromOrders(connection, null)
        : await buildDashboardRevenueTrend(connection, null);
      const subscriptionTrend = await buildDashboardSubscriptionTrend(connection, null);
      data.revenue_trend = orderTrend.map((point, index) => {
        const subPoint = subscriptionTrend[index] || { earned: 0, booked: 0 };
        return {
          ...point,
          earned: toAmountValue(point.earned + subPoint.earned),
          booked: toAmountValue(point.booked + subPoint.booked),
        };
      });
      data.revenue_analysis = buildDashboardRevenueAnalysis(data.revenue_trend);
      data.subscription_revenue_paid = toAmountValue(subscriptionSnapshot.paid);
      data.subscription_revenue_booked = toAmountValue(subscriptionSnapshot.booked);
      data.subscription_revenue_outstanding = toAmountValue(subscriptionSnapshot.outstanding);
      data.whatsapp_revenue_paid = toAmountValue(data.whatsapp_revenue_paid);
      data.whatsapp_revenue_booked = toAmountValue(data.whatsapp_revenue_booked);
      data.whatsapp_revenue_outstanding = toAmountValue(
        Math.max(data.whatsapp_revenue_booked - data.whatsapp_revenue_paid, 0)
      );
      data.total_revenue_paid = toAmountValue(
        data.whatsapp_revenue_paid + data.subscription_revenue_paid
      );
      data.total_revenue_booked = toAmountValue(
        data.whatsapp_revenue_booked + data.subscription_revenue_booked
      );
      data.total_revenue_outstanding = toAmountValue(
        Math.max(data.total_revenue_booked - data.total_revenue_paid, 0)
      );
      data.ai_spend_inr = toAmountValue(aiSpendRows?.[0]?.ai_spend_inr || 0);
      return data;
    }

    const [stats] = await connection.query(
      `
        SELECT
          (SELECT COUNT(*) FROM contacts WHERE assigned_admin_id = ?) as total_users,
          (SELECT COUNT(*) FROM messages WHERE message_type = 'incoming' AND admin_id = ?) as incoming_messages,
          (SELECT COUNT(*)
           FROM leads r
           JOIN contacts u ON r.user_id = u.id
           WHERE r.status = 'in_progress' AND u.assigned_admin_id = ?) as active_requirements,
          (SELECT COUNT(*)
           FROM tasks n
           JOIN contacts u ON n.user_id = u.id
           WHERE n.status = 'open' AND u.assigned_admin_id = ?) as open_needs,
          (SELECT COUNT(*) FROM orders WHERE admin_id = ?) as total_orders,
          (SELECT COUNT(*) FROM appointments WHERE admin_id = ?) as total_appointments,
          (SELECT COUNT(*)
           FROM order_revenue
           WHERE admin_id = ?
             AND LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp') as whatsapp_orders,
          (SELECT COUNT(*)
           FROM order_revenue
           WHERE admin_id = ?
             AND LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp'
             AND COALESCE(collected_amount, 0) > 0
             AND payment_status <> 'refunded') as whatsapp_paid_orders,
          (SELECT COALESCE(
             SUM(
               CASE
                 WHEN LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp'
                 THEN GREATEST(COALESCE(booked_amount, 0), 0)
                 ELSE 0
               END
             ),
             0
           )
           FROM order_revenue
           WHERE admin_id = ?) as whatsapp_revenue_booked,
          (SELECT COALESCE(
             SUM(
               CASE
                 WHEN LOWER(COALESCE(channel, 'whatsapp')) = 'whatsapp'
                  AND payment_status <> 'refunded'
                 THEN GREATEST(COALESCE(collected_amount, 0), 0)
                 ELSE 0
               END
             ),
             0
           )
           FROM order_revenue
           WHERE admin_id = ?) as whatsapp_revenue_paid
      `,
      [
        adminId,
        adminId,
        adminId,
        adminId,
        adminId,
        adminId,
        adminId,
        adminId,
        adminId,
        adminId,
      ]
    );
    const data = stats[0] || {};
    const [aiSpendRows] = await connection.query(
      `
        SELECT COALESCE(SUM(total_cost_inr), 0) AS ai_spend_inr
        FROM admin_ai_usage
        WHERE admin_id = ?
          AND is_billable = TRUE
      `,
      [adminId]
    );
    const needsRevenueFallback =
      Number(data?.total_orders || 0) > 0 &&
      Number(data?.whatsapp_orders || 0) === 0 &&
      toAmountValue(data?.whatsapp_revenue_booked || 0) === 0 &&
      toAmountValue(data?.whatsapp_revenue_paid || 0) === 0;
    if (needsRevenueFallback) {
      const fallback = await getDashboardOrderRevenueSnapshotFromOrders(connection, adminId);
      data.whatsapp_orders = fallback.whatsapp_orders;
      data.whatsapp_paid_orders = fallback.whatsapp_paid_orders;
      data.whatsapp_revenue_booked = fallback.whatsapp_revenue_booked;
      data.whatsapp_revenue_paid = fallback.whatsapp_revenue_paid;
    }
    const subscriptionSnapshot = await getDashboardSubscriptionRevenueSnapshot(connection, adminId);
    data.growth_trend = await buildDashboardGrowthTrend(connection, adminId);
    data.lead_status_breakdown = await buildDashboardLeadStatusBreakdown(connection, adminId);
    const orderTrend = needsRevenueFallback
      ? await buildDashboardRevenueTrendFromOrders(connection, adminId)
      : await buildDashboardRevenueTrend(connection, adminId);
    const subscriptionTrend = await buildDashboardSubscriptionTrend(connection, adminId);
    data.revenue_trend = orderTrend.map((point, index) => {
      const subPoint = subscriptionTrend[index] || { earned: 0, booked: 0 };
      return {
        ...point,
        earned: toAmountValue(point.earned + subPoint.earned),
        booked: toAmountValue(point.booked + subPoint.booked),
      };
    });
    data.revenue_analysis = buildDashboardRevenueAnalysis(data.revenue_trend);
    data.subscription_revenue_paid = toAmountValue(subscriptionSnapshot.paid);
    data.subscription_revenue_booked = toAmountValue(subscriptionSnapshot.booked);
    data.subscription_revenue_outstanding = toAmountValue(subscriptionSnapshot.outstanding);
    data.whatsapp_revenue_paid = toAmountValue(data.whatsapp_revenue_paid);
    data.whatsapp_revenue_booked = toAmountValue(data.whatsapp_revenue_booked);
    data.whatsapp_revenue_outstanding = toAmountValue(
      Math.max(data.whatsapp_revenue_booked - data.whatsapp_revenue_paid, 0)
    );
    data.total_revenue_paid = toAmountValue(
      data.whatsapp_revenue_paid + data.subscription_revenue_paid
    );
    data.total_revenue_booked = toAmountValue(
      data.whatsapp_revenue_booked + data.subscription_revenue_booked
    );
    data.total_revenue_outstanding = toAmountValue(
      Math.max(data.total_revenue_booked - data.total_revenue_paid, 0)
    );
    data.ai_spend_inr = toAmountValue(aiSpendRows?.[0]?.ai_spend_inr || 0);
    return data;
  } finally {
    connection.release();
  }
}

export async function getAdminById(adminId) {
  const connection = await getConnection();
  try {
    await connection.query(
      `UPDATE admins
       SET status = 'inactive'
       WHERE status = 'active'
         AND access_expires_at IS NOT NULL
         AND access_expires_at <= NOW()`
    );

    const [rows] = await connection.query(
      `SELECT id, name, email, phone, admin_tier, status,
              business_name, business_category, business_type, service_label, product_label, booking_enabled,
              dashboard_subscription_expires_at,
              business_address, business_hours, business_map_url, access_expires_at,
              free_delivery_enabled, free_delivery_min_amount, free_delivery_scope, free_delivery_product_rules,
              two_factor_enabled,
              whatsapp_service_limit, whatsapp_product_limit,
              whatsapp_number, whatsapp_name, whatsapp_connected_at,
              ai_enabled, ai_prompt, ai_blocklist,
              created_at, updated_at
       FROM admins
       WHERE id = ?
       LIMIT 1`,
      [adminId]
    );
    const admin = rows[0] || null;
    if (!admin) return null;
    admin.free_delivery_product_rules = normalizeFreeDeliveryProductRules(
      admin.free_delivery_product_rules
    );
    return admin;
  } finally {
    connection.release();
  }
}

export async function getAdmins() {
  const connection = await getConnection();
  try {
    await connection.query(
      `UPDATE admins
       SET status = 'inactive'
       WHERE status = 'active'
         AND access_expires_at IS NOT NULL
         AND access_expires_at <= NOW()`
    );

    const [rows] = await connection.query(
      `SELECT
         a.id,
         a.name,
         a.email,
         a.phone,
         a.admin_tier,
         a.status,
         a.business_category,
         a.business_type,
         a.booking_enabled,
         a.access_expires_at,
         CASE
           WHEN a.admin_tier = 'super_admin' THEN TRUE
           ELSE COALESCE(s.charge_enabled, FALSE)
         END AS token_system_enabled,
         a.created_at,
         a.updated_at
       FROM admins a
       LEFT JOIN admin_billing_settings s ON s.admin_id = a.id
       ORDER BY a.created_at DESC`
    );
    return rows;
  } finally {
    connection.release();
  }
}

export async function updateAdminAccess(
  adminId,
  {
    admin_tier,
    status,
    business_category,
    business_type,
    booking_enabled,
    access_expires_at,
  } = {}
) {
  const payload = arguments[1] || {};
  const connection = await getConnection();
  try {
    const updates = [];
    const values = [];
    if (admin_tier) {
      updates.push('admin_tier = ?');
      values.push(admin_tier);
    }
    if (status) {
      updates.push('status = ?');
      values.push(status);
    }
    if (typeof business_category === 'string') {
      updates.push('business_category = ?');
      values.push(business_category.trim() || null);
    }
    if (typeof business_type === 'string') {
      const normalized = business_type.trim().toLowerCase();
      if (ALLOWED_BUSINESS_TYPES.has(normalized)) {
        updates.push('business_type = ?');
        values.push(normalized);
      }
    }
    if (typeof booking_enabled === 'boolean') {
      updates.push('booking_enabled = ?');
      values.push(booking_enabled);
    }
    if (Object.prototype.hasOwnProperty.call(payload, 'access_expires_at')) {
      updates.push('access_expires_at = ?');
      values.push(access_expires_at || null);
    }
    if (updates.length === 0) {
      const [rows] = await connection.query(
        `SELECT
           a.id,
           a.name,
           a.email,
           a.phone,
           a.admin_tier,
           a.status,
           a.business_category,
           a.business_type,
           a.booking_enabled,
           a.access_expires_at,
           CASE
             WHEN a.admin_tier = 'super_admin' THEN TRUE
             ELSE COALESCE(s.charge_enabled, FALSE)
           END AS token_system_enabled,
           a.created_at,
           a.updated_at
         FROM admins a
         LEFT JOIN admin_billing_settings s ON s.admin_id = a.id
         WHERE a.id = ?
         LIMIT 1`,
        [adminId]
      );
      return rows[0] || null;
    }
    values.push(adminId);
    await connection.query(
      `UPDATE admins SET ${updates.join(', ')} WHERE id = ?`,
      values
    );
    const [rows] = await connection.query(
      `SELECT
         a.id,
         a.name,
         a.email,
         a.phone,
         a.admin_tier,
         a.status,
         a.business_category,
         a.business_type,
         a.booking_enabled,
         a.access_expires_at,
         CASE
           WHEN a.admin_tier = 'super_admin' THEN TRUE
           ELSE COALESCE(s.charge_enabled, FALSE)
         END AS token_system_enabled,
         a.created_at,
         a.updated_at
       FROM admins a
       LEFT JOIN admin_billing_settings s ON s.admin_id = a.id
       WHERE a.id = ?
       LIMIT 1`,
      [adminId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function getLatestBusinessTypeChangeRequest(adminId) {
  if (!Number.isFinite(Number(adminId))) return null;
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          reason,
          status,
          monthly_current_inr,
          monthly_requested_inr,
          monthly_delta_inr,
          payment_required,
          payment_status,
          payment_link_id,
          payment_link_url,
          payment_paid_amount,
          payment_paid_at,
          resolved_by,
          resolved_at,
          created_at,
          updated_at
        FROM business_type_change_requests
        WHERE admin_id = ?
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [adminId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function getPendingBusinessTypeChangeRequest(adminId, requestedType = null) {
  if (!Number.isFinite(Number(adminId))) return null;
  const connection = await getConnection();
  try {
    const params = [adminId];
    let where = `WHERE admin_id = ? AND status = 'pending'`;
    if (typeof requestedType === 'string' && ALLOWED_BUSINESS_TYPES.has(requestedType)) {
      where += ' AND requested_business_type = ?';
      params.push(requestedType);
    }
    const [rows] = await connection.query(
      `
        SELECT
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          reason,
          status,
          monthly_current_inr,
          monthly_requested_inr,
          monthly_delta_inr,
          payment_required,
          payment_status,
          payment_link_id,
          payment_link_url,
          payment_paid_amount,
          payment_paid_at,
          created_at,
          updated_at
        FROM business_type_change_requests
        ${where}
        ORDER BY created_at DESC
        LIMIT 1
      `,
      params
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function createBusinessTypeChangeRequest({
  adminId,
  currentBusinessType,
  requestedBusinessType,
  reason = '',
  monthlyCurrentInr = null,
  monthlyRequestedInr = null,
  monthlyDeltaInr = null,
  paymentRequired = false,
  paymentStatus = 'unpaid',
  paymentLinkId = null,
  paymentLinkUrl = null,
} = {}) {
  if (!Number.isFinite(Number(adminId))) return null;
  const connection = await getConnection();
  try {
    const safeReason =
      typeof reason === 'string' ? sanitizeText(reason, 500).trim() || null : null;
    const [rows] = await connection.query(
      `
        INSERT INTO business_type_change_requests (
          admin_id,
          current_business_type,
          requested_business_type,
          reason,
          status,
          monthly_current_inr,
          monthly_requested_inr,
          monthly_delta_inr,
          payment_required,
          payment_status,
          payment_link_id,
          payment_link_url
        )
        VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
        RETURNING
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          reason,
          status,
          monthly_current_inr,
          monthly_requested_inr,
          monthly_delta_inr,
          payment_required,
          payment_status,
          payment_link_id,
          payment_link_url,
          created_at,
          updated_at
      `,
      [
        adminId,
        currentBusinessType,
        requestedBusinessType,
        safeReason,
        monthlyCurrentInr,
        monthlyRequestedInr,
        monthlyDeltaInr,
        Boolean(paymentRequired),
        paymentStatus,
        paymentLinkId,
        paymentLinkUrl,
      ]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function getBusinessTypeChangeRequests({ status = null, adminId = null } = {}) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (status) {
      whereParts.push('r.status = ?');
      params.push(status);
    }
    const scopedAdminId = toScopedAdminId(adminId);
    if (scopedAdminId) {
      whereParts.push('r.admin_id = ?');
      params.push(scopedAdminId);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        SELECT
          r.id,
          r.admin_id,
          r.current_business_type,
          r.requested_business_type,
          r.reason,
          r.status,
          r.monthly_current_inr,
          r.monthly_requested_inr,
          r.monthly_delta_inr,
          r.payment_required,
          r.payment_status,
          r.payment_link_id,
          r.payment_link_url,
          r.payment_paid_amount,
          r.payment_paid_at,
          r.resolved_by,
          r.resolved_at,
          r.created_at,
          r.updated_at,
          a.name AS admin_name,
          a.email AS admin_email,
          a.phone AS admin_phone,
          a.business_type AS admin_business_type
        FROM business_type_change_requests r
        JOIN admins a ON a.id = r.admin_id
        ${whereClause}
        ORDER BY r.created_at DESC
      `,
      params
    );
    return rows || [];
  } finally {
    connection.release();
  }
}

export async function getBusinessTypeChangeRequestById(requestId) {
  if (!Number.isFinite(Number(requestId))) return null;
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        SELECT
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          reason,
          status,
          monthly_current_inr,
          monthly_requested_inr,
          monthly_delta_inr,
          payment_required,
          payment_status,
          payment_link_id,
          payment_link_url,
          payment_paid_amount,
          payment_paid_at,
          resolved_by,
          resolved_at,
          created_at,
          updated_at
        FROM business_type_change_requests
        WHERE id = ?
        LIMIT 1
      `,
      [requestId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function markBusinessTypeChangeRequestPaid({
  paymentLinkId,
  paidAmount = 0,
  paidAt = null,
} = {}) {
  if (!paymentLinkId) return null;
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        UPDATE business_type_change_requests
        SET payment_status = 'paid',
            payment_paid_amount = ?,
            payment_paid_at = ?,
            updated_at = NOW()
        WHERE payment_link_id = ?
        RETURNING
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          status,
          payment_required,
          payment_status,
          payment_link_id
      `,
      [normalizeCurrencyAmount(paidAmount) || 0, paidAt || null, paymentLinkId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function approveBusinessTypeChangeRequest({ requestId, resolvedBy = null } = {}) {
  if (!Number.isFinite(Number(requestId))) return null;
  const connection = await getConnection();
  try {
    await connection.query('BEGIN');
    const [rows] = await connection.query(
      `
        SELECT
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          status,
          payment_required,
          payment_status
        FROM business_type_change_requests
        WHERE id = ?
        FOR UPDATE
      `,
      [requestId]
    );
    const request = rows[0];
    if (!request) {
      await connection.query('ROLLBACK');
      return null;
    }
    if (request.status !== 'pending') {
      await connection.query('ROLLBACK');
      throw new Error('Request already resolved.');
    }
    if (request.payment_required && request.payment_status !== 'paid') {
      await connection.query('ROLLBACK');
      throw new Error('Payment is not confirmed for this request.');
    }
    await connection.query(
      `UPDATE admins SET business_type = ? WHERE id = ?`,
      [request.requested_business_type, request.admin_id]
    );
    const [updatedRows] = await connection.query(
      `
        UPDATE business_type_change_requests
        SET status = 'approved',
            resolved_by = ?,
            resolved_at = NOW(),
            updated_at = NOW()
        WHERE id = ?
        RETURNING
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          status,
          resolved_by,
          resolved_at,
          updated_at
      `,
      [resolvedBy, requestId]
    );
    await connection.query('COMMIT');
    return updatedRows[0] || null;
  } catch (error) {
    try {
      await connection.query('ROLLBACK');
    } catch (_) {
      // ignore rollback errors
    }
    throw error;
  } finally {
    connection.release();
  }
}

export async function rejectBusinessTypeChangeRequest({ requestId, resolvedBy = null } = {}) {
  if (!Number.isFinite(Number(requestId))) return null;
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `
        UPDATE business_type_change_requests
        SET status = 'rejected',
            resolved_by = ?,
            resolved_at = NOW(),
            updated_at = NOW()
        WHERE id = ?
          AND status = 'pending'
        RETURNING
          id,
          admin_id,
          current_business_type,
          requested_business_type,
          status,
          resolved_by,
          resolved_at,
          updated_at
      `,
      [resolvedBy, requestId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function countSuperAdmins() {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `SELECT COUNT(*) as count FROM admins WHERE admin_tier = 'super_admin'`
    );
    return Number(rows?.[0]?.count || 0);
  } finally {
    connection.release();
  }
}

export async function deleteAdminAndData(adminId, transferToAdminId = null) {
  const connection = await getConnection();
  try {
    const [adminRows] = await connection.query(
      `SELECT id, name, admin_tier FROM admins WHERE id = ? LIMIT 1`,
      [adminId]
    );
    const admin = adminRows?.[0] || null;
    if (!admin) {
      return { ok: false, reason: 'not_found' };
    }

    let targetSuperAdminId = Number.isFinite(Number(transferToAdminId))
      ? Number(transferToAdminId)
      : null;
    if (targetSuperAdminId === adminId) {
      targetSuperAdminId = null;
    }
    if (targetSuperAdminId) {
      const [superRows] = await connection.query(
        `SELECT id FROM admins WHERE id = ? AND admin_tier = 'super_admin' LIMIT 1`,
        [targetSuperAdminId]
      );
      if (!superRows?.length) {
        targetSuperAdminId = null;
      }
    }
    if (!targetSuperAdminId) {
      const [superRows] = await connection.query(
        `SELECT id
         FROM admins
         WHERE admin_tier = 'super_admin' AND id <> ?
         ORDER BY id ASC
         LIMIT 1`,
        [adminId]
      );
      targetSuperAdminId = superRows?.[0]?.id || null;
    }
    if (!targetSuperAdminId) {
      return { ok: false, reason: 'no_super_admin_to_transfer' };
    }

    const [contactRows] = await connection.query(
      `SELECT id FROM contacts WHERE assigned_admin_id = ?`,
      [adminId]
    );
    const contactIds = contactRows.map((row) => Number(row.id)).filter((id) => Number.isFinite(id));

    await connection.query('BEGIN');

    await connection.query(
      `UPDATE contacts
       SET assigned_admin_id = ?,
           previous_owner_admin = CASE
             WHEN COALESCE(btrim(previous_owner_admin), '') = '' THEN ?
             ELSE previous_owner_admin
           END
       WHERE assigned_admin_id = ?`,
      [targetSuperAdminId, `${admin.id}:${admin.name || ''}`.slice(0, 180), adminId]
    );

    if (contactIds.length > 0) {
      const placeholders = contactIds.map(() => '?').join(', ');
      await connection.query(
        `DELETE FROM messages WHERE user_id IN (${placeholders})`,
        contactIds
      );
      await connection.query(
        `DELETE FROM leads WHERE user_id IN (${placeholders})`,
        contactIds
      );
      await connection.query(
        `DELETE FROM tasks WHERE user_id IN (${placeholders})`,
        contactIds
      );
      await connection.query(
        `DELETE FROM appointments WHERE user_id IN (${placeholders})`,
        contactIds
      );
    }

    await connection.query(`DELETE FROM messages WHERE admin_id = ?`, [adminId]);
    await connection.query(`DELETE FROM tasks WHERE assigned_to = ?`, [adminId]);
    await connection.query(`DELETE FROM appointments WHERE admin_id = ?`, [adminId]);
    await connection.query(`DELETE FROM orders WHERE admin_id = ?`, [adminId]);
    await connection.query(`DELETE FROM catalog_items WHERE admin_id = ?`, [adminId]);
    await connection.query(`DELETE FROM broadcasts WHERE created_by = ?`, [adminId]);
    await connection.query(`DELETE FROM templates WHERE created_by = ?`, [adminId]);

    const [deletedRows] = await connection.query(
      `DELETE FROM admins WHERE id = ? RETURNING id, name, email, phone, admin_tier`,
      [adminId]
    );

    await connection.query('COMMIT');
    return { ok: true, admin: deletedRows?.[0] || null, adminTier: admin.admin_tier };
  } catch (error) {
    try {
      await connection.query('ROLLBACK');
    } catch (rollbackError) {
      console.error('Failed to rollback admin delete:', rollbackError?.message || rollbackError);
    }
    throw error;
  } finally {
    connection.release();
  }
}

export async function getAdminAISettings(adminId) {
  const connection = await getConnection();
  try {
    let rows;
    try {
      [rows] = await connection.query(
        `SELECT ai_enabled, ai_prompt, ai_blocklist, automation_enabled,
                automation_trigger_mode, automation_trigger_keyword,
                appointment_start_hour, appointment_end_hour,
                appointment_slot_minutes, appointment_window_months
         FROM admins
         WHERE id = ?
         LIMIT 1`,
        [adminId]
      );
    } catch (error) {
      if (!isMissingColumnError(error)) {
        throw error;
      }
      [rows] = await connection.query(
        `SELECT ai_enabled, ai_prompt, ai_blocklist
         FROM admins
         WHERE id = ?
         LIMIT 1`,
        [adminId]
      );
    }
    const row = rows[0];
    if (!row) return null;
    const triggerModeRaw = String(row.automation_trigger_mode || 'any').trim().toLowerCase();
    const automation_trigger_mode = triggerModeRaw === 'keyword' ? 'keyword' : 'any';
    const automation_trigger_keyword =
      typeof row.automation_trigger_keyword === 'string' ? row.automation_trigger_keyword.trim() : '';
    return {
      ...row,
      automation_enabled:
        typeof row.automation_enabled === 'boolean' ? row.automation_enabled : true,
      automation_trigger_mode,
      automation_trigger_keyword,
      appointment_start_hour:
        normalizeAppointmentStartHour(row.appointment_start_hour) ??
        APPOINTMENT_SETTING_DEFAULTS.startHour,
      appointment_end_hour:
        normalizeAppointmentEndHour(row.appointment_end_hour) ??
        APPOINTMENT_SETTING_DEFAULTS.endHour,
      appointment_slot_minutes:
        normalizeAppointmentSlotMinutes(row.appointment_slot_minutes) ??
        APPOINTMENT_SETTING_DEFAULTS.slotMinutes,
      appointment_window_months:
        normalizeAppointmentWindowMonths(row.appointment_window_months) ??
        APPOINTMENT_SETTING_DEFAULTS.windowMonths,
    };
  } finally {
    connection.release();
  }
}

export async function updateAdminAISettings(
  adminId,
  {
    ai_enabled,
    ai_prompt,
    ai_blocklist,
    automation_enabled,
    automation_trigger_mode,
    automation_trigger_keyword,
    appointment_start_hour,
    appointment_end_hour,
    appointment_slot_minutes,
    appointment_window_months,
  }
) {
  const connection = await getConnection();
  try {
    const updates = [];
    const values = [];

    if (typeof ai_enabled === 'boolean') {
      updates.push('ai_enabled = ?');
      values.push(ai_enabled);
    }
    if (typeof ai_prompt === 'string') {
      updates.push('ai_prompt = ?');
      values.push(ai_prompt.trim() || null);
    }
    if (typeof ai_blocklist === 'string') {
      updates.push('ai_blocklist = ?');
      values.push(ai_blocklist.trim() || null);
    }
    if (typeof automation_enabled === 'boolean') {
      updates.push('automation_enabled = ?');
      values.push(automation_enabled);
    }
    if (typeof automation_trigger_mode === 'string') {
      const normalized = automation_trigger_mode.trim().toLowerCase();
      updates.push('automation_trigger_mode = ?');
      values.push(normalized === 'keyword' ? 'keyword' : 'any');
    }
    if (typeof automation_trigger_keyword === 'string') {
      const keyword = sanitizeText(automation_trigger_keyword, 40).trim();
      updates.push('automation_trigger_keyword = ?');
      values.push(keyword || null);
    }
    if (appointment_start_hour !== undefined) {
      const normalized = normalizeAppointmentStartHour(appointment_start_hour);
      if (normalized !== null) {
        updates.push('appointment_start_hour = ?');
        values.push(normalized);
      }
    }
    if (appointment_end_hour !== undefined) {
      const normalized = normalizeAppointmentEndHour(appointment_end_hour);
      if (normalized !== null) {
        updates.push('appointment_end_hour = ?');
        values.push(normalized);
      }
    }
    if (appointment_slot_minutes !== undefined) {
      const normalized = normalizeAppointmentSlotMinutes(appointment_slot_minutes);
      if (normalized !== null) {
        updates.push('appointment_slot_minutes = ?');
        values.push(normalized);
      }
    }
    if (appointment_window_months !== undefined) {
      const normalized = normalizeAppointmentWindowMonths(appointment_window_months);
      if (normalized !== null) {
        updates.push('appointment_window_months = ?');
        values.push(normalized);
      }
    }

    const startHourCandidate =
      appointment_start_hour !== undefined
        ? normalizeAppointmentStartHour(appointment_start_hour)
        : null;
    const endHourCandidate =
      appointment_end_hour !== undefined
        ? normalizeAppointmentEndHour(appointment_end_hour)
        : null;
    if (
      startHourCandidate !== null &&
      endHourCandidate !== null &&
      endHourCandidate <= startHourCandidate
    ) {
      throw new Error('appointment_end_hour must be greater than appointment_start_hour');
    }

    if (updates.length === 0) {
      return await getAdminAISettings(adminId);
    }

    values.push(adminId);
    await connection.query(
      `UPDATE admins SET ${updates.join(', ')} WHERE id = ?`,
      values
    );
    await connection.query(
      `
        UPDATE admins
        SET appointment_end_hour = LEAST(24, appointment_start_hour + 1)
        WHERE id = ?
          AND appointment_end_hour <= appointment_start_hour
      `,
      [adminId]
    );
    return await getAdminAISettings(adminId);
  } finally {
    connection.release();
  }
}

export async function updateAdminProfile(
  adminId,
  {
    name,
    email,
    business_name,
    business_category,
    business_type,
    service_label,
    product_label,
    business_address,
    business_hours,
    business_map_url,
    free_delivery_enabled,
    free_delivery_min_amount,
    free_delivery_scope,
    free_delivery_product_rules,
    two_factor_enabled,
    whatsapp_service_limit,
    whatsapp_product_limit,
  }
) {
  const connection = await getConnection();
  try {
    const updates = [];
    const values = [];
    if (typeof name === 'string') {
      const normalizedName = sanitizeNameUpper(name);
      if (normalizedName) {
        updates.push('name = ?');
        values.push(normalizedName);
      }
    }
    if (typeof email === 'string') {
      const normalizedEmail = sanitizeEmail(email);
      updates.push('email = ?');
      values.push(normalizedEmail);
    }
    if (typeof business_name === 'string') {
      updates.push('business_name = ?');
      values.push(sanitizeText(business_name, 140).trim() || null);
    }
    if (typeof business_category === 'string') {
      const normalizedCategory = business_category.trim();
      updates.push('business_category = ?');
      values.push(normalizedCategory || null);
    }
    if (typeof business_type === 'string') {
      const normalizedType = business_type.trim().toLowerCase();
      if (ALLOWED_BUSINESS_TYPES.has(normalizedType)) {
        updates.push('business_type = ?');
        values.push(normalizedType);
      }
    }
    if (typeof service_label === 'string') {
      updates.push('service_label = ?');
      values.push(sanitizeText(service_label, 60).trim() || null);
    }
    if (typeof product_label === 'string') {
      updates.push('product_label = ?');
      values.push(sanitizeText(product_label, 60).trim() || null);
    }
    if (typeof business_address === 'string') {
      updates.push('business_address = ?');
      values.push(sanitizeText(business_address, 500).trim() || null);
    }
    if (typeof business_hours === 'string') {
      updates.push('business_hours = ?');
      values.push(sanitizeText(business_hours, 160).trim() || null);
    }
    if (typeof business_map_url === 'string') {
      updates.push('business_map_url = ?');
      values.push(normalizeBusinessUrl(business_map_url));
    }
    if (typeof free_delivery_enabled === 'boolean') {
      updates.push('free_delivery_enabled = ?');
      values.push(free_delivery_enabled);
    }
    if (Object.prototype.hasOwnProperty.call(arguments[1] || {}, 'free_delivery_min_amount')) {
      updates.push('free_delivery_min_amount = ?');
      values.push(normalizeCurrencyAmount(free_delivery_min_amount));
    }
    if (typeof free_delivery_scope === 'string') {
      updates.push('free_delivery_scope = ?');
      values.push(normalizeFreeDeliveryScope(free_delivery_scope));
    }
    if (Object.prototype.hasOwnProperty.call(arguments[1] || {}, 'free_delivery_product_rules')) {
      updates.push('free_delivery_product_rules = ?');
      values.push(JSON.stringify(normalizeFreeDeliveryProductRules(free_delivery_product_rules)));
    }
    if (typeof two_factor_enabled === 'boolean') {
      updates.push('two_factor_enabled = ?');
      values.push(two_factor_enabled);
    }
    if (Object.prototype.hasOwnProperty.call(arguments[1] || {}, 'whatsapp_service_limit')) {
      updates.push('whatsapp_service_limit = ?');
      values.push(normalizeWhatsappLimit(whatsapp_service_limit));
    }
    if (Object.prototype.hasOwnProperty.call(arguments[1] || {}, 'whatsapp_product_limit')) {
      updates.push('whatsapp_product_limit = ?');
      values.push(normalizeWhatsappLimit(whatsapp_product_limit));
    }
    if (updates.length === 0) {
      return await getAdminById(adminId);
    }
    values.push(adminId);
    await connection.query(
      `UPDATE admins SET ${updates.join(', ')} WHERE id = ?`,
      values
    );
    return await getAdminById(adminId);
  } finally {
    connection.release();
  }
}

export async function getLatestRequirementForUser(userId, adminId = null) {
  const connection = await getConnection();
  try {
    const params = [userId];
    let whereClause = 'WHERE r.user_id = ?';
    if (adminId) {
      whereClause += ' AND u.assigned_admin_id = ?';
      params.push(adminId);
    }
    const [rows] = await connection.query(
      `
        SELECT r.*
        FROM leads r
        LEFT JOIN contacts u ON r.user_id = u.id
        ${whereClause}
        ORDER BY r.created_at DESC, r.id DESC
        LIMIT 1
      `,
      params
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function createRequirementFromRecentMessages(userId, adminId = null) {
  const connection = await getConnection();
  try {
    const params = [userId];
    const whereParts = ['m.user_id = ?'];
    if (adminId) {
      whereParts.push('m.admin_id = ?');
      params.push(adminId);
    }
    const whereClause = `WHERE ${whereParts.join(' AND ')}`;
    const [messages] = await connection.query(
      `
        SELECT m.message_text, m.message_type, m.created_at
        FROM messages m
        ${whereClause}
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT 20
      `,
      params
    );

    if (!messages.length) return null;
    const ordered = [...messages].sort(
      (a, b) => new Date(a.created_at) - new Date(b.created_at)
    );
    const incoming = ordered.filter((msg) => msg.message_type === 'incoming');
    const latestIncoming = incoming[incoming.length - 1];
    const fallbackLatest = ordered[ordered.length - 1];

    const reasonOfContacting = sanitizeText(
      latestIncoming?.message_text ||
      fallbackLatest?.message_text ||
      'Customer contacted for product/service information.',
      220
    );
    const summary = sanitizeText(
      [
        'Auto-generated lead summary from recent conversation.',
        ...ordered
          .slice(-12)
          .map(
            (msg) =>
              `${msg.message_type === 'incoming' ? 'Customer' : 'Business'}: ${sanitizeText(
                msg.message_text,
                280
              )}`
          ),
      ].join('\n'),
      4000
    );
    const category = sanitizeText(
      reasonOfContacting.split(/\s+/).slice(0, 6).join(' ') || 'General',
      120
    );

    const [rows] = await connection.query(
      `
        INSERT INTO leads (user_id, requirement_text, category, reason_of_contacting, status)
        VALUES (?, ?, ?, ?, 'pending')
        RETURNING *
      `,
      [userId, summary, category || 'General', reasonOfContacting || null]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

function parseTemplateVariables(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (err) {
    return [];
  }
}

export async function getAllBroadcasts(
  adminId = null,
  { search = '', limit = 50, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('b.created_by = ?');
      params.push(adminId);
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push('(LOWER(b.title) LIKE ? OR LOWER(b.message) LIKE ?)');
      params.push(q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        SELECT b.*, a.name as created_by_name
        FROM broadcasts b
        LEFT JOIN admins a ON b.created_by = a.id
        ${whereClause}
        ORDER BY b.created_at DESC, b.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return rows;
  } finally {
    connection.release();
  }
}

export async function getBroadcastStats(adminId = null) {
  const connection = await getConnection();
  try {
    const params = [];
    let whereClause = '';
    if (adminId) {
      whereClause = 'WHERE created_by = ?';
      params.push(adminId);
    }
    const [rows] = await connection.query(
      `
        SELECT
          COUNT(*)::int as total_count,
          COALESCE(SUM(sent_count), 0)::int as total_sent,
          COALESCE(SUM(delivered_count), 0)::int as total_delivered,
          SUM(CASE WHEN status = 'scheduled' THEN 1 ELSE 0 END)::int as scheduled_count
        FROM broadcasts
        ${whereClause}
      `,
      params
    );
    return rows[0] || {
      total_count: 0,
      total_sent: 0,
      total_delivered: 0,
      scheduled_count: 0,
    };
  } finally {
    connection.release();
  }
}

export async function getBroadcastById(broadcastId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `SELECT b.*, a.name as created_by_name
       FROM broadcasts b
       LEFT JOIN admins a ON b.created_by = a.id
       WHERE b.id = ?
       LIMIT 1`,
      [broadcastId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function createBroadcast({
  title,
  message,
  targetAudienceType,
  scheduledAt,
  status,
  createdBy,
}) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `INSERT INTO broadcasts
       (title, message, target_audience_type, scheduled_at, status, created_by)
       VALUES (?, ?, ?, ?, ?, ?)
       RETURNING id`,
      [
        title,
        message,
        targetAudienceType || 'all',
        scheduledAt || null,
        status || 'draft',
        createdBy || null,
      ]
    );
    return await getBroadcastById(rows[0]?.id);
  } finally {
    connection.release();
  }
}

export async function getAllTemplates(
  adminId = null,
  { search = '', limit = 50, offset = 0 } = {}
) {
  const connection = await getConnection();
  try {
    const params = [];
    const whereParts = [];
    if (adminId) {
      whereParts.push('t.created_by = ?');
      params.push(adminId);
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push('(LOWER(t.name) LIKE ? OR LOWER(t.category) LIKE ? OR LOWER(t.content) LIKE ?)');
      params.push(q, q, q);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `
        SELECT t.*, a.name as created_by_name
        FROM templates t
        LEFT JOIN admins a ON t.created_by = a.id
        ${whereClause}
        ORDER BY t.created_at DESC, t.id DESC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return rows.map((row) => ({
      ...row,
      variables: parseTemplateVariables(row.variables_json),
    }));
  } finally {
    connection.release();
  }
}

export async function getTemplateById(templateId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `SELECT t.*, a.name as created_by_name
       FROM templates t
       LEFT JOIN admins a ON t.created_by = a.id
       WHERE t.id = ?
       LIMIT 1`,
      [templateId]
    );
    const row = rows[0];
    if (!row) return null;
    return { ...row, variables: parseTemplateVariables(row.variables_json) };
  } finally {
    connection.release();
  }
}

export async function createTemplate({ name, category, content, variables, createdBy }) {
  const connection = await getConnection();
  try {
    const variablesJson = Array.isArray(variables) ? JSON.stringify(variables) : null;
    const [rows] = await connection.query(
      `INSERT INTO templates (name, category, content, variables_json, created_by)
       VALUES (?, ?, ?, ?, ?)
       RETURNING id`,
      [name, category, content, variablesJson, createdBy || null]
    );
    return await getTemplateById(rows[0]?.id);
  } finally {
    connection.release();
  }
}

const parseCatalogKeywords = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value
      .map((entry) => String(entry || '').trim())
      .filter(Boolean);
  }
  if (typeof value === 'string') {
    return value
      .split(/[,;\n]+/)
      .map((entry) => entry.trim())
      .filter(Boolean);
  }
  return [];
};

const serializeCatalogKeywords = (value) => {
  const keywords = parseCatalogKeywords(value);
  return keywords.length ? keywords.join(', ') : null;
};

const DURATION_UNIT_FACTORS = {
  minutes: 1,
  minute: 1,
  min: 1,
  mins: 1,
  hours: 60,
  hour: 60,
  hr: 60,
  hrs: 60,
  weeks: 60 * 24 * 7,
  week: 60 * 24 * 7,
  months: 60 * 24 * 30,
  month: 60 * 24 * 30,
};

const normalizeDurationUnit = (value) => {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  if (['minutes', 'minute', 'min', 'mins'].includes(raw)) return 'minutes';
  if (['hours', 'hour', 'hr', 'hrs'].includes(raw)) return 'hours';
  if (['weeks', 'week'].includes(raw)) return 'weeks';
  if (['months', 'month'].includes(raw)) return 'months';
  return null;
};

const normalizePriceLabelInr = (value) => {
  const text = String(value || '').trim();
  if (!text) return null;
  if (text.includes('₹')) {
    return text.replace(/₹\s*/g, '₹ ').replace(/\s{2,}/g, ' ').trim();
  }
  let normalized = text.replace(/^\s*(?:inr|rs\.?|rupees?)\s*/i, '₹ ');
  if (!normalized.includes('₹') && /^\d/.test(normalized)) {
    normalized = `₹ ${normalized}`;
  }
  return normalized.replace(/\s{2,}/g, ' ').trim();
};

const normalizeQuantityUnit = (value) => {
  const text = String(value || '').trim();
  if (!text) return null;
  return text.slice(0, 40);
};

const parseFiniteNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const normalizeCatalogSection = (value, fallback = 'catalog') => {
  const normalized = String(value || '').trim().toLowerCase();
  if (ALLOWED_CATALOG_SECTIONS.has(normalized)) return normalized;
  return fallback;
};

export async function getCatalogItems(
  adminId,
  { type = 'all', status = 'all', search = '', limit = 200, offset = 0, section = 'catalog' } = {}
) {
  const connection = await getConnection();
  try {
    const params = [adminId];
    const whereParts = ['admin_id = ?'];
    const normalizedSection = normalizeCatalogSection(section, 'catalog');
    if (type && type !== 'all') {
      whereParts.push('item_type = ?');
      params.push(type);
    }
    if (normalizedSection === 'catalog') {
      whereParts.push('COALESCE(is_booking_item, FALSE) = FALSE');
    } else if (normalizedSection === 'booking') {
      whereParts.push('COALESCE(is_booking_item, FALSE) = TRUE');
    }
    if (status && status !== 'all') {
      whereParts.push('is_active = ?');
      params.push(status === 'active');
    }
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      whereParts.push(
        '(LOWER(name) LIKE ? OR LOWER(COALESCE(category, \'\')) LIKE ? OR LOWER(COALESCE(description, \'\')) LIKE ? OR LOWER(COALESCE(keywords, \'\')) LIKE ?)'
      );
      params.push(q, q, q, q);
    }
    const whereClause = `WHERE ${whereParts.join(' AND ')}`;
    const [rows] = await connection.query(
      `
        SELECT *
        FROM catalog_items
        ${whereClause}
        ORDER BY sort_order ASC, name ASC, id ASC
        LIMIT ?
        OFFSET ?
      `,
      [...params, limit, offset]
    );
    return rows.map((row) => ({
      ...row,
      keywords: parseCatalogKeywords(row.keywords),
    }));
  } finally {
    connection.release();
  }
}

export async function getCatalogItemById(itemId, adminId) {
  const connection = await getConnection();
  try {
    const params = [itemId];
    let whereClause = 'WHERE id = ?';
    if (adminId) {
      whereClause += ' AND admin_id = ?';
      params.push(adminId);
    }
    const [rows] = await connection.query(
      `
        SELECT *
        FROM catalog_items
        ${whereClause}
        LIMIT 1
      `,
      params
    );
    const row = rows[0];
    if (!row) return null;
    return { ...row, keywords: parseCatalogKeywords(row.keywords) };
  } finally {
    connection.release();
  }
}

export async function createCatalogItem({
  adminId,
  item_type,
  name,
  category,
  description,
  price_label,
  duration_value,
  duration_unit,
  duration_minutes,
  quantity_value,
  quantity_unit,
  details_prompt,
  keywords,
  is_active,
  sort_order,
  is_bookable,
  is_booking_item,
  payment_required,
  free_delivery_eligible,
  show_on_whatsapp,
  whatsapp_sort_order,
}) {
  const connection = await getConnection();
  try {
    const keywordsValue = serializeCatalogKeywords(keywords);
    const normalizedDurationUnit = normalizeDurationUnit(duration_unit) || null;
    const normalizedDurationValue = parseFiniteNumber(duration_value);
    const legacyDurationMinutes = parseFiniteNumber(duration_minutes);
    let computedDurationMinutes = null;
    let computedDurationValue = null;
    let computedDurationUnit = null;

    if (normalizedDurationValue !== null && normalizedDurationValue > 0) {
      const unit = normalizedDurationUnit || 'minutes';
      const factor = DURATION_UNIT_FACTORS[unit] || 1;
      computedDurationValue = normalizedDurationValue;
      computedDurationUnit = unit;
      computedDurationMinutes = Math.round(normalizedDurationValue * factor);
    } else if (legacyDurationMinutes !== null && legacyDurationMinutes > 0) {
      computedDurationValue = legacyDurationMinutes;
      computedDurationUnit = normalizedDurationUnit || 'minutes';
      computedDurationMinutes = Math.round(legacyDurationMinutes);
    }

    const parsedQuantityValue = parseFiniteNumber(quantity_value);
    const computedQuantityValue =
      item_type === 'product' && parsedQuantityValue !== null && parsedQuantityValue > 0
        ? parsedQuantityValue
        : null;
    const computedQuantityUnit =
      computedQuantityValue !== null ? normalizeQuantityUnit(quantity_unit) || 'unit' : null;

    const [rows] = await connection.query(
      `INSERT INTO catalog_items
       (admin_id, item_type, name, category, description, price_label, duration_value, duration_unit, duration_minutes, quantity_value, quantity_unit, details_prompt, keywords, is_active, sort_order, is_bookable, is_booking_item, payment_required, free_delivery_eligible, show_on_whatsapp, whatsapp_sort_order)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
      [
        adminId,
        item_type,
        name,
        category || null,
        description || null,
        normalizePriceLabelInr(price_label),
        computedDurationValue,
        computedDurationUnit,
        computedDurationMinutes,
        computedQuantityValue,
        computedQuantityUnit,
        details_prompt || null,
        keywordsValue,
        typeof is_active === 'boolean' ? is_active : true,
        Number.isFinite(sort_order) ? sort_order : 0,
        typeof is_bookable === 'boolean' ? is_bookable : false,
        typeof is_booking_item === 'boolean' ? is_booking_item : false,
        typeof payment_required === 'boolean' ? payment_required : false,
        typeof free_delivery_eligible === 'boolean' ? free_delivery_eligible : false,
        typeof show_on_whatsapp === 'boolean' ? show_on_whatsapp : true,
        Number.isFinite(whatsapp_sort_order) ? whatsapp_sort_order : 0,
      ]
    );
    return await getCatalogItemById(rows[0]?.id, adminId);
  } finally {
    connection.release();
  }
}

export async function updateCatalogItem(itemId, adminId, updates = {}) {
  const connection = await getConnection();
  try {
    const fields = [];
    const params = [];

    if (Object.prototype.hasOwnProperty.call(updates, 'item_type')) {
      fields.push('item_type = ?');
      params.push(updates.item_type);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'name')) {
      fields.push('name = ?');
      params.push(updates.name);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'category')) {
      fields.push('category = ?');
      params.push(updates.category || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'description')) {
      fields.push('description = ?');
      params.push(updates.description || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'price_label')) {
      fields.push('price_label = ?');
      params.push(normalizePriceLabelInr(updates.price_label));
    }

    const hasDurationValue = Object.prototype.hasOwnProperty.call(updates, 'duration_value');
    const hasDurationUnit = Object.prototype.hasOwnProperty.call(updates, 'duration_unit');
    const hasDurationMinutes = Object.prototype.hasOwnProperty.call(updates, 'duration_minutes');
    if (hasDurationValue || hasDurationUnit || hasDurationMinutes) {
      const normalizedDurationUnit = normalizeDurationUnit(updates.duration_unit);
      const parsedDurationValue = parseFiniteNumber(updates.duration_value);
      const parsedDurationMinutes = parseFiniteNumber(updates.duration_minutes);

      let computedDurationValue = null;
      let computedDurationUnit = null;
      let computedDurationMinutes = null;

      if (parsedDurationValue !== null && parsedDurationValue > 0) {
        computedDurationUnit = normalizedDurationUnit || 'minutes';
        computedDurationValue = parsedDurationValue;
        computedDurationMinutes = Math.round(
          parsedDurationValue * (DURATION_UNIT_FACTORS[computedDurationUnit] || 1)
        );
      } else if (parsedDurationMinutes !== null && parsedDurationMinutes > 0) {
        computedDurationUnit = normalizedDurationUnit || 'minutes';
        computedDurationValue = parsedDurationMinutes;
        computedDurationMinutes = Math.round(parsedDurationMinutes);
      }

      fields.push('duration_value = ?');
      params.push(computedDurationValue);
      fields.push('duration_unit = ?');
      params.push(computedDurationUnit);
      fields.push('duration_minutes = ?');
      params.push(computedDurationMinutes);
    }

    const hasQuantityValue = Object.prototype.hasOwnProperty.call(updates, 'quantity_value');
    const hasQuantityUnit = Object.prototype.hasOwnProperty.call(updates, 'quantity_unit');
    if (hasQuantityValue || hasQuantityUnit) {
      const parsedQuantityValue = parseFiniteNumber(updates.quantity_value);
      const computedQuantityValue = parsedQuantityValue !== null && parsedQuantityValue > 0
        ? parsedQuantityValue
        : null;
      const computedQuantityUnit =
        computedQuantityValue !== null ? normalizeQuantityUnit(updates.quantity_unit) || 'unit' : null;

      fields.push('quantity_value = ?');
      params.push(computedQuantityValue);
      fields.push('quantity_unit = ?');
      params.push(computedQuantityUnit);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'details_prompt')) {
      fields.push('details_prompt = ?');
      params.push(updates.details_prompt || null);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'keywords')) {
      fields.push('keywords = ?');
      params.push(serializeCatalogKeywords(updates.keywords));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'is_active')) {
      fields.push('is_active = ?');
      params.push(Boolean(updates.is_active));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'sort_order')) {
      const order = updates.sort_order;
      fields.push('sort_order = ?');
      params.push(Number.isFinite(order) ? order : 0);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'is_bookable')) {
      fields.push('is_bookable = ?');
      params.push(Boolean(updates.is_bookable));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'is_booking_item')) {
      fields.push('is_booking_item = ?');
      params.push(Boolean(updates.is_booking_item));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'payment_required')) {
      fields.push('payment_required = ?');
      params.push(Boolean(updates.payment_required));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'free_delivery_eligible')) {
      fields.push('free_delivery_eligible = ?');
      params.push(Boolean(updates.free_delivery_eligible));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'show_on_whatsapp')) {
      fields.push('show_on_whatsapp = ?');
      params.push(Boolean(updates.show_on_whatsapp));
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'whatsapp_sort_order')) {
      const order = updates.whatsapp_sort_order;
      fields.push('whatsapp_sort_order = ?');
      params.push(Number.isFinite(order) ? order : 0);
    }

    if (fields.length === 0) {
      return await getCatalogItemById(itemId, adminId);
    }

    fields.push('updated_at = NOW()');
    params.push(itemId, adminId);

    await connection.query(
      `UPDATE catalog_items
       SET ${fields.join(', ')}
       WHERE id = ? AND admin_id = ?`,
      params
    );
    return await getCatalogItemById(itemId, adminId);
  } finally {
    connection.release();
  }
}

export async function deleteCatalogItem(itemId, adminId) {
  const connection = await getConnection();
  try {
    const [rows] = await connection.query(
      `DELETE FROM catalog_items
       WHERE id = ? AND admin_id = ?
       RETURNING id`,
      [itemId, adminId]
    );
    return rows[0] || null;
  } finally {
    connection.release();
  }
}

export async function getReportOverview(startDate, adminId = null) {
  const connection = await getConnection();
  try {
    const buildLeadStatusBreakdown = async () => {
      const counts = {
        pending: 0,
        in_progress: 0,
        completed: 0,
      };

      const [rows] = adminId
        ? await connection.query(
            `
              WITH latest_leads AS (
                SELECT DISTINCT ON (l.user_id) l.user_id, l.status
                FROM leads l
                ORDER BY l.user_id, l.created_at DESC, l.id DESC
              )
              SELECT COALESCE(latest_leads.status, 'pending') AS status, COUNT(*) AS count
              FROM contacts c
              LEFT JOIN latest_leads ON latest_leads.user_id = c.id
              WHERE c.assigned_admin_id = ?
              GROUP BY COALESCE(latest_leads.status, 'pending')
            `,
            [adminId]
          )
        : await connection.query(
            `
              WITH latest_leads AS (
                SELECT DISTINCT ON (l.user_id) l.user_id, l.status
                FROM leads l
                ORDER BY l.user_id, l.created_at DESC, l.id DESC
              )
              SELECT COALESCE(latest_leads.status, 'pending') AS status, COUNT(*) AS count
              FROM contacts c
              LEFT JOIN latest_leads ON latest_leads.user_id = c.id
              GROUP BY COALESCE(latest_leads.status, 'pending')
            `
          );

      for (const row of rows || []) {
        const key = String(row?.status || '').trim().toLowerCase();
        if (!Object.prototype.hasOwnProperty.call(counts, key)) continue;
        counts[key] = Number(row?.count || 0);
      }

      return [
        { status: 'pending', label: 'Pending', count: counts.pending },
        { status: 'in_progress', label: 'In Progress', count: counts.in_progress },
        { status: 'completed', label: 'Completed', count: counts.completed },
      ];
    };

    const messageParams = [startDate];
    let messageWhere = "WHERE created_at >= ? AND message_type = 'incoming'";
    if (adminId) {
      messageWhere += ' AND admin_id = ?';
      messageParams.push(adminId);
    }
    const [messageStats] = await connection.query(
      `
        SELECT date_trunc('day', created_at) as date, COUNT(*) as count
        FROM messages
        ${messageWhere}
        GROUP BY date_trunc('day', created_at)
        ORDER BY date_trunc('day', created_at)
      `,
      messageParams
    );

    const totalParams = [];
    let totalWhere = "WHERE message_type = 'incoming'";
    if (adminId) {
      totalWhere += ' AND admin_id = ?';
      totalParams.push(adminId);
    }
    const [totalRows] = await connection.query(
      `
        SELECT COUNT(*) as count
        FROM messages
        ${totalWhere}
      `,
      totalParams
    );
    const totalMessages = Number(totalRows?.[0]?.count || 0);

    if (adminId) {
      const leadStats = await buildLeadStatusBreakdown();
      const [contactRows] = await connection.query(
        `
          SELECT COUNT(*) as count
          FROM contacts
          WHERE assigned_admin_id = ?
        `,
        [adminId]
      );
      const totalContacts = Number(contactRows?.[0]?.count || 0);

      const [agentPerformance] = await connection.query(
        `
          SELECT
            a.id,
            a.name,
            a.admin_tier,
            a.status,
            SUM(CASE WHEN m.message_type = 'outgoing' THEN 1 ELSE 0 END) AS messages_sent,
            COUNT(DISTINCT CASE
              WHEN m.created_at >= (NOW() - INTERVAL '7 days') THEN m.user_id
              ELSE NULL
            END) AS active_chats
          FROM admins a
          LEFT JOIN messages m ON m.admin_id = a.id
          WHERE a.id = ?
          GROUP BY a.id, a.name, a.admin_tier, a.status
        `,
        [adminId]
      );

      const [topCampaigns] = await connection.query(
        `
          SELECT id, title, status, sent_count, delivered_count, created_at
          FROM broadcasts
          WHERE created_by = ?
          ORDER BY sent_count DESC, created_at DESC
          LIMIT 5
        `,
        [adminId]
      );

      return {
        messageStats,
        totalMessages,
        leadStats,
        totalContacts,
        agentPerformance,
        topCampaigns,
        revenueSources: [],
      };
    }

    const leadStats = await buildLeadStatusBreakdown();
    const [contactRows] = await connection.query(
      `
        SELECT COUNT(*) as count
        FROM contacts
      `
    );
    const totalContacts = Number(contactRows?.[0]?.count || 0);

    const [agentPerformance] = await connection.query(`
      SELECT
        a.id,
        a.name,
        a.admin_tier,
        a.status,
        SUM(CASE WHEN m.message_type = 'outgoing' THEN 1 ELSE 0 END) AS messages_sent,
        COUNT(DISTINCT CASE
          WHEN m.created_at >= (NOW() - INTERVAL '7 days') THEN m.user_id
          ELSE NULL
        END) AS active_chats
      FROM admins a
      LEFT JOIN messages m ON m.admin_id = a.id
      GROUP BY a.id, a.name, a.admin_tier, a.status, a.created_at
      ORDER BY a.created_at DESC
    `);

    const [topCampaigns] = await connection.query(`
      SELECT id, title, status, sent_count, delivered_count, created_at
      FROM broadcasts
      ORDER BY sent_count DESC, created_at DESC
      LIMIT 5
    `);

    return {
      messageStats,
      totalMessages,
      leadStats,
      totalContacts,
      agentPerformance,
      topCampaigns,
      revenueSources: [],
    };
  } finally {
    connection.release();
  }
}

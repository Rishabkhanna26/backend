import { requireAuth, requireReadAuth } from '../../../lib/auth-server';
import { protectModificationAction } from '../../../lib/api-protection';
import { createCatalogItem, getCatalogItems } from '../../../lib/db-helpers';
import { parsePagination, parseSearch, parseStatus } from '../../../lib/api-utils';
import { hasBookingAccess } from '../../../lib/business.js';
import { resolveBookingCategoryLabel } from '../../../lib/booking.js';

const parseBoolean = (value, fallback = false) => {
  if (value === true || value === false) return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
  }
  return fallback;
};

const parseNumber = (value, fallback = null) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return num;
};

const parseDurationUnit = (value, fallback = 'minutes') => {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['minutes', 'minute', 'min', 'mins'].includes(raw)) return 'minutes';
  if (['hours', 'hour', 'hr', 'hrs'].includes(raw)) return 'hours';
  if (['weeks', 'week'].includes(raw)) return 'weeks';
  if (['months', 'month'].includes(raw)) return 'months';
  return fallback;
};

const toDurationMinutes = (value, unit) => {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return null;
  const factors = {
    minutes: 1,
    hours: 60,
    weeks: 60 * 24 * 7,
    months: 60 * 24 * 30,
  };
  const factor = factors[parseDurationUnit(unit, 'minutes')] || 1;
  return Math.round(num * factor);
};

const parsePriceAmount = (value) => {
  if (value == null) return null;
  const raw = String(value || '').replace(/,/g, '');
  const matched = raw.match(/(\d+(?:\.\d+)?)/);
  if (!matched) return null;
  const amount = Number(matched[1]);
  return Number.isFinite(amount) ? Number(amount.toFixed(2)) : null;
};

export const runtime = 'nodejs';

export async function GET(request) {
  try {
    const user = await requireReadAuth();
    if (!user.restricted_mode && !hasBookingAccess(user)) {
      return Response.json({ success: false, error: 'Booking section is disabled.' }, { status: 403 });
    }

    const { searchParams } = new URL(request.url);
    const { limit, offset } = parsePagination(searchParams, { defaultLimit: 200, maxLimit: 500 });
    const search = parseSearch(searchParams);
    let status = parseStatus(searchParams, 'all');
    if (!['all', 'active', 'inactive'].includes(status)) {
      status = 'all';
    }

    const items = await getCatalogItems(user.id, {
      type: 'service',
      status,
      search,
      limit: limit + 1,
      offset,
      section: 'booking',
    });
    const hasMore = items.length > limit;
    const data = hasMore ? items.slice(0, limit) : items;

    return Response.json({
      success: true,
      data,
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
    const guard = await protectModificationAction(user, 'create');
    if (guard) return guard;
    if (!hasBookingAccess(user)) {
      return Response.json({ success: false, error: 'Booking section is disabled.' }, { status: 403 });
    }

    const body = await request.json();
    const name = String(body?.name || '').trim();
    const paymentRequired = parseBoolean(body?.payment_required, false);
    if (!name) {
      return Response.json({ success: false, error: 'Name is required.' }, { status: 400 });
    }
    if (paymentRequired && !(Number.isFinite(parsePriceAmount(body?.price_label)) && parsePriceAmount(body?.price_label) > 0)) {
      return Response.json(
        { success: false, error: 'Paid booking items must include a valid numeric price.' },
        { status: 400 }
      );
    }

    const item = await createCatalogItem({
      adminId: user.id,
      item_type: 'service',
      name,
      category: resolveBookingCategoryLabel(body?.category, 'Booking'),
      description: String(body?.description || '').trim(),
      price_label: String(body?.price_label || '').trim(),
      duration_value: parseNumber(body?.duration_value),
      duration_unit: parseDurationUnit(body?.duration_unit),
      duration_minutes:
        toDurationMinutes(body?.duration_value, body?.duration_unit) ??
        parseNumber(body?.duration_minutes),
      details_prompt: String(body?.details_prompt || '').trim(),
      keywords: body?.keywords,
      is_active: parseBoolean(body?.is_active, true),
      sort_order: parseNumber(body?.sort_order, 0),
      is_bookable: true,
      is_booking_item: true,
      payment_required: paymentRequired,
      free_delivery_eligible: false,
    });

    return Response.json({ success: true, data: item });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

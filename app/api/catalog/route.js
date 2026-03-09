import { requireAuth } from '../../../lib/auth-server';
import { createCatalogItem, getCatalogItems } from '../../../lib/db-helpers';
import { parsePagination, parseSearch, parseStatus } from '../../../lib/api-utils';
import { canUseCatalogItemType } from '../../../lib/business.js';

const parseType = (searchParams) => {
  const value = searchParams?.get('type');
  if (!value) return 'all';
  const normalized = value.trim().toLowerCase();
  if (['service', 'product', 'all'].includes(normalized)) return normalized;
  return 'all';
};

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

export async function GET(request) {
  try {
    const user = await requireAuth();
    const { searchParams } = new URL(request.url);
    const { limit, offset } = parsePagination(searchParams, { defaultLimit: 200, maxLimit: 500 });
    const search = parseSearch(searchParams);
    let status = parseStatus(searchParams, 'all');
    if (!['all', 'active', 'inactive'].includes(status)) {
      status = 'all';
    }
    const type = parseType(searchParams);

    const items = await getCatalogItems(user.id, {
      type,
      status,
      search,
      limit: limit + 1,
      offset,
      section: 'catalog',
    });
    const hasMore = items.length > limit;
    const data = hasMore ? items.slice(0, limit) : items;

    const response = Response.json({
      success: true,
      data,
      meta: {
        limit,
        offset,
        hasMore,
        nextOffset: hasMore ? offset + limit : null,
      },
    });
    response.headers.set('Cache-Control', 'private, max-age=30, stale-while-revalidate=120');
    return response;
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
    const body = await request.json();
    const itemType = String(body?.item_type || body?.type || '').trim().toLowerCase();
    const name = String(body?.name || '').trim();
    const isBookable = itemType === 'service' ? parseBoolean(body?.is_bookable, false) : false;
    const paymentRequired = itemType === 'service' && isBookable
      ? parseBoolean(body?.payment_required, false)
      : false;
    const freeDeliveryEligible = itemType === 'product'
      ? parseBoolean(body?.free_delivery_eligible, false)
      : false;
    const showOnWhatsApp = parseBoolean(body?.show_on_whatsapp, true);
    const whatsappSortOrder = parseNumber(body?.whatsapp_sort_order, 0);

    if (!['service', 'product'].includes(itemType)) {
      return Response.json({ success: false, error: 'Invalid item type.' }, { status: 400 });
    }
    if (!name) {
      return Response.json({ success: false, error: 'Name is required.' }, { status: 400 });
    }
    if (!canUseCatalogItemType(user, itemType)) {
      return Response.json(
        { success: false, error: `Your business type cannot add ${itemType} items.` },
        { status: 403 }
      );
    }
    if (paymentRequired && !(Number.isFinite(parsePriceAmount(body?.price_label)) && parsePriceAmount(body?.price_label) > 0)) {
      return Response.json(
        { success: false, error: 'Paid services must include a valid numeric price.' },
        { status: 400 }
      );
    }

    const item = await createCatalogItem({
      adminId: user.id,
      item_type: itemType,
      name,
      category: String(body?.category || '').trim(),
      description: String(body?.description || '').trim(),
      price_label: String(body?.price_label || '').trim(),
      duration_value: parseNumber(body?.duration_value),
      duration_unit: parseDurationUnit(body?.duration_unit),
      duration_minutes:
        toDurationMinutes(body?.duration_value, body?.duration_unit) ??
        parseNumber(body?.duration_minutes),
      quantity_value: itemType === 'product' ? parseNumber(body?.quantity_value) : null,
      quantity_unit: itemType === 'product' ? String(body?.quantity_unit || '').trim() : null,
      details_prompt: String(body?.details_prompt || '').trim(),
      keywords: body?.keywords,
      is_active: parseBoolean(body?.is_active, true),
      sort_order: parseNumber(body?.sort_order, 0),
      is_bookable: isBookable,
      is_booking_item: false,
      payment_required: paymentRequired,
      free_delivery_eligible: freeDeliveryEligible,
      show_on_whatsapp: showOnWhatsApp,
      whatsapp_sort_order: whatsappSortOrder,
      is_time_based: itemType === 'service' ? parseBoolean(body?.is_time_based, false) : false,
    });

    return Response.json({ success: true, data: item });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

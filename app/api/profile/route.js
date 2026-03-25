import { NextResponse } from 'next/server';
import fs from 'fs/promises';
import path from 'path';
import { requireAuth, requireReadAuth } from '../../../lib/auth-server';
import { protectModificationAction } from '../../../lib/api-protection';
import { signAuthToken } from '../../../lib/auth';
import { getAdminById, updateAdminProfile } from '../../../lib/db-helpers';
import { sanitizeEmail, sanitizeNameUpper, sanitizeText } from '../../../lib/sanitize.js';

export const runtime = 'nodejs';

const getSafeFolder = (value) => String(value || '').replace(/\D/g, '');
const normalizeBusinessUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    if (!['http:', 'https:'].includes(parsed.protocol)) return '';
    return parsed.toString();
  } catch {
    return '';
  }
};

const parseBoolean = (value, fallback = undefined) => {
  if (value === true || value === false) return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
  }
  return fallback;
};

const parseAmount = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return Number(num.toFixed(2));
};

const parseWhatsappLimit = (value) => {
  if (value === null || value === undefined || value === '') return undefined;
  const num = Number(value);
  if (!Number.isFinite(num)) return undefined;
  const normalized = Math.trunc(num);
  if (normalized < 0) return 0;
  if (normalized > 25) return 25;
  return normalized;
};

const normalizeFreeDeliveryScope = (value) => {
  const normalized = String(value || '').trim().toLowerCase();
  if (['combined', 'eligible_only'].includes(normalized)) return normalized;
  return 'combined';
};

const parseFreeDeliveryProductRules = (value) => {
  const list = Array.isArray(value) ? value : [];
  const seen = new Set();
  const normalized = [];
  list.forEach((entry) => {
    const productIdRaw =
      entry?.catalog_item_id ?? entry?.catalogItemId ?? entry?.product_id ?? entry?.productId;
    const minAmountRaw = entry?.min_amount ?? entry?.minAmount;
    const productNameRaw =
      entry?.product_name ?? entry?.productName ?? entry?.name ?? '';
    const productId = Math.trunc(Number(productIdRaw));
    const minAmount = parseAmount(minAmountRaw);
    if (!Number.isFinite(productId) || productId <= 0) return;
    if (!(Number.isFinite(minAmount) && minAmount > 0)) return;
    if (seen.has(productId)) return;
    seen.add(productId);
    normalized.push({
      catalog_item_id: productId,
      min_amount: minAmount,
      product_name: sanitizeText(productNameRaw, 160) || null,
    });
  });
  return normalized.slice(0, 100);
};

const getProfilePhotoUrl = async (phone, request) => {
  const folderName = getSafeFolder(phone);
  if (!folderName) return null;
  const publicDir = path.join(process.cwd(), 'public', folderName);
  const candidates = ['profile.jpg', 'profile.jpeg', 'profile.png', 'profile.webp'];
  for (const candidate of candidates) {
    try {
      await fs.access(path.join(publicDir, candidate));
      return new URL(`/${folderName}/${candidate}`, request.url).toString();
    } catch (error) {
      // continue
    }
  }
  return null;
};

export async function GET(request) {
  try {
    const user = await requireReadAuth();
    const admin = await getAdminById(user.id);
    if (!admin) {
      const response = NextResponse.json({ success: false, error: 'User not found' }, { status: 401 });
      response.cookies.set({
        name: 'auth_token',
        value: '',
        httpOnly: true,
        sameSite: 'lax',
        path: '/',
        maxAge: 0,
        secure: process.env.NODE_ENV === 'production',
      });
      return response;
    }
    const profilePhotoUrl = await getProfilePhotoUrl(admin.phone, request);
    return NextResponse.json({
      success: true,
      data: {
        ...admin,
        profile_photo_url: profilePhotoUrl,
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function PUT(request) {
  try {
    const user = await requireAuth();
    const guard = await protectModificationAction(user, 'update');
    if (guard) return guard;
    const body = await request.json();
    const name = typeof body.name === 'string' ? sanitizeNameUpper(body.name) : undefined;
    const email = typeof body.email === 'string' ? sanitizeEmail(body.email) || '' : undefined;
    const businessName =
      typeof body.business_name === 'string' ? sanitizeText(body.business_name, 140) : undefined;
    const businessCategory =
      typeof body.business_category === 'string'
        ? sanitizeText(body.business_category, 120)
        : undefined;
    const businessTypeRaw =
      typeof body.business_type === 'string' ? body.business_type.trim().toLowerCase() : undefined;
    const serviceLabel =
      typeof body.service_label === 'string' ? sanitizeText(body.service_label, 60) : undefined;
    const productLabel =
      typeof body.product_label === 'string' ? sanitizeText(body.product_label, 60) : undefined;
    const businessAddress =
      typeof body.business_address === 'string'
        ? sanitizeText(body.business_address, 500)
        : undefined;
    const businessHours =
      typeof body.business_hours === 'string' ? sanitizeText(body.business_hours, 160) : undefined;
    const businessMapUrl =
      typeof body.business_map_url === 'string'
        ? normalizeBusinessUrl(body.business_map_url)
        : undefined;
    const freeDeliveryEnabled = Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_enabled')
      ? parseBoolean(body?.free_delivery_enabled)
      : undefined;
    const twoFactorEnabled = Object.prototype.hasOwnProperty.call(body || {}, 'two_factor_enabled')
      ? parseBoolean(body?.two_factor_enabled)
      : undefined;
    const freeDeliveryMinAmount = Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_min_amount')
      ? parseAmount(body?.free_delivery_min_amount)
      : undefined;
    const freeDeliveryScope = Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_scope')
      ? normalizeFreeDeliveryScope(body?.free_delivery_scope)
      : undefined;
    const freeDeliveryProductRules = Object.prototype.hasOwnProperty.call(
      body || {},
      'free_delivery_product_rules'
    )
      ? parseFreeDeliveryProductRules(body?.free_delivery_product_rules)
      : undefined;
    const whatsappServiceLimit = Object.prototype.hasOwnProperty.call(body || {}, 'whatsapp_service_limit')
      ? parseWhatsappLimit(body?.whatsapp_service_limit)
      : undefined;
    const whatsappProductLimit = Object.prototype.hasOwnProperty.call(body || {}, 'whatsapp_product_limit')
      ? parseWhatsappLimit(body?.whatsapp_product_limit)
      : undefined;
    const allowedBusinessTypes = new Set(['product', 'service', 'both']);
    const businessType =
      businessTypeRaw && allowedBusinessTypes.has(businessTypeRaw)
        ? businessTypeRaw
        : undefined;
    const currentBusinessType = String(user?.business_type || 'both').trim().toLowerCase();

    if (
      businessType &&
      user.admin_tier !== 'super_admin' &&
      businessType !== currentBusinessType
    ) {
      return NextResponse.json(
        {
          success: false,
          error: 'Business type changes require super admin approval. Please submit a request.',
        },
        { status: 403 }
      );
    }
    if (Object.prototype.hasOwnProperty.call(body || {}, 'two_factor_enabled')) {
      if (typeof twoFactorEnabled !== 'boolean') {
        return NextResponse.json(
          { success: false, error: 'Invalid 2FA setting value.' },
          { status: 400 }
        );
      }
      const effectiveEmail = typeof email === 'string' ? email : sanitizeEmail(user?.email);
      if (twoFactorEnabled && !effectiveEmail) {
        return NextResponse.json(
          { success: false, error: 'Add a valid email before enabling 2-step login.' },
          { status: 400 }
        );
      }
    }

    const effectiveFreeDeliveryEnabled =
      typeof freeDeliveryEnabled === 'boolean'
        ? freeDeliveryEnabled
        : Boolean(user?.free_delivery_enabled);
    const effectiveFreeDeliveryScope =
      freeDeliveryScope || normalizeFreeDeliveryScope(user?.free_delivery_scope);
    const effectiveFreeDeliveryMinAmount =
      freeDeliveryMinAmount !== undefined
        ? freeDeliveryMinAmount
        : parseAmount(user?.free_delivery_min_amount);
    const effectiveFreeDeliveryRules =
      freeDeliveryProductRules !== undefined
        ? freeDeliveryProductRules
        : parseFreeDeliveryProductRules(user?.free_delivery_product_rules);
    const freeDeliverySettingsTouched =
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_enabled') ||
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_min_amount') ||
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_scope') ||
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_product_rules');

    if (freeDeliverySettingsTouched && effectiveFreeDeliveryEnabled) {
      if (effectiveFreeDeliveryScope === 'combined') {
        if (
          !(
            Number.isFinite(effectiveFreeDeliveryMinAmount) &&
            effectiveFreeDeliveryMinAmount > 0
          )
        ) {
          return NextResponse.json(
            { success: false, error: 'Free delivery minimum amount must be greater than 0.' },
            { status: 400 }
          );
        }
      } else if (effectiveFreeDeliveryScope === 'eligible_only') {
        if (!Array.isArray(effectiveFreeDeliveryRules) || effectiveFreeDeliveryRules.length === 0) {
          return NextResponse.json(
            {
              success: false,
              error: 'Add at least one product rule for "Only marked products".',
            },
            { status: 400 }
          );
        }
      }
    }

    const updatePayload = {
      name,
      email,
      business_name: businessName,
      business_category: businessCategory,
      business_type: businessType,
      service_label: serviceLabel,
      product_label: productLabel,
      business_address: businessAddress,
      business_hours: businessHours,
      business_map_url: businessMapUrl,
    };
    if (Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_enabled')) {
      updatePayload.free_delivery_enabled = freeDeliveryEnabled;
    }
    if (Object.prototype.hasOwnProperty.call(body || {}, 'two_factor_enabled')) {
      updatePayload.two_factor_enabled = twoFactorEnabled;
    }
    if (
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_min_amount') ||
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_scope') ||
      Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_product_rules')
    ) {
      updatePayload.free_delivery_min_amount =
        effectiveFreeDeliveryScope === 'combined' ? effectiveFreeDeliveryMinAmount : null;
    }
    if (Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_scope')) {
      updatePayload.free_delivery_scope = freeDeliveryScope;
    }
    if (Object.prototype.hasOwnProperty.call(body || {}, 'free_delivery_product_rules')) {
      updatePayload.free_delivery_product_rules =
        effectiveFreeDeliveryScope === 'eligible_only'
          ? freeDeliveryProductRules ?? []
          : [];
    }
    if (Object.prototype.hasOwnProperty.call(body || {}, 'whatsapp_service_limit')) {
      updatePayload.whatsapp_service_limit = whatsappServiceLimit;
    }
    if (Object.prototype.hasOwnProperty.call(body || {}, 'whatsapp_product_limit')) {
      updatePayload.whatsapp_product_limit = whatsappProductLimit;
    }

    const admin = await updateAdminProfile(user.id, updatePayload);
    if (!admin) {
      return NextResponse.json({ success: false, error: 'User not found' }, { status: 404 });
    }
    const token = signAuthToken({
      id: admin.id,
      name: admin.name,
      email: admin.email,
      phone: admin.phone,
      admin_tier: admin.admin_tier,
      business_name: admin.business_name,
      business_category: admin.business_category,
      business_type: admin.business_type,
      service_label: admin.service_label,
      product_label: admin.product_label,
      booking_enabled: admin.booking_enabled,
      business_address: admin.business_address,
      business_hours: admin.business_hours,
      business_map_url: admin.business_map_url,
      free_delivery_enabled: admin.free_delivery_enabled,
      free_delivery_min_amount: admin.free_delivery_min_amount,
      free_delivery_scope: admin.free_delivery_scope,
      free_delivery_product_rules: admin.free_delivery_product_rules,
      two_factor_enabled: admin.two_factor_enabled === true,
      whatsapp_service_limit: admin.whatsapp_service_limit,
      whatsapp_product_limit: admin.whatsapp_product_limit,
      access_expires_at: admin.access_expires_at,
    });

    const profilePhotoUrl = await getProfilePhotoUrl(admin.phone, request);
    const response = NextResponse.json({
      success: true,
      data: {
        ...admin,
        profile_photo_url: profilePhotoUrl,
      },
    });
    response.cookies.set({
      name: 'auth_token',
      value: token,
      httpOnly: true,
      sameSite: 'lax',
      path: '/',
      maxAge: 60 * 60 * 24 * 7,
      secure: process.env.NODE_ENV === 'production',
    });
    return response;
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

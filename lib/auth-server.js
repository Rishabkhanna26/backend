import { cookies } from 'next/headers';
import { verifyAuthToken } from './auth';
import { getAdminById, getAdminBillingSettings } from './db-helpers';

const isAccessExpired = (value) => {
  if (!value) return false;
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return false;
  return time <= Date.now();
};

const isAccessGrantActive = (value) => {
  if (!value) return false;
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return false;
  return time > Date.now();
};

const buildAuthUser = (admin, extras = {}) => ({
  id: admin.id,
  name: admin.name,
  email: admin.email,
  phone: admin.phone,
  admin_tier: admin.admin_tier,
  status: admin.status,
  restricted_mode: admin.status !== 'active',
  business_name: admin.business_name,
  business_category: admin.business_category,
  business_type: admin.business_type,
  service_label: admin.service_label,
  product_label: admin.product_label,
  dashboard_subscription_expires_at: admin.dashboard_subscription_expires_at,
  booking_enabled: admin.booking_enabled,
  business_address: admin.business_address,
  business_hours: admin.business_hours,
  business_map_url: admin.business_map_url,
  free_delivery_enabled: admin.free_delivery_enabled,
  free_delivery_min_amount: admin.free_delivery_min_amount,
  free_delivery_scope: admin.free_delivery_scope,
  access_expires_at: admin.access_expires_at,
  ...extras,
});

export async function getAuthUser({ allowInactive = false } = {}) {
  const cookieStore = await cookies();
  const token = cookieStore.get('auth_token')?.value;
  if (!token) return null;
  const payload = verifyAuthToken(token);
  if (!payload?.id) return null;
  if (payload?.scope === 'backend') return null;
  const admin = await getAdminById(payload.id);
  if (!admin) return null;
  if (isAccessExpired(admin.access_expires_at)) return null;
  if (!allowInactive && admin.status !== 'active') return null;
  const accessGrantActive = isAccessGrantActive(admin.access_expires_at);
  let extras = {};
  try {
    const billing = await getAdminBillingSettings(admin.id);
    const chargeEnabled = billing?.dashboard_charge_enabled !== false;
    const freeUntil = billing?.free_until ? new Date(billing.free_until) : null;
    const freeUntilActive =
      freeUntil && !Number.isNaN(freeUntil.getTime()) && freeUntil > new Date();
    const hasExpiry =
      admin.dashboard_subscription_expires_at &&
      new Date(admin.dashboard_subscription_expires_at).getTime() > Date.now();
    const dashboardActive =
      admin.admin_tier === 'super_admin' ||
      accessGrantActive ||
      freeUntilActive ||
      !chargeEnabled ||
      Boolean(hasExpiry);
    extras = {
      dashboard_charge_enabled: chargeEnabled,
      dashboard_subscription_active: dashboardActive,
      dashboard_free_until_active: freeUntilActive,
      dashboard_access_grant_active: accessGrantActive,
      restricted_mode: admin.status !== 'active' || !dashboardActive,
    };
  } catch {
    extras = { restricted_mode: admin.status !== 'active' };
  }
  return buildAuthUser(admin, extras);
}

export async function getSessionUser() {
  return getAuthUser({ allowInactive: true });
}

export async function requireAuth() {
  const user = await getAuthUser();
  if (!user) {
    const error = new Error('Unauthorized');
    error.status = 401;
    throw error;
  }
  return user;
}

export async function requireReadAuth() {
  const user = await getAuthUser({ allowInactive: true });
  if (!user) {
    const error = new Error('Unauthorized');
    error.status = 401;
    throw error;
  }
  return user;
}

export async function requireDashboardAuth() {
  const user = await getSessionUser();
  if (!user) {
    const error = new Error('Unauthorized');
    error.status = 401;
    throw error;
  }
  if (user.admin_tier !== 'super_admin' && user.dashboard_subscription_active === false) {
    const error = new Error('Dashboard access requires an active plan or access grant.');
    error.status = 403;
    throw error;
  }
  return user;
}

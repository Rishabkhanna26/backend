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
  free_delivery_product_rules: admin.free_delivery_product_rules,
  two_factor_enabled: admin.two_factor_enabled === true,
  access_expires_at: admin.access_expires_at,
  ...extras,
});

export async function getAuthUser({ allowInactive = false, allowExpired = false } = {}) {
  const cookieStore = await cookies();
  const token = cookieStore.get('auth_token')?.value;
  if (!token) return null;
  const payload = verifyAuthToken(token);
  if (!payload?.id) return null;
  if (payload?.scope === 'backend') return null;
  const admin = await getAdminById(payload.id);
  if (!admin) return null;
  // Allow authentication even if subscription is expired - just mark it as expired
  // Do NOT check isAccessExpired here - that was blocking users from logging in
  if (!allowInactive && admin.status !== 'active') return null;
  const accessGrantActive = isAccessGrantActive(admin.access_expires_at);
  let extras = {};
  try {
    const billing = await getAdminBillingSettings(admin.id);
    const chargeEnabled = billing?.dashboard_charge_enabled !== false;
    const tokenSystemEnabled =
      admin.admin_tier === 'super_admin' || billing?.charge_enabled === true;
    const freeUntil = billing?.free_until ? new Date(billing.free_until) : null;
    const freeUntilActive =
      freeUntil && !Number.isNaN(freeUntil.getTime()) && freeUntil > new Date();
    const subscriptionExpiresAt = admin.dashboard_subscription_expires_at 
      ? new Date(admin.dashboard_subscription_expires_at) 
      : null;
    const hasValidSubscription =
      subscriptionExpiresAt && subscriptionExpiresAt.getTime() > Date.now();
    const subscriptionExpired =
      chargeEnabled && 
      subscriptionExpiresAt && 
      subscriptionExpiresAt.getTime() <= Date.now();
    const dashboardActive =
      admin.admin_tier === 'super_admin' ||
      accessGrantActive ||
      freeUntilActive ||
      !chargeEnabled ||
      Boolean(hasValidSubscription);
    extras = {
      dashboard_charge_enabled: chargeEnabled,
      token_system_enabled: tokenSystemEnabled,
      dashboard_subscription_active: dashboardActive,
      dashboard_subscription_expired: Boolean(subscriptionExpired),
      dashboard_subscription_expires_at: admin.dashboard_subscription_expires_at,
      dashboard_free_until_active: freeUntilActive,
      dashboard_access_grant_active: accessGrantActive,
      restricted_mode: admin.status !== 'active' || !dashboardActive,
    };
  } catch {
    extras = {
      token_system_enabled: admin.admin_tier === 'super_admin',
      restricted_mode: admin.status !== 'active',
    };
  }
  return buildAuthUser(admin, extras);
}

export async function getSessionUser() {
  return getAuthUser({ allowInactive: true, allowExpired: true });
}

export async function requireAuth() {
  const user = await getAuthUser({ allowInactive: true });
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
  // Allow users to view even with expired subscription, but mark them as expired
  // They cannot perform write operations (checked separately in endpoints)
  return user;
}

export async function requireDashboardAuthActive() {
  const user = await getSessionUser();
  if (!user) {
    const error = new Error('Unauthorized');
    error.status = 401;
    throw error;
  }
  if (user.admin_tier !== 'super_admin' && user.dashboard_subscription_active === false && !user.dashboard_subscription_expired) {
    const error = new Error('Dashboard access requires an active plan or access grant.');
    error.status = 403;
    throw error;
  }
  return user;
}

export function checkSubscriptionStatus(user) {
  if (!user) {
    return { allowed: false, status: 'unauthorized', message: 'Unauthorized' };
  }
  if (user.admin_tier === 'super_admin') {
    return { allowed: true, status: 'active', message: 'Super admin access' };
  }
  if (String(user.status || '').toLowerCase() !== 'active') {
    return {
      allowed: false,
      status: 'inactive',
      message: 'Your access period is over. Please contact super admin to reactivate your account.',
      viewOnly: true,
    };
  }
  if (user.dashboard_subscription_expired) {
    return { 
      allowed: false, 
      status: 'expired', 
      message: 'Your subscription has expired. Please contact the super admin to reactivate your account or purchase a new subscription.',
      viewOnly: true
    };
  }
  if (!user.dashboard_subscription_active) {
    return { 
      allowed: false, 
      status: 'inactive', 
      message: 'Dashboard access requires an active subscription. Please contact the super admin to reactivate your account or purchase a subscription.',
      viewOnly: true
    };
  }
  return { allowed: true, status: 'active', message: 'Access granted' };
}

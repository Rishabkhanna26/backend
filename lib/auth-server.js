import { cookies } from 'next/headers';
import { verifyAuthToken } from './auth';
import { getAdminById } from './db-helpers';

const isAccessExpired = (value) => {
  if (!value) return false;
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return false;
  return time <= Date.now();
};

const buildAuthUser = (admin) => ({
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
  booking_enabled: admin.booking_enabled,
  business_address: admin.business_address,
  business_hours: admin.business_hours,
  business_map_url: admin.business_map_url,
  free_delivery_enabled: admin.free_delivery_enabled,
  free_delivery_min_amount: admin.free_delivery_min_amount,
  free_delivery_scope: admin.free_delivery_scope,
  access_expires_at: admin.access_expires_at,
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
  return buildAuthUser(admin);
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

export async function requireDashboardAuth() {
  const user = await getSessionUser();
  if (!user) {
    const error = new Error('Unauthorized');
    error.status = 401;
    throw error;
  }
  return user;
}

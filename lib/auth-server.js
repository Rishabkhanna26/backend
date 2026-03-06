import { cookies } from 'next/headers';
import { verifyAuthToken } from './auth';
import { getAdminById } from './db-helpers';

export async function getAuthUser() {
  const cookieStore = await cookies();
  const token = cookieStore.get('auth_token')?.value;
  if (!token) return null;
  const payload = verifyAuthToken(token);
  if (!payload?.id) return null;
  if (payload?.scope === 'backend') return null;
  const admin = await getAdminById(payload.id);
  if (!admin) return null;
  if (admin.status !== 'active') return null;

  return {
    id: admin.id,
    name: admin.name,
    email: admin.email,
    phone: admin.phone,
    admin_tier: admin.admin_tier,
    status: admin.status,
    business_category: admin.business_category,
    business_type: admin.business_type,
    access_expires_at: admin.access_expires_at,
  };
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

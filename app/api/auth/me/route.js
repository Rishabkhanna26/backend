import { NextResponse } from 'next/server';
import { getAuthUser } from '../../../../lib/auth-server';
import { getAdminById } from '../../../../lib/db-helpers';

export async function GET() {
  const user = await getAuthUser();
  if (!user) {
    return NextResponse.json({ user: null }, { status: 401 });
  }

  const admin = await getAdminById(user.id);
  if (!admin) {
    const response = NextResponse.json({ user: null }, { status: 401 });
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

  return NextResponse.json({
    user: {
      id: admin.id,
      name: admin.name,
      email: admin.email,
      phone: admin.phone,
      admin_tier: admin.admin_tier,
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
      status: admin.status,
      access_expires_at: admin.access_expires_at,
    },
  });
}

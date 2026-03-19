import { NextResponse } from 'next/server';
import { getSessionUser } from '../../../../lib/auth-server';

export async function GET() {
  const user = await getSessionUser();
  if (!user) {
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
      id: user.id,
      name: user.name,
      email: user.email,
      phone: user.phone,
      admin_tier: user.admin_tier,
      business_name: user.business_name,
      business_category: user.business_category,
      business_type: user.business_type,
      service_label: user.service_label,
      product_label: user.product_label,
      booking_enabled: user.booking_enabled,
      dashboard_charge_enabled: user.dashboard_charge_enabled,
      dashboard_subscription_active: user.dashboard_subscription_active,
      dashboard_subscription_expires_at: user.dashboard_subscription_expires_at,
      dashboard_free_until_active: user.dashboard_free_until_active,
      dashboard_access_grant_active: user.dashboard_access_grant_active,
      business_address: user.business_address,
      business_hours: user.business_hours,
      business_map_url: user.business_map_url,
      free_delivery_enabled: user.free_delivery_enabled,
      free_delivery_min_amount: user.free_delivery_min_amount,
      free_delivery_scope: user.free_delivery_scope,
      status: user.status,
      restricted_mode: user.restricted_mode,
      access_expires_at: user.access_expires_at,
    },
  });
}

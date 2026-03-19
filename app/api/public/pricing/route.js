import { NextResponse } from 'next/server';
import { getDashboardChargeRates } from '../../../../lib/db-helpers';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const rates = await getDashboardChargeRates();
    return NextResponse.json({
      success: true,
      data: {
        currency: 'INR',
        unit: 'month',
        service_inr: Number(rates?.service_inr || 0),
        product_inr: Number(rates?.product_inr || 0),
        both_inr: Number(rates?.both_inr || 0),
        booking_inr: Number(rates?.booking_inr || 0),
      },
    });
  } catch (error) {
    return NextResponse.json(
      { success: false, error: error.message || 'Failed to load pricing.' },
      { status: 500 }
    );
  }
}

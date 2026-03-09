import { requireAuth } from '../../../../lib/auth-server';

export async function POST(req) {
  try {
    await requireAuth();
    return Response.json(
      {
        success: false,
        error:
          'Business type changes are now managed directly by super admin from the Admins panel.',
      },
      { status: 410 }
    );
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

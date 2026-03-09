import { NextResponse } from 'next/server';
import { requireAuth } from '../../../../lib/auth-server';
import { getConnection } from '../../../../lib/db-helpers';
import { hashPassword, verifyPassword } from '../../../../lib/auth';

export async function POST(request) {
  try {
    const user = await requireAuth();
    const body = await request.json();
    const currentPassword = String(body?.currentPassword || '');
    const newPassword = String(body?.newPassword || '');

    if (!newPassword || newPassword.length < 8) {
      return NextResponse.json(
        { success: false, error: 'New password must be at least 8 characters.' },
        { status: 400 }
      );
    }

    const connection = await getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT password_hash FROM admins WHERE id = ? LIMIT 1`,
        [user.id]
      );

      if (!rows || rows.length === 0) {
        const response = NextResponse.json(
          { success: false, error: 'User not found' },
          { status: 401 }
        );
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

      const storedHash = rows[0]?.password_hash;
      if (storedHash) {
        const isValid = verifyPassword(currentPassword, storedHash);
        if (!isValid) {
          return NextResponse.json(
            { success: false, error: 'Current password is incorrect.' },
            { status: 400 }
          );
        }
      } else if (!currentPassword) {
        // No password on record yet; allow setting a new one.
      }

      const passwordHash = hashPassword(newPassword);
      await connection.execute(
        `UPDATE admins SET password_hash = ? WHERE id = ?`,
        [passwordHash, user.id]
      );

      return NextResponse.json({ success: true });
    } finally {
      connection.release();
    }
  } catch (error) {
    if (error.status === 401) {
      return NextResponse.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}

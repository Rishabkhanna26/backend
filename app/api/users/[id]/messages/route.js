import {
  deleteMessagesOlderThan,
  getMessagesForUser,
  getUserById,
  markMessagesRead,
} from '../../../../../lib/db-helpers';
import { parsePagination } from '../../../../../lib/api-utils';
import { requireAuth } from '../../../../../lib/auth-server';
import { signAuthToken } from '../../../../../lib/auth';

export const runtime = 'nodejs';

const WHATSAPP_API_BASE =
  process.env.WHATSAPP_API_BASE || 'http://localhost:3001';
const BACKEND_TOKEN_TTL_SECONDS = 10 * 60;

export async function GET(req, context) {
  try {
    const authUser = await requireAuth();
    await deleteMessagesOlderThan(15);
    const { searchParams } = new URL(req.url);
    const { limit, offset } = parsePagination(searchParams, { defaultLimit: 50, maxLimit: 200 });
    const beforeRaw = searchParams.get('before');
    const beforeDate = beforeRaw ? new Date(beforeRaw) : null;
    const before = beforeDate && !Number.isNaN(beforeDate.getTime()) ? beforeDate.toISOString() : null;
    const params = await context.params;
    const userId = Number(params?.id);
    if (!Number.isFinite(userId)) {
      return Response.json({ success: false, error: 'Invalid user id' }, { status: 400 });
    }

    const shouldMarkRead = offset === 0 && !before;
    if (shouldMarkRead) {
      await markMessagesRead(
        userId,
        authUser.id
      );
    }

    const messages = await getMessagesForUser(
      userId,
      authUser.id,
      {
        limit: limit + 1,
        offset,
        before,
      }
    );
    const hasMore = messages.length > limit;
    const data = hasMore ? messages.slice(0, limit) : messages;
    const response = Response.json({
      success: true,
      data,
      meta: {
        limit,
        offset,
        hasMore,
        nextOffset: hasMore ? offset + limit : null,
      },
    });
    response.headers.set('Cache-Control', 'private, max-age=5, stale-while-revalidate=15');
    return response;
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function POST(req, context) {
  try {
    const authUser = await requireAuth();
    const params = await context.params;
    const userId = Number(params?.id);
    if (!Number.isFinite(userId)) {
      return Response.json({ success: false, error: 'Invalid user id' }, { status: 400 });
    }

    const body = await req.json().catch(() => ({}));
    const message = String(body?.message || '').trim();
    if (!message) {
      return Response.json({ success: false, error: 'Message is required' }, { status: 400 });
    }

    const user = await getUserById(userId, authUser.id);
    if (!user) {
      return Response.json({ success: false, error: 'Contact not found' }, { status: 404 });
    }

    let payload;
    try {
      const backendToken = signAuthToken(
        {
          id: authUser.id,
          admin_tier: authUser.admin_tier,
          scope: 'backend',
        },
        { expiresIn: `${BACKEND_TOKEN_TTL_SECONDS}s` }
      );
      const whatsappResponse = await fetch(`${WHATSAPP_API_BASE}/whatsapp/send`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${backendToken}`,
        },
        body: JSON.stringify({
          adminId: authUser.id,
          userId,
          message,
        }),
      });
      payload = await whatsappResponse.json().catch(() => null);
      if (!whatsappResponse.ok || payload?.success === false) {
        const errorMessage = payload?.error || 'Failed to send message via WhatsApp';
        return Response.json({ success: false, error: errorMessage }, { status: whatsappResponse.status || 500 });
      }
    } catch (error) {
      return Response.json({ success: false, error: 'WhatsApp service unavailable' }, { status: 502 });
    }

    return Response.json({
      success: true,
      data: {
        id: payload?.data?.id || payload?.data?.messageId || null,
        user_id: userId,
        admin_id: authUser.id,
        message_text: message,
        message_type: 'outgoing',
        status: payload?.data?.status || 'sent',
        created_at: payload?.data?.created_at || new Date().toISOString(),
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

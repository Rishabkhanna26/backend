import { requireAuth } from '../../../lib/auth-server';
import { createTemplate, getAllTemplates } from '../../../lib/db-helpers';
import { parsePagination, parseSearch } from '../../../lib/api-utils';

export async function GET(request) {
  try {
    const authUser = await requireAuth();
    const { searchParams } = new URL(request.url);
    const { limit, offset } = parsePagination(searchParams);
    const search = parseSearch(searchParams);
    const templates = await getAllTemplates(
      authUser.admin_tier === 'super_admin' ? null : authUser.id,
      { search, limit: limit + 1, offset }
    );
    const hasMore = templates.length > limit;
    const data = hasMore ? templates.slice(0, limit) : templates;
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
    response.headers.set('Cache-Control', 'private, max-age=30, stale-while-revalidate=120');
    return response;
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function POST(request) {
  try {
    const user = await requireAuth();
    const body = await request.json();
    const name = String(body?.name || '').trim();
    const category = String(body?.category || '').trim();
    const content = String(body?.content || '').trim();
    const variables = Array.isArray(body?.variables) ? body.variables : [];

    if (!name || !category || !content) {
      return Response.json(
        { success: false, error: 'Name, category, and content are required' },
        { status: 400 }
      );
    }

    const template = await createTemplate({
      name,
      category,
      content,
      variables,
      createdBy: user.id,
    });

    return Response.json({ success: true, data: template });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

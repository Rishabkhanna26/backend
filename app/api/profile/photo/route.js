import fs from 'fs/promises';
import path from 'path';
import { requireAuth } from '../../../../lib/auth-server';
import { protectModificationAction } from '../../../../lib/api-protection';

export const runtime = 'nodejs';

const ALLOWED_TYPES = new Map([
  ['image/jpeg', 'jpg'],
  ['image/png', 'png'],
  ['image/webp', 'webp'],
]);

const getSafeFolder = (value) => String(value || '').replace(/\D/g, '');

const removeExistingProfiles = async (folderPath, keepExt) => {
  const extensions = ['jpg', 'jpeg', 'png', 'webp'];
  await Promise.all(
    extensions.map(async (ext) => {
      if (ext === keepExt) return;
      const filePath = path.join(folderPath, `profile.${ext}`);
      try {
        await fs.unlink(filePath);
      } catch (error) {
        // Ignore if missing
      }
    })
  );
};

export async function POST(request) {
  try {
    const user = await requireAuth();
    const guard = await protectModificationAction(user, 'update');
    if (guard) return guard;
    const formData = await request.formData();
    const file = formData.get('photo');

    if (!file || typeof file.arrayBuffer !== 'function') {
      return Response.json({ success: false, error: 'Photo file is required.' }, { status: 400 });
    }

    const fileType = file.type || '';
    const ext = ALLOWED_TYPES.get(fileType);
    if (!ext) {
      return Response.json({ success: false, error: 'Only JPG, PNG, or WEBP images are allowed.' }, { status: 400 });
    }

    const folderName = getSafeFolder(user.phone || user.id);
    if (!folderName) {
      return Response.json({ success: false, error: 'Unable to determine admin folder.' }, { status: 400 });
    }

    const publicDir = path.join(process.cwd(), 'public');
    const adminDir = path.join(publicDir, folderName);
    await fs.mkdir(adminDir, { recursive: true });

    const buffer = Buffer.from(await file.arrayBuffer());
    const filename = `profile.${ext}`;
    const filePath = path.join(adminDir, filename);
    await fs.writeFile(filePath, buffer);
    await removeExistingProfiles(adminDir, ext);

    return Response.json({
      success: true,
      url: new URL(`/${folderName}/${filename}?v=${Date.now()}`, request.url).toString(),
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

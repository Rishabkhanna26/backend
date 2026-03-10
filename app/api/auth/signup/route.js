import { NextResponse } from 'next/server';
import crypto from 'crypto';
import { getConnection } from '../../../../lib/db-helpers';
import { hashPassword, signAuthToken } from '../../../../lib/auth';
import { sanitizeEmail, sanitizeNameUpper, sanitizePhone, sanitizeText } from '../../../../lib/sanitize.js';
import { consumeRateLimit, getClientIp } from '../../../../lib/rate-limit';
import { buildGmailTransporterFromEnv, normalizeSmtpEmail } from '../../../../lib/mailer.js';

export const runtime = 'nodejs';

const SIGNUP_CODE_TTL_MINUTES = 10;
const MAX_CODE_ATTEMPTS = 5;
const OTP_LENGTH = 6;
const SIGNUP_REQUEST_WINDOW_MS = 15 * 60 * 1000;
const SIGNUP_REQUEST_MAX_ATTEMPTS = 5;
const SIGNUP_VERIFY_WINDOW_MS = 15 * 60 * 1000;
const SIGNUP_VERIFY_MAX_ATTEMPTS = 20;

function buildTransporter() {
  return buildGmailTransporterFromEnv();
}

function normalizeSignupPayload(body) {
  const businessTypeRaw = typeof body.business_type === 'string' ? body.business_type.trim().toLowerCase() : '';
  const allowedBusinessTypes = new Set(['product', 'service', 'both']);
  return {
    name: sanitizeNameUpper(body.name),
    email: sanitizeEmail(body.email),
    phone: sanitizePhone(body.phone),
    password: String(body.password || ''),
    businessCategory: sanitizeText(body.business_category, 120),
    businessType: allowedBusinessTypes.has(businessTypeRaw) ? businessTypeRaw : 'both',
  };
}

function createVerificationCode() {
  const min = 10 ** (OTP_LENGTH - 1);
  const max = 10 ** OTP_LENGTH;
  return String(crypto.randomInt(min, max));
}

function hashVerificationCode(email, code) {
  return crypto
    .createHash('sha256')
    .update(`${String(email || '').toLowerCase()}:${code}:${process.env.JWT_SECRET || 'algochat-signup-code'}`)
    .digest('hex');
}

function formatNameForEmail(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildVerificationEmail({ name, code }) {
  const displayName = formatNameForEmail(name) || 'there';
  const safeDisplayName = escapeHtml(displayName);
  const safeCode = escapeHtml(code);
  const subject = 'Verify your email to complete AlgoChat signup';
  const text = [
    `Hello ${displayName},`,
    '',
    'Use the verification code below to complete your AlgoChat account setup:',
    code,
    '',
    `This code will expire in ${SIGNUP_CODE_TTL_MINUTES} minutes.`,
    '',
    'If you did not start this request, please ignore this email.',
    '',
    'Thanks,',
    'AlgoChat Team',
  ].join('\n');
  const html = `
    <div style="font-family:Arial,sans-serif;background:#f5f8ff;padding:24px;color:#0f172a;">
      <div style="max-width:560px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:14px;overflow:hidden;">
        <div style="background:linear-gradient(135deg,#0a1f44,#ff6b00);padding:18px 20px;">
          <h1 style="margin:0;color:#ffffff;font-size:20px;">AlgoChat CRM</h1>
        </div>
        <div style="padding:22px 20px;">
          <p style="margin:0 0 14px;">Hello ${safeDisplayName},</p>
          <p style="margin:0 0 18px;">Use this verification code to complete your signup:</p>
          <div style="margin:0 0 18px;padding:12px 14px;border-radius:10px;background:#fff7ed;border:1px solid #fed7aa;font-size:28px;font-weight:700;letter-spacing:6px;text-align:center;color:#9a3412;">
            ${safeCode}
          </div>
          <p style="margin:0 0 12px;color:#475569;">This code expires in ${SIGNUP_CODE_TTL_MINUTES} minutes.</p>
          <p style="margin:0;color:#64748b;font-size:13px;">If you did not start this request, you can safely ignore this email.</p>
        </div>
      </div>
    </div>
  `;
  return { subject, text, html };
}

function buildWelcomeEmail({ name, userId, status, loginUrl, businessCategory, businessType }) {
  const displayName = formatNameForEmail(name) || 'there';
  const safeDisplayName = escapeHtml(displayName);
  const safeUserId = escapeHtml(userId);
  const safeBusinessCategory = escapeHtml(businessCategory || 'General');
  const safeBusinessType = escapeHtml(businessType || 'both');
  const safeLoginUrl = escapeHtml(loginUrl);
  const isActive = status === 'active';
  const subject = isActive
    ? 'Welcome to AlgoChat CRM - Your account is ready'
    : 'AlgoChat signup received - account pending activation';
  const text = [
    `Hello ${displayName},`,
    '',
    'Your AlgoChat account has been created successfully.',
    `User ID: ${userId}`,
    `Business Category: ${businessCategory || 'General'}`,
    `Business Type: ${businessType || 'both'}`,
    '',
    isActive
      ? `You can sign in now: ${loginUrl}`
      : 'Your account is pending super admin activation. You will be able to sign in once approved.',
    '',
    'Thanks,',
    'AlgoChat Team',
  ].join('\n');
  const html = `
    <div style="font-family:Arial,sans-serif;background:#f5f8ff;padding:24px;color:#0f172a;">
      <div style="max-width:560px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:14px;overflow:hidden;">
        <div style="background:linear-gradient(135deg,#0a1f44,#ff6b00);padding:18px 20px;">
          <h1 style="margin:0;color:#ffffff;font-size:20px;">AlgoChat CRM</h1>
        </div>
        <div style="padding:22px 20px;">
          <p style="margin:0 0 14px;">Hello ${safeDisplayName},</p>
          <p style="margin:0 0 18px;">Your account has been created successfully.</p>
          <table style="width:100%;border-collapse:collapse;margin:0 0 18px;">
            <tr>
              <td style="padding:8px;border:1px solid #e2e8f0;background:#f8fafc;font-weight:600;">User ID</td>
              <td style="padding:8px;border:1px solid #e2e8f0;">${safeUserId}</td>
            </tr>
            <tr>
              <td style="padding:8px;border:1px solid #e2e8f0;background:#f8fafc;font-weight:600;">Business Category</td>
              <td style="padding:8px;border:1px solid #e2e8f0;">${safeBusinessCategory}</td>
            </tr>
            <tr>
              <td style="padding:8px;border:1px solid #e2e8f0;background:#f8fafc;font-weight:600;">Business Type</td>
              <td style="padding:8px;border:1px solid #e2e8f0;">${safeBusinessType}</td>
            </tr>
          </table>
          ${
            isActive
              ? `<p style="margin:0 0 16px;">Your account is active. You can sign in now:</p>
                 <p style="margin:0;"><a href="${safeLoginUrl}" style="color:#ea580c;font-weight:700;text-decoration:none;">Go to Login</a></p>`
              : '<p style="margin:0;color:#475569;">Your account is currently pending super admin activation. You will be able to sign in once approved.</p>'
          }
        </div>
      </div>
    </div>
  `;
  return { subject, text, html };
}

async function findExistingAccountConflicts(connection, { phone, email }) {
  const [existing] = await connection.execute(
    `SELECT id, phone, email
     FROM admins
     WHERE phone = ?
        OR regexp_replace(phone, '\\D', '', 'g') = ?
        OR LOWER(email) = ?`,
    [phone, phone, String(email || '').toLowerCase()]
  );

  if (!existing.length) {
    return null;
  }

  const phoneExists = existing.some((row) => sanitizePhone(row.phone) === phone);
  const emailExists = email ? existing.some((row) => sanitizeEmail(row.email) === email) : false;

  return {
    phone: phoneExists,
    email: emailExists,
  };
}

function conflictResponse(fields) {
  return NextResponse.json(
    {
      error: 'An account with this phone or email already exists',
      fields,
    },
    { status: 409 }
  );
}

export async function POST(request) {
  try {
    const body = await request.json();
    const action = String(body?.action || 'request_code').trim().toLowerCase();
    const payload = normalizeSignupPayload(body);
    const clientIp = getClientIp(request);

    if (action === 'verify_code') {
      const verificationCode = String(body?.verification_code || body?.code || '').trim();
      if (!payload.email || !verificationCode) {
        return NextResponse.json({ error: 'Email and verification code are required' }, { status: 400 });
      }

      const verifyRateLimit = consumeRateLimit({
        bucket: 'auth_signup_verify',
        key: `${clientIp}:${payload.email}`,
        max: SIGNUP_VERIFY_MAX_ATTEMPTS,
        windowMs: SIGNUP_VERIFY_WINDOW_MS,
      });
      if (!verifyRateLimit.allowed) {
        return NextResponse.json(
          { error: 'Too many verification attempts. Please try again later.' },
          {
            status: 429,
            headers: {
              'Retry-After': String(Math.ceil(verifyRateLimit.retryAfterMs / 1000)),
            },
          }
        );
      }

      const connection = await getConnection();
      try {
        let createdAdmin = null;
        try {
          await connection.query('BEGIN');

          const [pendingRows] = await connection.query(
            `SELECT email, code_hash, payload_json, attempts, expires_at
             FROM signup_verifications
             WHERE email = ?
             FOR UPDATE`,
            [payload.email]
          );

          if (!pendingRows.length) {
            await connection.query('ROLLBACK');
            return NextResponse.json(
              { error: 'Verification session not found. Please request a new code.' },
              { status: 400 }
            );
          }

          const pending = pendingRows[0];
          const expiresAt = new Date(pending.expires_at);
          if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() < Date.now()) {
            await connection.query(`DELETE FROM signup_verifications WHERE email = ?`, [payload.email]);
            await connection.query('COMMIT');
            return NextResponse.json(
              { error: 'Verification code expired. Please request a new code.' },
              { status: 400 }
            );
          }

          const expectedHash = hashVerificationCode(payload.email, verificationCode);
          if (expectedHash !== pending.code_hash) {
            const attempts = Number(pending.attempts || 0) + 1;
            if (attempts >= MAX_CODE_ATTEMPTS) {
              await connection.query(`DELETE FROM signup_verifications WHERE email = ?`, [payload.email]);
              await connection.query('COMMIT');
              return NextResponse.json(
                { error: 'Too many invalid attempts. Please request a new code.' },
                { status: 400 }
              );
            }

            await connection.query(
              `UPDATE signup_verifications
               SET attempts = ?, updated_at = NOW()
               WHERE email = ?`,
              [attempts, payload.email]
            );
            await connection.query('COMMIT');
            return NextResponse.json(
              { error: `Invalid verification code. ${MAX_CODE_ATTEMPTS - attempts} attempts left.` },
              { status: 400 }
            );
          }

          const pendingPayload = pending.payload_json || {};
          const name = sanitizeNameUpper(pendingPayload.name);
          const phone = sanitizePhone(pendingPayload.phone);
          const email = sanitizeEmail(pendingPayload.email);
          const businessCategory = sanitizeText(pendingPayload.business_category, 120);
          const businessTypeRaw =
            typeof pendingPayload.business_type === 'string'
              ? pendingPayload.business_type.trim().toLowerCase()
              : '';
          const passwordHash = String(pendingPayload.password_hash || '').trim();
          const businessType = new Set(['product', 'service', 'both']).has(businessTypeRaw)
            ? businessTypeRaw
            : 'both';

          if (!name || !email || !phone || !passwordHash || !businessCategory) {
            await connection.query(`DELETE FROM signup_verifications WHERE email = ?`, [payload.email]);
            await connection.query('COMMIT');
            return NextResponse.json(
              { error: 'Signup data is invalid. Please submit signup form again.' },
              { status: 400 }
            );
          }

          const conflict = await findExistingAccountConflicts(connection, { phone, email });
          if (conflict) {
            await connection.query(`DELETE FROM signup_verifications WHERE email = ?`, [payload.email]);
            await connection.query('COMMIT');
            return conflictResponse(conflict);
          }

          const [superAdmins] = await connection.execute(
            `SELECT COUNT(*) as count FROM admins WHERE admin_tier = 'super_admin'`
          );
          const hasSuperAdmin = Number(superAdmins[0]?.count || 0) > 0;
          const adminTier = hasSuperAdmin ? 'client_admin' : 'super_admin';
          const status = hasSuperAdmin ? 'inactive' : 'active';

          const [rows] = await connection.query(
            `INSERT INTO admins (
                name, phone, email, password_hash, admin_tier, status,
                business_category, business_type
             )
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING id`,
            [name, phone, email, passwordHash, adminTier, status, businessCategory, businessType]
          );

          await connection.query(`DELETE FROM signup_verifications WHERE email = ?`, [payload.email]);
          await connection.query('COMMIT');

          createdAdmin = {
            id: rows[0]?.id,
            name,
            email,
            phone,
            admin_tier: adminTier,
            status,
            business_category: businessCategory,
            business_type: businessType,
          };
        } catch (txError) {
          try {
            await connection.query('ROLLBACK');
          } catch (_) {}
          throw txError;
        }

        if (!createdAdmin) {
          return NextResponse.json({ error: 'Failed to create account' }, { status: 500 });
        }

        const response = NextResponse.json({
          user: createdAdmin,
          requires_activation: createdAdmin.status !== 'active',
        });

        if (createdAdmin.status === 'active') {
          const token = signAuthToken({
            id: createdAdmin.id,
            name: createdAdmin.name,
            email: createdAdmin.email,
            phone: createdAdmin.phone,
            admin_tier: createdAdmin.admin_tier,
            status: createdAdmin.status,
            business_category: createdAdmin.business_category,
            business_type: createdAdmin.business_type,
          });

          response.cookies.set({
            name: 'auth_token',
            value: token,
            httpOnly: true,
            sameSite: 'lax',
            path: '/',
            maxAge: 60 * 60 * 24 * 7,
            secure: process.env.NODE_ENV === 'production',
          });
        }

        const transporter = buildTransporter();
        if (transporter) {
          const smtpFrom = normalizeSmtpEmail(process.env.SMTP_EMAIL);
          const loginUrl = new URL(
            '/login',
            process.env.FRONTEND_URL || process.env.FRONTEND_ORIGIN || request.url
          ).toString();
          const welcomeMail = buildWelcomeEmail({
            name: createdAdmin.name,
            userId: createdAdmin.id,
            status: createdAdmin.status,
            loginUrl,
            businessCategory: createdAdmin.business_category,
            businessType: createdAdmin.business_type,
          });
          try {
            await transporter.sendMail({
              from: smtpFrom ? `"AlgoChat CRM" <${smtpFrom}>` : undefined,
              to: createdAdmin.email,
              subject: welcomeMail.subject,
              text: welcomeMail.text,
              html: welcomeMail.html,
            });
          } catch (mailError) {
            console.error('Signup welcome email error:', mailError);
          }
        }

        return response;
      } finally {
        connection.release();
      }
    }

    const { name, email, phone, password, businessCategory, businessType } = payload;
    if (!name || !email || !phone || !password || !businessCategory) {
      return NextResponse.json(
        { error: 'Valid name, email, phone, password, and business category are required' },
        { status: 400 }
      );
    }

    const signupRateLimit = consumeRateLimit({
      bucket: 'auth_signup_request',
      key: `${clientIp}:${email}:${phone}`,
      max: SIGNUP_REQUEST_MAX_ATTEMPTS,
      windowMs: SIGNUP_REQUEST_WINDOW_MS,
    });
    if (!signupRateLimit.allowed) {
      return NextResponse.json(
        { error: 'Too many signup requests. Please try again later.' },
        {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil(signupRateLimit.retryAfterMs / 1000)),
          },
        }
      );
    }

    const transporter = buildTransporter();
    if (!transporter) {
      return NextResponse.json(
        { error: 'SMTP is not configured. Please set SMTP_EMAIL and SMTP_PASSWORD.' },
        { status: 500 }
      );
    }

    const smtpFrom = normalizeSmtpEmail(process.env.SMTP_EMAIL);
    const connection = await getConnection();
    try {
      const conflict = await findExistingAccountConflicts(connection, { phone, email });
      if (conflict) {
        return conflictResponse(conflict);
      }

      const verificationCode = createVerificationCode();
      const codeHash = hashVerificationCode(email, verificationCode);
      const expiresAt = new Date(Date.now() + SIGNUP_CODE_TTL_MINUTES * 60 * 1000).toISOString();
      const signupPayload = JSON.stringify({
        name,
        email,
        phone,
        business_category: businessCategory,
        business_type: businessType,
        password_hash: hashPassword(password),
      });

      await connection.query(
        `INSERT INTO signup_verifications (email, code_hash, payload_json, attempts, expires_at)
         VALUES (?, ?, ?::jsonb, 0, ?)
         ON CONFLICT (email)
         DO UPDATE SET
           code_hash = EXCLUDED.code_hash,
           payload_json = EXCLUDED.payload_json,
           attempts = 0,
           expires_at = EXCLUDED.expires_at,
           updated_at = NOW()`,
        [email, codeHash, signupPayload, expiresAt]
      );
      await connection.query(`DELETE FROM signup_verifications WHERE expires_at < NOW()`);

      const verificationMail = buildVerificationEmail({ name, code: verificationCode });
      await transporter.sendMail({
        from: smtpFrom ? `"AlgoChat CRM" <${smtpFrom}>` : undefined,
        to: email,
        subject: verificationMail.subject,
        text: verificationMail.text,
        html: verificationMail.html,
      });

      return NextResponse.json({
        success: true,
        verification_required: true,
        email,
        expires_in_minutes: SIGNUP_CODE_TTL_MINUTES,
      });
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Signup error:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}

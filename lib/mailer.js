import nodemailer from 'nodemailer';

export function normalizeSmtpEmail(value) {
  return String(value || '').trim();
}

export function normalizeSmtpPassword(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  const unquoted =
    (raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith("'") && raw.endsWith("'"))
      ? raw.slice(1, -1)
      : raw;

  return unquoted.replace(/\s+/g, '');
}

export function buildGmailTransporterFromEnv() {
  const user = normalizeSmtpEmail(process.env.SMTP_EMAIL);
  const pass = normalizeSmtpPassword(process.env.SMTP_PASSWORD);
  if (!user || !pass) return null;

  return nodemailer.createTransport({
    service: 'gmail',
    auth: { user, pass },
  });
}


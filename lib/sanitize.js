const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/i;

export const sanitizePhone = (value, { min = 7, max = 15 } = {}) => {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length < min) return "";
  if (digits.length > max) return digits.slice(-max);
  return digits;
};

export const sanitizeEmail = (value) => {
  const email = String(value || "").trim().toLowerCase();
  if (!email) return null;
  return EMAIL_REGEX.test(email) ? email : null;
};

export const sanitizeNameUpper = (value) => {
  const cleaned = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return null;
  return cleaned.toUpperCase();
};

export const sanitizeText = (value, maxLength = 2000) => {
  const cleaned = String(value || "").replace(/\s+/g, " ").trim();
  if (!cleaned) return "";
  if (!Number.isFinite(maxLength) || maxLength <= 0) return cleaned;
  return cleaned.slice(0, maxLength);
};

const toCleanText = (value, maxLength = 500) =>
  String(value || "")
    .replace(/\r/g, "")
    .trim()
    .slice(0, Math.max(Number(maxLength) || 0, 0));

const toOptionalUrl = (value) => {
  const raw = toCleanText(value, 500);
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) return "";
    return parsed.toString();
  } catch {
    return "";
  }
};

export const normalizeBusinessInfo = (profile = {}) => ({
  brandName:
    toCleanText(
      profile?.business_name ||
        profile?.whatsapp_name ||
        profile?.business_category ||
        "Our Store",
      140
    ) || "Our Store",
  category: toCleanText(profile?.business_category, 120),
  address: toCleanText(profile?.business_address, 500),
  hours: toCleanText(profile?.business_hours, 160),
  mapUrl: toOptionalUrl(profile?.business_map_url),
  phone: toCleanText(profile?.whatsapp_number || profile?.phone, 40),
  email: toCleanText(profile?.email, 160),
});

const NORMALIZED_LOCATION_HINTS = [
  "location",
  "address",
  "where are you",
  "where is your shop",
  "where is your store",
  "google map",
  "map link",
  "kahan",
  "kahaan",
  "kaha",
  "kidhar",
  "pata",
];

const RAW_LOCATION_HINTS = ["लोकेशन", "पता", "कहाँ", "कहां", "मैप"];

const NORMALIZED_HOURS_HINTS = [
  "timing",
  "timings",
  "business hours",
  "opening time",
  "closing time",
  "open today",
  "close today",
  "kitne baje",
  "kab khulte",
  "kab band",
];

const RAW_HOURS_HINTS = ["टाइमिंग", "समय", "कितने बजे", "खुलते", "बंद"];

const NORMALIZED_CONTACT_HINTS = [
  "contact",
  "phone number",
  "mobile number",
  "call number",
  "call",
  "email",
  "mail",
  "sampark",
];

const RAW_CONTACT_HINTS = ["कॉल", "नंबर", "नम्बर", "फोन", "ईमेल", "मेल", "संपर्क"];

const hasAnyHint = (value, hints) =>
  hints.some((hint) => value.includes(hint));

export const detectBusinessInfoIntent = ({
  normalizedText = "",
  rawText = "",
} = {}) => {
  const normalized = String(normalizedText || "").toLowerCase();
  const raw = String(rawText || "").toLowerCase();

  if (hasAnyHint(normalized, NORMALIZED_LOCATION_HINTS) || hasAnyHint(raw, RAW_LOCATION_HINTS)) {
    return "location";
  }
  if (hasAnyHint(normalized, NORMALIZED_HOURS_HINTS) || hasAnyHint(raw, RAW_HOURS_HINTS)) {
    return "hours";
  }
  if (hasAnyHint(normalized, NORMALIZED_CONTACT_HINTS) || hasAnyHint(raw, RAW_CONTACT_HINTS)) {
    return "contact";
  }
  return null;
};

const BUSINESS_REPLY_COPY = Object.freeze({
  en: {
    locationTitle: "📍 *Our Location*",
    contactTitle: "📞 *Contact Details*",
    hoursTitle: "🕒 *Business Hours*",
    addressMissing: "Our exact address is not available right now.",
    hoursMissing: "Our exact business hours are not available right now.",
    contactMissing: "Our direct contact details are not available right now.",
    addressLabel: "📍 *Address:*",
    mapLabel: "🗺️ *Map:*",
    hoursLabel: "🕒 *Hours:*",
    callLabel: "📞 *Call:*",
    emailLabel: "✉️ *Email:*",
  },
  hinglish: {
    locationTitle: "📍 *Hamari Location*",
    contactTitle: "📞 *Hamari Contact Details*",
    hoursTitle: "🕒 *Hamari Timing*",
    addressMissing: "Hamara exact address abhi available nahi hai.",
    hoursMissing: "Hamari exact timing abhi available nahi hai.",
    contactMissing: "Hamari direct contact details abhi available nahi hain.",
    addressLabel: "📍 *Address:*",
    mapLabel: "🗺️ *Map:*",
    hoursLabel: "🕒 *Timing:*",
    callLabel: "📞 *Call:*",
    emailLabel: "✉️ *Email:*",
  },
  hi: {
    locationTitle: "📍 *हमारी लोकेशन*",
    contactTitle: "📞 *हमारी कॉन्टैक्ट डिटेल्स*",
    hoursTitle: "🕒 *हमारी टाइमिंग*",
    addressMissing: "हमारा सही पता अभी उपलब्ध नहीं है।",
    hoursMissing: "हमारी सही टाइमिंग अभी उपलब्ध नहीं है।",
    contactMissing: "हमारी सीधी कॉन्टैक्ट डिटेल्स अभी उपलब्ध नहीं हैं।",
    addressLabel: "📍 *पता:*",
    mapLabel: "🗺️ *मैप:*",
    hoursLabel: "🕒 *टाइमिंग:*",
    callLabel: "📞 *कॉल:*",
    emailLabel: "✉️ *ईमेल:*",
  },
});

const resolveReplyCopy = (languageCode = "en") =>
  BUSINESS_REPLY_COPY[languageCode] || BUSINESS_REPLY_COPY.en;

const appendLine = (lines, label, value) => {
  if (!value) return;
  lines.push(`${label} ${value}`);
};

export const buildBusinessInfoReplyTemplate = ({
  intent,
  businessInfo,
  languageCode = "en",
} = {}) => {
  const info = normalizeBusinessInfo(businessInfo);
  const copy = resolveReplyCopy(languageCode);
  const lines = [];

  if (intent === "location") {
    lines.push(copy.locationTitle);
    if (info.address) {
      lines.push(info.address);
    } else {
      lines.push(copy.addressMissing);
    }
    appendLine(lines, copy.mapLabel, info.mapUrl);
    appendLine(lines, copy.hoursLabel, info.hours);
    appendLine(lines, copy.callLabel, info.phone);
    appendLine(lines, copy.emailLabel, info.email);
    return lines.join("\n");
  }

  if (intent === "hours") {
    lines.push(copy.hoursTitle);
    lines.push(info.hours || copy.hoursMissing);
    appendLine(lines, copy.addressLabel, info.address);
    appendLine(lines, copy.callLabel, info.phone);
    appendLine(lines, copy.emailLabel, info.email);
    return lines.join("\n");
  }

  lines.push(copy.contactTitle);
  if (!info.phone && !info.email && !info.address) {
    lines.push(copy.contactMissing);
    return lines.join("\n");
  }
  appendLine(lines, copy.callLabel, info.phone);
  appendLine(lines, copy.emailLabel, info.email);
  appendLine(lines, copy.addressLabel, info.address);
  appendLine(lines, copy.hoursLabel, info.hours);
  appendLine(lines, copy.mapLabel, info.mapUrl);
  return lines.join("\n");
};

export const buildBusinessInfoAiContext = (businessInfo = {}) => {
  const info = normalizeBusinessInfo(businessInfo);
  return [
    `Display name: ${info.brandName}`,
    `Category: ${info.category || "not available"}`,
    `Address: ${info.address || "not available"}`,
    `Business hours: ${info.hours || "not available"}`,
    `Phone: ${info.phone || "not available"}`,
    `Email: ${info.email || "not available"}`,
    `Map link: ${info.mapUrl || "not available"}`,
  ].join("\n");
};

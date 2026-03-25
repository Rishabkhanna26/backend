const GENERIC_CATALOG_TERMS = new Set([
  "service",
  "services",
  "product",
  "products",
  "item",
  "items",
  "general",
]);

const normalizeComparableText = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

const sanitizeText = (value, maxLength = 240) => {
  const cleaned = String(value || "").replace(/\r/g, "").trim();
  if (!cleaned) return "";
  return cleaned.slice(0, Math.max(Number(maxLength) || 0, 0));
};

export const parseCatalogKeywords = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry || "").trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(/[,;\n]+/)
      .map((entry) => entry.trim())
      .filter(Boolean);
  }
  return [];
};

const normalizePriceLabelInr = (value) => {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.includes("₹")) {
    return text.replace(/₹\s*/g, "₹ ").replace(/\s{2,}/g, " ").trim();
  }
  let normalized = text.replace(/^\s*(?:inr|rs\.?|rupees?)\s*/i, "₹ ");
  if (!normalized.includes("₹") && /^\d/.test(normalized)) {
    normalized = `₹ ${normalized}`;
  }
  return normalized.replace(/\s{2,}/g, " ").trim();
};

const parsePriceAmount = (value) => {
  if (value == null) return null;
  if (typeof value === "number" && Number.isFinite(value)) {
    return Number(value);
  }
  const raw = String(value || "").replace(/,/g, "");
  const matched = raw.match(/(\d+(?:\.\d+)?)/);
  if (!matched) return null;
  const numeric = Number(matched[1]);
  return Number.isFinite(numeric) ? numeric : null;
};

const formatCatalogDuration = (item) => {
  const durationValue = Number(item?.duration_value);
  const durationUnit = String(item?.duration_unit || "").trim().toLowerCase();
  if (Number.isFinite(durationValue) && durationValue > 0 && durationUnit) {
    const normalizedUnit = durationValue === 1 ? durationUnit.replace(/s$/, "") : durationUnit;
    return `${durationValue} ${normalizedUnit}`;
  }
  const durationMinutes = Number(item?.duration_minutes);
  if (Number.isFinite(durationMinutes) && durationMinutes > 0) {
    return `${durationMinutes} min`;
  }
  return "";
};

const formatCatalogPack = (item) => {
  const quantityValue = Number(item?.quantity_value);
  if (!Number.isFinite(quantityValue) || quantityValue <= 0) return "";
  const quantityUnit = sanitizeText(item?.quantity_unit || "unit", 40);
  return `${quantityValue} ${quantityUnit || "unit"}`;
};

const uniqueNonEmpty = (values = []) => {
  const seen = new Set();
  const output = [];
  for (const value of values) {
    const text = sanitizeText(value, 120);
    if (!text) continue;
    const key = text.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(text);
  }
  return output;
};

const buildCatalogKnowledgeLine = (item, itemType) => {
  const parts = [`${itemType}: ${sanitizeText(item?.name || "Unnamed item", 120)}`];
  const category = sanitizeText(item?.category, 120);
  const description = sanitizeText(item?.description, 240);
  const priceLabel = normalizePriceLabelInr(item?.price_label);
  const durationLabel = formatCatalogDuration(item);
  const packLabel = formatCatalogPack(item);
  const askFor = sanitizeText(item?.details_prompt, 220);
  const keywords = uniqueNonEmpty(parseCatalogKeywords(item?.keywords)).slice(0, 10);

  if (category) parts.push(`category: ${category}`);
  if (description) parts.push(`description: ${description}`);
  if (priceLabel) parts.push(`price: ${priceLabel}`);
  if (durationLabel) parts.push(`duration: ${durationLabel}`);
  if (packLabel) parts.push(`pack: ${packLabel}`);
  if (itemType === "service") {
    parts.push(`booking: ${item?.is_bookable ? "bookable" : "not bookable"}`);
  }
  if (askFor) parts.push(`ask for: ${askFor}`);
  if (keywords.length) parts.push(`aliases: ${keywords.join(", ")}`);

  return `- ${parts.join(" | ")}`;
};

const buildCategorySummary = (items = []) => uniqueNonEmpty(items.map((item) => item?.category)).join(", ");

const buildOfferingSummary = (items = []) =>
  uniqueNonEmpty(items.map((item) => item?.name)).slice(0, 10).join(", ");

export const collectCatalogComparableTerms = (catalog = {}) => {
  const terms = new Set();
  const addTerm = (value) => {
    const normalized = normalizeComparableText(value);
    if (!normalized || normalized.length <= 2) return;
    if (GENERIC_CATALOG_TERMS.has(normalized)) return;
    terms.add(normalized);
  };

  for (const item of [...(catalog?.services || []), ...(catalog?.products || [])]) {
    addTerm(item?.name);
    addTerm(item?.category);
    parseCatalogKeywords(item?.keywords).forEach(addTerm);
  }

  return Array.from(terms);
};

export const buildCatalogAiContext = ({ catalog, maxItemsPerType = 25 } = {}) => {
  const services = (catalog?.services || []).slice(0, maxItemsPerType);
  const products = (catalog?.products || []).slice(0, maxItemsPerType);

  const serviceLines = services
    .map((item) => buildCatalogKnowledgeLine(item, "Service"))
    .join("\n");
  const productLines = products
    .map((item) => buildCatalogKnowledgeLine(item, "Product"))
    .join("\n");

  return [
    `Catalog summary: ${services.length} active services and ${products.length} active products are configured.`,
    `Service categories: ${buildCategorySummary(services) || "none"}`,
    `Product categories: ${buildCategorySummary(products) || "none"}`,
    `Key services: ${buildOfferingSummary(services) || "none"}`,
    `Key products: ${buildOfferingSummary(products) || "none"}`,
    "Use 'aliases' as alternate user wording and use 'ask for' as lead-qualification guidance.",
    "",
    "Services:",
    serviceLines || "- None configured",
    "",
    "Products:",
    productLines || "- None configured",
  ].join("\n");
};

const buildCatalogItemMetaParts = (item, itemType) => {
  const details = [];
  const priceLabel = normalizePriceLabelInr(item?.price_label);
  const durationLabel = formatCatalogDuration(item);
  const packLabel = formatCatalogPack(item);

  if (priceLabel) details.push(`Price: ${priceLabel}`);
  if (itemType === "service" && durationLabel) details.push(`Duration: ${durationLabel}`);
  if (itemType === "product" && packLabel) details.push(`Pack: ${packLabel}`);
  return details;
};

const buildCatalogItemBlockLines = (item, itemType, index, { includeDescription = false } = {}) => {
  const name = sanitizeText(item?.name || "Unnamed item", 120);
  const meta = buildCatalogItemMetaParts(item, itemType);
  const description = includeDescription ? sanitizeText(item?.description, 220) : "";
  const lines = [`${index}. *${name}*`];

  if (meta.length) {
    lines.push(`   ${meta.join(" | ")}`);
  }
  if (description) {
    lines.push(`   ${description}`);
  }
  return lines;
};

const buildCatalogSectionLines = ({
  items,
  title = "",
  itemType,
  maxItems = 8,
  includeDescription = false,
} = {}) => {
  const visibleItems = (items || []).slice(0, maxItems);
  const hiddenCount = Math.max((items || []).length - visibleItems.length, 0);
  const lines = [];

  if (title) {
    lines.push(title);
  }

  if (!visibleItems.length) {
    lines.push("_None available right now_");
    return lines;
  }

  visibleItems.forEach((item, index) => {
    if (index > 0) lines.push("");
    lines.push(
      ...buildCatalogItemBlockLines(item, itemType, index + 1, {
        includeDescription,
      })
    );
  });

  if (hiddenCount > 0) {
    lines.push("");
    lines.push(`_+${hiddenCount} more available_`);
  }

  return lines;
};

const resolveCatalogItemPriceLabel = (item) =>
  normalizePriceLabelInr(item?.priceLabel || item?.price_label);

const resolveCatalogItemDurationLabel = (item) =>
  sanitizeText(item?.durationLabel, 80) || formatCatalogDuration(item);

const resolveCatalogItemPackLabel = (item) =>
  sanitizeText(item?.packLabel, 80) || formatCatalogPack(item);

const resolveCatalogItemPrompt = (item) =>
  sanitizeText(item?.prompt || item?.details_prompt, 220);

const buildCatalogReplySections = ({ items, title, itemType, maxItems = 8 }) => {
  return buildCatalogSectionLines({
    items,
    title,
    itemType,
    maxItems,
  });
};

export const findCatalogItemByPrice = ({
  catalog,
  itemType = "product",
  direction = "lowest",
} = {}) => {
  const items = itemType === "service" ? catalog?.services || [] : catalog?.products || [];
  const pricedItems = items
    .map((item) => ({
      item,
      price: parsePriceAmount(item?.price_label),
    }))
    .filter((entry) => Number.isFinite(entry.price));

  if (!pricedItems.length) return null;

  pricedItems.sort((left, right) => {
    const diff =
      direction === "highest" ? right.price - left.price : left.price - right.price;
    if (diff !== 0) return diff;
    return String(left.item?.name || "").localeCompare(String(right.item?.name || ""));
  });

  return pricedItems[0].item;
};

export const buildCatalogListReply = ({
  catalog,
  brandName = "Our Store",
  itemType = "all",
  languageCode = "en",
  maxItemsPerType = 8,
} = {}) => {
  const services = catalog?.services || [];
  const products = catalog?.products || [];
  const safeBrandName = sanitizeText(brandName, 140) || "Our Store";
  const language = ["hi", "hinglish"].includes(languageCode) ? languageCode : "en";
  const lines = [];

  if (language === "hi") {
    if (itemType === "product") {
      lines.push("*Product Catalog*");
      lines.push("यह products अभी available हैं:");
    } else if (itemType === "service") {
      lines.push("*Service Catalog*");
      lines.push("यह services अभी available हैं:");
    } else {
      lines.push(`*${safeBrandName} Catalog*`);
      lines.push("यह हमारे available offerings हैं:");
    }
  } else if (language === "hinglish") {
    if (itemType === "product") {
      lines.push("*Product Catalog*");
      lines.push("Yeh products abhi available hain:");
    } else if (itemType === "service") {
      lines.push("*Service Catalog*");
      lines.push("Yeh services abhi available hain:");
    } else {
      lines.push(`*${safeBrandName} Catalog*`);
      lines.push("Yeh hamare available offerings hain:");
    }
  } else if (itemType === "product") {
    lines.push("*Product Catalog*");
    lines.push("Here are the available products right now:");
  } else if (itemType === "service") {
    lines.push("*Service Catalog*");
    lines.push("Here are the available services right now:");
  } else {
    lines.push(`*${safeBrandName} Catalog*`);
    lines.push("Here are the main offerings available right now:");
  }

  if (itemType === "product") {
    lines.push("");
    lines.push(...buildCatalogReplySections({
      items: products,
      title: "",
      itemType: "product",
      maxItems: maxItemsPerType,
    }));
  } else if (itemType === "service") {
    lines.push("");
    lines.push(...buildCatalogReplySections({
      items: services,
      title: "",
      itemType: "service",
      maxItems: maxItemsPerType,
    }));
  } else {
    if (products.length) {
      lines.push("");
      lines.push(...buildCatalogReplySections({
        items: products,
        title: "*Products*",
        itemType: "product",
        maxItems: maxItemsPerType,
      }));
    }
    if (services.length) {
      lines.push("");
      lines.push(...buildCatalogReplySections({
        items: services,
        title: "*Services*",
        itemType: "service",
        maxItems: maxItemsPerType,
      }));
    }
  }

  if (!products.length && !services.length) {
    if (language === "hi") {
      lines.push("");
      lines.push("अभी कोई active products ya services configured नहीं हैं।");
    } else if (language === "hinglish") {
      lines.push("");
      lines.push("Abhi koi active products ya services configured nahin hain.");
    } else {
      lines.push("");
      lines.push("No active products or services are configured right now.");
    }
    return lines.join("\n");
  }

  lines.push("");
  if (language === "hi") {
    lines.push("Aap item ka naam bhejkar price, details, booking, delivery ya full catalog pooch sakte hain.");
  } else if (language === "hinglish") {
    lines.push("Aap item ka naam bhejkar price, details, booking, delivery ya full catalog pooch sakte hain.");
  } else {
    lines.push("Reply with any item name for price, details, booking, delivery, or the full catalog.");
  }

  return lines.join("\n");
};

export const buildCatalogListPageReply = ({
  catalog,
  brandName = "Our Store",
  itemType = "all",
  languageCode = "en",
  pageSize = 10,
  pageOffset = 0,
} = {}) => {
  const services = catalog?.services || [];
  const products = catalog?.products || [];
  const safeBrandName = sanitizeText(brandName, 140) || "Our Store";
  const language = ["hi", "hinglish"].includes(languageCode) ? languageCode : "en";

  if (itemType === "all") {
    return {
      text: buildCatalogListReply({
        catalog,
        brandName,
        itemType,
        languageCode,
        maxItemsPerType: pageSize,
      }),
      hasMore: false,
      nextOffset: null,
    };
  }

  const items = itemType === "service" ? services : products;
  if (!items.length) {
    return {
      text: buildCatalogListReply({
        catalog,
        brandName,
        itemType,
        languageCode,
        maxItemsPerType: pageSize,
      }),
      hasMore: false,
      nextOffset: null,
    };
  }

  const offset = Math.max(Number(pageOffset) || 0, 0);
  const limit = Math.max(Number(pageSize) || 10, 1);
  const visibleItems = items.slice(offset, offset + limit);
  const hasMore = items.length > offset + limit;
  const lines = [];
  const isFollowUp = offset > 0;

  if (language === "hi") {
    if (isFollowUp) {
      lines.push(itemType === "product" ? "*More Products*" : "*More Services*");
      lines.push(itemType === "product" ? "यह अगले available products हैं:" : "यह अगली available services हैं:");
    } else if (itemType === "product") {
      lines.push("*Product Catalog*");
      lines.push("यह products अभी available हैं:");
    } else {
      lines.push("*Service Catalog*");
      lines.push("यह services अभी available हैं:");
    }
  } else if (language === "hinglish") {
    if (isFollowUp) {
      lines.push(itemType === "product" ? "*More Products*" : "*More Services*");
      lines.push(itemType === "product" ? "Yeh next available products hain:" : "Yeh next available services hain:");
    } else if (itemType === "product") {
      lines.push("*Product Catalog*");
      lines.push("Yeh products abhi available hain:");
    } else {
      lines.push("*Service Catalog*");
      lines.push("Yeh services abhi available hain:");
    }
  } else if (isFollowUp) {
    lines.push(itemType === "product" ? "*More Products*" : "*More Services*");
    lines.push(itemType === "product" ? "Here are the next available products:" : "Here are the next available services:");
  } else if (itemType === "product") {
    lines.push("*Product Catalog*");
    lines.push("Here are the available products right now:");
  } else {
    lines.push("*Service Catalog*");
    lines.push("Here are the available services right now:");
  }

  lines.push("");
  lines.push(
    ...buildCatalogSectionLines({
      items: visibleItems,
      title: "",
      itemType,
      maxItems: limit,
    })
  );

  lines.push("");
  if (hasMore) {
    if (language === "hi") {
      lines.push("Aur options dekhne hain? *yes* reply karein.");
    } else if (language === "hinglish") {
      lines.push("Aur options dekhne hain? *yes* reply karein.");
    } else {
      lines.push("Want to see more options? Reply *yes*.");
    }
  } else if (language === "hi") {
    lines.push("Aap item ka naam bhejkar price, details, booking, delivery ya full catalog pooch sakte hain.");
  } else if (language === "hinglish") {
    lines.push("Aap item ka naam bhejkar price, details, booking, delivery ya full catalog pooch sakte hain.");
  } else {
    lines.push("Reply with any item name for price, details, booking, delivery, or the full catalog.");
  }

  return {
    text: lines.join("\n"),
    hasMore,
    nextOffset: hasMore ? offset + limit : null,
  };
};

export const buildCatalogPriceReply = ({
  item,
  itemType = "product",
  direction = "lowest",
  languageCode = "en",
} = {}) => {
  const language = ["hi", "hinglish"].includes(languageCode) ? languageCode : "en";
  const scopeLabel = itemType === "service" ? "service" : "product";
  const priceLabel = normalizePriceLabelInr(item?.price_label);
  const durationLabel = formatCatalogDuration(item);
  const packLabel = formatCatalogPack(item);
  const description = sanitizeText(item?.description, 220);

  if (!item) {
    if (language === "hi") {
      return `माफ कीजिए, अभी हमारे ${scopeLabel}s की pricing available नहीं है।`;
    }
    if (language === "hinglish") {
      return `Sorry, abhi hamare ${scopeLabel}s ki pricing available nahin hai.`;
    }
    return `Sorry, I couldn't find priced ${scopeLabel}s right now.`;
  }

  const qualifier =
    direction === "highest"
      ? language === "hi"
        ? "सबसे महंगा"
        : language === "hinglish"
          ? "sabse mehnga"
          : "most expensive"
      : language === "hi"
        ? "सबसे सस्ता"
        : language === "hinglish"
          ? "sabse sasta"
          : "cheapest";

  const lines = [];
  if (language === "hi") {
    lines.push(`जी हां, हमारा ${qualifier} ${scopeLabel} *${sanitizeText(item?.name, 120)}* है।`);
  } else if (language === "hinglish") {
    lines.push(`Ji haan, hamara ${qualifier} ${scopeLabel} *${sanitizeText(item?.name, 120)}* hai.`);
  } else {
    lines.push(`The ${qualifier} ${scopeLabel} we have right now is *${sanitizeText(item?.name, 120)}*.`);
  }

  if (priceLabel) lines.push(`*Price:* ${priceLabel}`);
  if (itemType === "product" && packLabel) lines.push(`*Pack:* ${packLabel}`);
  if (itemType === "service" && durationLabel) lines.push(`*Duration:* ${durationLabel}`);
  if (description) lines.push(`*Details:* ${description}`);

  if (language === "hi") {
    lines.push("अगर आप चाहें तो मैं इसकी details ya order mein help कर सकता हूँ।");
  } else if (language === "hinglish") {
    lines.push("Agar aap chahen to main iski details ya order mein help kar sakta hoon.");
  } else {
    lines.push("If you want, I can share details or help you order it.");
  }

  return lines.join("\n");
};

export const buildCatalogPopularReply = ({
  item,
  itemType = "product",
  appointmentKind = "service",
  languageCode = "en",
  source = "sales",
} = {}) => {
  const language = ["hi", "hinglish"].includes(languageCode) ? languageCode : "en";
  const scopeLabel =
    itemType === "service"
      ? appointmentKind === "booking"
        ? "booking"
        : "service"
      : "product";
  const name = sanitizeText(item?.name || item?.label, 120);
  const priceLabel = normalizePriceLabelInr(item?.price_label);
  const durationLabel = formatCatalogDuration(item);
  const packLabel = formatCatalogPack(item);
  const description = sanitizeText(item?.description, 220);
  const basedOnHistory = source === "sales" || source === "bookings";

  if (!name) {
    if (language === "hi") {
      return `माफ कीजिए, अभी मैं कोई recommended ${scopeLabel} नहीं चुन पा रहा हूँ।`;
    }
    if (language === "hinglish") {
      return `Sorry, abhi main koi recommended ${scopeLabel} pick nahin kar pa raha hoon.`;
    }
    return `Sorry, I couldn't choose a recommended ${scopeLabel} right now.`;
  }

  const lines = [];
  if (basedOnHistory) {
    if (language === "hi") {
      lines.push(
        `जी हां, हमारा सबसे popular ${scopeLabel} *${name}* है। यह अभी तक सबसे ज्यादा ${
          itemType === "service" ? "book" : "order"
        } हुआ है।`
      );
    } else if (language === "hinglish") {
      lines.push(
        `Ji haan, hamara sabse popular ${scopeLabel} *${name}* hai. Yeh ab tak sabse zyada ${
          itemType === "service" ? "book" : "order"
        } hua hai.`
      );
    } else {
      lines.push(
        `Our most popular ${scopeLabel} right now is *${name}*. It has been ${
          itemType === "service" ? "booked" : "ordered"
        } the most so far.`
      );
    }
  } else if (language === "hi") {
    lines.push(`जी हां, recommendation के हिसाब से *${name}* हमारा suggested ${scopeLabel} है।`);
  } else if (language === "hinglish") {
    lines.push(`Ji haan, recommendation ke hisaab se *${name}* hamara suggested ${scopeLabel} hai.`);
  } else {
    lines.push(`If you want my recommendation, *${name}* is a strong ${scopeLabel} choice.`);
  }

  if (priceLabel) lines.push(`*Price:* ${priceLabel}`);
  if (itemType === "product" && packLabel) lines.push(`*Pack:* ${packLabel}`);
  if (itemType === "service" && durationLabel) lines.push(`*Duration:* ${durationLabel}`);
  if (description) lines.push(`*Details:* ${description}`);

  if (language === "hi") {
    lines.push("अगर आप चाहें तो मैं इसकी details share कर सकता हूँ या order में help कर सकता हूँ।");
  } else if (language === "hinglish") {
    lines.push("Agar aap chahen to main iski details share kar sakta hoon ya order mein help kar sakta hoon.");
  } else {
    lines.push("If you want, I can share more details or help you order it.");
  }

  return lines.join("\n");
};

export const buildCatalogLowestPopularityDeflectionReply = ({
  item,
  itemType = "product",
  appointmentKind = "service",
  languageCode = "en",
} = {}) => {
  const language = ["hi", "hinglish"].includes(languageCode) ? languageCode : "en";
  const scopeLabel =
    itemType === "service"
      ? appointmentKind === "booking"
        ? "booking"
        : "service"
      : "product";
  const name = sanitizeText(item?.name || item?.label, 120);

  const lines = [];
  if (language === "hi") {
    lines.push(
      `Main "sabse kam bikne wala" ${scopeLabel} recommend nahi karta, kyunki woh aksar aapki need ke hisaab se sahi choice nahi hota.`
    );
    if (name) lines.push(`Agar aap safe choice chahte hain, hamara sabse popular ${scopeLabel} *${name}* hai.`);
    lines.push("Aap apni requirement bata dijiye, main aapke liye best option suggest kar dunga.");
  } else if (language === "hinglish") {
    lines.push(
      `Main "sabse kam bikne wala" ${scopeLabel} recommend nahin karta, kyunki woh aksar aapki need ke hisaab se sahi choice nahin hota.`
    );
    if (name) lines.push(`Agar aap safe choice chahte hain, hamara sabse popular ${scopeLabel} *${name}* hai.`);
    lines.push("Aap apni requirement bata dijiye, main aapke liye best option suggest kar dunga.");
  } else {
    lines.push(
      `I wouldn’t recommend picking the “lowest selling” ${scopeLabel} just for that reason.`
    );
    if (name) lines.push(`If you want a safe pick, our most popular ${scopeLabel} is *${name}*.`);
    lines.push("Tell me what you need, and I’ll suggest the best option for you.");
  }

  return lines.join("\n");
};

export const buildCatalogAvailabilityReply = ({
  requestedName = "",
  matchedItem = null,
  itemType = "all",
  catalog,
  languageCode = "en",
} = {}) => {
  const language = ["hi", "hinglish"].includes(languageCode) ? languageCode : "en";
  const normalizedRequestedName = normalizeComparableText(requestedName)
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .trim();
  const genericBrowseWords = new Set([
    "all",
    "full",
    "sabhi",
    "sare",
    "saare",
    "pura",
    "poora",
    "product",
    "products",
    "service",
    "services",
    "item",
    "items",
    "catalog",
    "menu",
    "show",
    "list",
    "dikha",
    "dikhao",
    "dikhaiye",
    "available",
    "mujhe",
    "mujko",
    "mujhko",
    "mereko",
    "merko",
    "kya",
    "aap",
    "hai",
    "hain",
    "ho",
    "sakte",
    "sakta",
    "sakti",
  ]);
  const requestedTokens = normalizedRequestedName
    .split(/\s+/)
    .map((word) => word.trim())
    .filter((word) => word.length > 1);
  const meaningfulRequestedTokens = requestedTokens.filter(
    (word) => !genericBrowseWords.has(word)
  );
  const safeRequestedName =
    meaningfulRequestedTokens.length > 0
      ? sanitizeText(requestedName, 120)
      : itemType === "product"
        ? "those products"
        : itemType === "service"
          ? "those services"
          : "that item";

  if (matchedItem) {
    const name = sanitizeText(matchedItem?.name || matchedItem?.label, 120) || "Selected item";
    const category = sanitizeText(matchedItem?.category, 120);
    const description = sanitizeText(matchedItem?.description, 320);
    const priceLabel = resolveCatalogItemPriceLabel(matchedItem);
    const durationLabel = resolveCatalogItemDurationLabel(matchedItem);
    const packLabel = resolveCatalogItemPackLabel(matchedItem);
    const prompt = resolveCatalogItemPrompt(matchedItem);
    const lines = [];

    if (language === "hi") {
      lines.push(`जी हां, *${name}* available है।`);
    } else if (language === "hinglish") {
      lines.push(`Ji haan, *${name}* available hai.`);
    } else {
      lines.push(`Yes, *${name}* is available.`);
    }

    lines.push("");
    lines.push(`*${name}*`);
    if (category) lines.push(`*Category:* ${category}`);
    if (description) lines.push(`*Details:* ${description}`);
    if (itemType === "service" && durationLabel) lines.push(`*Duration:* ${durationLabel}`);
    if (itemType === "product" && packLabel) lines.push(`*Pack:* ${packLabel}`);
    if (priceLabel) lines.push(`*Price:* ${priceLabel}`);
    if (prompt) lines.push(`*Info Needed:* ${prompt}`);

    if (language === "hi") {
      lines.push("अगर आप चाहें तो मैं booking, details ya next step में help कर सकता हूँ।");
    } else if (language === "hinglish") {
      lines.push("Agar aap chahen to main booking, details ya next step mein help kar sakta hoon.");
    } else {
      lines.push("If you want, I can help with booking, details, or the next step.");
    }

    return lines.join("\n");
  }

  const previewItems =
    itemType === "product"
      ? catalog?.products || []
      : itemType === "service"
        ? catalog?.services || []
        : [...(catalog?.services || []), ...(catalog?.products || [])];
  const lines = [];

  if (language === "hi") {
    lines.push(`माफ कीजिए, अभी *${safeRequestedName}* available नहीं है।`);
  } else if (language === "hinglish") {
    lines.push(`Sorry, abhi *${safeRequestedName}* available nahin hai.`);
  } else {
    lines.push(`Sorry, *${safeRequestedName}* is not available right now.`);
  }

  if (previewItems.length) {
    lines.push("");
    if (itemType === "product" || itemType === "service") {
      lines.push(
        itemType === "product"
          ? "*Available products right now*"
          : "*Available services right now*"
      );
      lines.push(
        ...buildCatalogSectionLines({
          items: previewItems,
          title: "",
          itemType,
          maxItems: 4,
        })
      );
    } else {
      lines.push("*Available options right now*");
      if ((catalog?.products || []).length) {
        lines.push("");
        lines.push(
          ...buildCatalogSectionLines({
            items: catalog?.products || [],
            title: "*Products*",
            itemType: "product",
            maxItems: 3,
          })
        );
      }
      if ((catalog?.services || []).length) {
        lines.push("");
        lines.push(
          ...buildCatalogSectionLines({
            items: catalog?.services || [],
            title: "*Services*",
            itemType: "service",
            maxItems: 3,
          })
        );
      }
    }
  }

  lines.push("");
  if (language === "hi") {
    lines.push("अगर आप चाहें तो अपनी requirement ya budget बताइए, मैं closest option suggest कर दूँगा।");
  } else if (language === "hinglish") {
    lines.push("Agar aap chahen to apni requirement ya budget batayein, main closest option suggest kar dunga.");
  } else {
    lines.push("If you want, tell me your requirement or budget and I will suggest the closest option.");
  }

  return lines.join("\n");
};

export const buildCatalogGreetingPreview = ({
  brandName = "Our Store",
  catalog,
  maxItemsPerType = 3,
} = {}) => {
  const services = (catalog?.services || []).slice(0, maxItemsPerType);
  const products = (catalog?.products || []).slice(0, maxItemsPerType);
  const lines = [`Hi! Welcome to ${sanitizeText(brandName, 140) || "Our Store"}.`];

  if (!services.length && !products.length) {
    lines.push("I can help with our products and services.");
    lines.push("Ask me what you need, and I will guide you.");
    return lines.join("\n");
  }

  lines.push("");
  lines.push("*Quick Catalog Preview*");
  lines.push("Here are a few things available right now:");

  if (products.length) {
    lines.push("");
    lines.push("*Products*");
    lines.push(...buildCatalogSectionLines({
      items: products,
      title: "",
      itemType: "product",
      maxItems: maxItemsPerType,
    }));
  }

  if (services.length) {
    lines.push("");
    lines.push("*Services*");
    lines.push(...buildCatalogSectionLines({
      items: services,
      title: "",
      itemType: "service",
      maxItems: maxItemsPerType,
    }));
  }

  lines.push("");
  lines.push("You can reply with *all products*, *all services*, *price*, *details*, *booking*, or *delivery*.");
  return lines.join("\n");
};

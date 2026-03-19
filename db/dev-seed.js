import "dotenv/config";
import pg from "pg";
import { hashPassword } from "../lib/auth.js";
import { initDatabase } from "./init.js";

const { Client } = pg;

const DUMMY_ADMIN = {
  name: "AARTI SHARMA",
  phone: "24720908871740",
  email: "aartiisharma26@gmail.com",
  password: "demo12345",
  businessName: "creative code",
  businessCategory: "creative code",
  businessType: "product",
};

const DUMMY_CATALOG = [
  {
    item_type: "service",
    name: "Initial Consultation",
    category: "Consultation",
    description: "30-minute discovery call to understand customer needs.",
    price_label: "₹ 499",
    duration_value: 30,
    duration_unit: "minutes",
    quantity_value: null,
    quantity_unit: null,
    details_prompt: "Share your concern and preferred timing.",
    keywords: "consultation,discovery,intro",
    sort_order: 10,
    is_bookable: true,
  },
  {
    item_type: "service",
    name: "Follow-up Visit",
    category: "Consultation",
    description: "Follow-up support session for ongoing customers.",
    price_label: "₹ 799",
    duration_value: 45,
    duration_unit: "minutes",
    quantity_value: null,
    quantity_unit: null,
    details_prompt: "Share previous order/appointment reference if available.",
    keywords: "follow-up,support,revisit",
    sort_order: 20,
    is_bookable: true,
  },
  {
    item_type: "service",
    name: "Teleconsultation",
    category: "Online",
    description: "Remote consultation over call/video.",
    price_label: "₹ 999",
    duration_value: 60,
    duration_unit: "minutes",
    quantity_value: null,
    quantity_unit: null,
    details_prompt: "Mention preferred slot and communication mode.",
    keywords: "tele,online,video consultation",
    sort_order: 30,
    is_bookable: true,
  },
  {
    item_type: "product",
    name: "Starter Pack",
    category: "Bundles",
    description: "Entry-level starter product bundle for first-time buyers.",
    price_label: "₹ 1,499",
    duration_value: null,
    duration_unit: null,
    quantity_value: 1,
    quantity_unit: "pack",
    details_prompt: "Share quantity and delivery location.",
    keywords: "starter,bundle,entry",
    sort_order: 110,
    is_bookable: false,
  },
  {
    item_type: "product",
    name: "Premium Pack",
    category: "Bundles",
    description: "Premium bundle with extended features and support.",
    price_label: "₹ 2,999",
    duration_value: null,
    duration_unit: null,
    quantity_value: 1,
    quantity_unit: "pack",
    details_prompt: "Share quantity and preferred delivery date.",
    keywords: "premium,bundle,pro",
    sort_order: 120,
    is_bookable: false,
  },
  {
    item_type: "product",
    name: "Wellness Kit",
    category: "Kits",
    description: "Compact test kit suitable for day-to-day use.",
    price_label: "₹ 899",
    duration_value: null,
    duration_unit: null,
    quantity_value: 1,
    quantity_unit: "kit",
    details_prompt: "Share quantity and any special packing request.",
    keywords: "wellness,kit,daily use",
    sort_order: 130,
    is_bookable: false,
  },
];

const normalizePriceLabel = (value) => {
  const text = String(value || "").trim();
  if (!text) return null;
  if (text.includes("₹")) {
    return text.replace(/₹\s*/g, "₹ ").replace(/\s{2,}/g, " ").trim();
  }
  let normalized = text.replace(/^\s*(?:inr|rs\.?|rupees?)\s*/i, "₹ ");
  if (!normalized.includes("₹") && /^\d/.test(normalized)) {
    normalized = `₹ ${normalized}`;
  }
  return normalized.replace(/\s{2,}/g, " ").trim();
};

async function getOrCreateSeedAdmin(client) {
  const existing = await client.query(
    `
      SELECT id
      FROM admins
      WHERE regexp_replace(COALESCE(phone, ''), '\\D', '', 'g') = regexp_replace($1, '\\D', '', 'g')
         OR LOWER(COALESCE(email, '')) = LOWER($2)
      ORDER BY id ASC
      LIMIT 1
    `,
    [DUMMY_ADMIN.phone, DUMMY_ADMIN.email]
  );
  if (existing.rows.length > 0) {
    return Number(existing.rows[0].id);
  }

  const [superAdmin, firstAdmin] = await Promise.all([
    client.query(
      `
        SELECT id
        FROM admins
        WHERE admin_tier = 'super_admin'
        ORDER BY id ASC
        LIMIT 1
      `
    ),
    client.query(
      `
        SELECT id
        FROM admins
        ORDER BY id ASC
        LIMIT 1
      `
    ),
  ]);

  if (superAdmin.rows.length > 0) {
    return Number(superAdmin.rows[0].id);
  }
  if (firstAdmin.rows.length > 0) {
    return Number(firstAdmin.rows[0].id);
  }

  const created = await client.query(
    `
      INSERT INTO admins (
        name,
        phone,
        email,
        password_hash,
        admin_tier,
        status,
        business_name,
        business_category,
        business_type
      )
      VALUES ($1, $2, $3, $4, 'client_admin', 'active', $5, $6, $7)
      RETURNING id
    `,
    [
      DUMMY_ADMIN.name,
      DUMMY_ADMIN.phone,
      DUMMY_ADMIN.email,
      hashPassword(DUMMY_ADMIN.password),
      DUMMY_ADMIN.businessName,
      DUMMY_ADMIN.businessCategory,
      DUMMY_ADMIN.businessType,
    ]
  );
  return Number(created.rows[0].id);
}

async function seedCatalogItems(client, adminId) {
  let inserted = 0;
  let skipped = 0;

  for (const item of DUMMY_CATALOG) {
    const exists = await client.query(
      `
        SELECT id
        FROM catalog_items
        WHERE admin_id = $1
          AND item_type = $2
          AND LOWER(name) = LOWER($3)
        LIMIT 1
      `,
      [adminId, item.item_type, item.name]
    );

    if (exists.rows.length > 0) {
      skipped += 1;
      continue;
    }

    const durationValue =
      item.item_type === "service" && Number.isFinite(Number(item.duration_value))
        ? Number(item.duration_value)
        : null;
    const durationUnit = durationValue ? String(item.duration_unit || "minutes") : null;
    const durationMinutes = durationValue
      ? durationUnit === "hours"
        ? Math.round(durationValue * 60)
        : Math.round(durationValue)
      : null;
    const quantityValue =
      item.item_type === "product" && Number.isFinite(Number(item.quantity_value))
        ? Number(item.quantity_value)
        : null;
    const quantityUnit = quantityValue ? String(item.quantity_unit || "unit") : null;

    await client.query(
      `
        INSERT INTO catalog_items (
          admin_id,
          item_type,
          name,
          category,
          description,
          price_label,
          duration_value,
          duration_unit,
          duration_minutes,
          quantity_value,
          quantity_unit,
          details_prompt,
          keywords,
          is_active,
          sort_order,
          is_bookable
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9, $10, $11, $12, $13, TRUE, $14, $15
        )
      `,
      [
        adminId,
        item.item_type,
        item.name,
        item.category,
        item.description,
        normalizePriceLabel(item.price_label),
        durationValue,
        durationUnit,
        durationMinutes,
        quantityValue,
        quantityUnit,
        item.details_prompt,
        item.keywords,
        Number(item.sort_order) || 0,
        Boolean(item.is_bookable),
      ]
    );
    inserted += 1;
  }

  return { inserted, skipped };
}

async function run() {
  if (process.env.NODE_ENV === "production") {
    console.log("ℹ️ Skipping dev seed in production mode.");
    return;
  }

  if (!process.env.DATABASE_URL) {
    console.warn("⚠️ DATABASE_URL is not set. Skipping dev seed.");
    return;
  }

  await initDatabase({ recreate: false });

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await client.connect();
    await client.query("BEGIN");

    const adminId = await getOrCreateSeedAdmin(client);
    const { inserted, skipped } = await seedCatalogItems(client, adminId);

    await client.query("COMMIT");
    console.log(
      `✅ Dev seed complete for admin ${adminId}: inserted=${inserted}, already_present=${skipped}`
    );
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    await client.end();
  }
}

run()
  .then(() => process.exit(0))
  .catch((error) => {
    const message = error?.message || String(error);
    if (process.env.DEV_SEED_STRICT === "true") {
      console.error("❌ Dev seed failed:", message);
      process.exit(1);
      return;
    }
    console.warn("⚠️ Dev seed skipped:", message);
    process.exit(0);
  });

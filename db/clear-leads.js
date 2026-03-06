import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

const confirm = process.env.CONFIRM_DB_CLEAR;
if (confirm !== "YES") {
  console.error("Refusing to run. Set CONFIRM_DB_CLEAR=YES to proceed.");
  process.exit(1);
}

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is not set.");
  process.exit(1);
}

const SEEDED_ADMIN_PHONES = [
  "9000000001",
  "9000000002",
  "9000000003",
  "9000000004",
  "9000000005",
];

const SEEDED_ADMIN_EMAILS = [
  "neha.admin1@example.com",
  "arjun.admin2@example.com",
  "zara.admin3@example.com",
  "vivaan.admin4@example.com",
  "priya.admin5@example.com",
];

const SEEDED_CONTACT_PHONES = [
  "9100000001",
  "9100000002",
  "9100000003",
  "9100000004",
  "9100000005",
  "9100000006",
  "9100000007",
  "9100000008",
  "9100000009",
  "9100000010",
];

const SEEDED_CONTACT_EMAILS = [
  "aarav@example.com",
  "diya@example.com",
  "kabir@example.com",
  "nisha@example.com",
  "ritika@example.com",
  "ishaan@example.com",
  "maya@example.com",
  "ravi@example.com",
  "sara@example.com",
  "tanvi@example.com",
];

const SEEDED_BROADCAST_TITLES = [
  "Weekly Tips",
  "New Package Launch",
  "Weekend Offer",
];

const SEEDED_TEMPLATE_NAMES = [
  "Welcome Message",
  "Payment Link",
  "Appointment Confirmed",
];

const SEEDED_CATALOG_NAMES = [
  "Birth Chart Reading",
  "Compatibility Match",
  "Career Guidance",
  "Personalized Report PDF",
  "Personal Shopping Assist",
  "Bulk Order Assist",
  "Starter Pack",
  "Premium Pack",
  "Initial Consultation",
  "Follow-up Visit",
  "Teleconsultation",
  "Wellness Kit",
];

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const countDelete = async (client, query, params) => {
  const result = await client.query(query, params);
  return result.rowCount || 0;
};

try {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const seededAdminQuery = await client.query(
      `
      SELECT id
      FROM admins
      WHERE regexp_replace(COALESCE(phone, ''), '\\D', '', 'g') = ANY($1::text[])
         OR LOWER(COALESCE(email, '')) = ANY($2::text[])
      `,
      [SEEDED_ADMIN_PHONES, SEEDED_ADMIN_EMAILS]
    );
    const seededAdminIds = seededAdminQuery.rows.map((row) => Number(row.id)).filter(Number.isFinite);

    const deletedAdmins = seededAdminIds.length
      ? await countDelete(
          client,
          `DELETE FROM admins WHERE id = ANY($1::int[])`,
          [seededAdminIds]
        )
      : 0;

    const deletedContacts = await countDelete(
      client,
      `
      DELETE FROM contacts
      WHERE regexp_replace(COALESCE(phone, ''), '\\D', '', 'g') = ANY($1::text[])
         OR LOWER(COALESCE(email, '')) = ANY($2::text[])
      `,
      [SEEDED_CONTACT_PHONES, SEEDED_CONTACT_EMAILS]
    );

    const deletedOrders = await countDelete(
      client,
      `
      DELETE FROM orders
      WHERE order_number ~ '^(SRV|PRD|BTH|GEN)-[0-9]+$'
         OR regexp_replace(COALESCE(customer_phone, ''), '\\D', '', 'g') = ANY($1::text[])
         OR LOWER(COALESCE(customer_email, '')) = ANY($2::text[])
      `,
      [SEEDED_CONTACT_PHONES, SEEDED_CONTACT_EMAILS]
    );

    const deletedBroadcasts = await countDelete(
      client,
      `
      DELETE FROM broadcasts
      WHERE title = ANY($1::text[])
      `,
      [SEEDED_BROADCAST_TITLES]
    );

    const deletedTemplates = await countDelete(
      client,
      `
      DELETE FROM templates
      WHERE name = ANY($1::text[])
      `,
      [SEEDED_TEMPLATE_NAMES]
    );

    const deletedCatalogItems = await countDelete(
      client,
      `
      DELETE FROM catalog_items
      WHERE name = ANY($1::text[])
      `,
      [SEEDED_CATALOG_NAMES]
    );

    await client.query("COMMIT");

    console.log("✅ Dummy data cleanup complete.");
    console.log({
      deletedAdmins,
      deletedContacts,
      deletedOrders,
      deletedBroadcasts,
      deletedTemplates,
      deletedCatalogItems,
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
} catch (error) {
  console.error("❌ Failed to clear dummy data:", error?.message || error);
  process.exitCode = 1;
} finally {
  await pool.end();
}

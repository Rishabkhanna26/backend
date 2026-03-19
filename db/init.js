import fs from "fs";
import path from "path";
import pg from "pg";
import nodemailer from "nodemailer";
import { randomBytes } from "node:crypto";
import { hashPassword } from "../lib/auth.js";

function parseEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const content = fs.readFileSync(filePath, "utf8");
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIndex = trimmed.indexOf("=");
    if (eqIndex === -1) continue;
    const key = trimmed.slice(0, eqIndex).trim();
    let value = trimmed.slice(eqIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

const root = path.resolve(process.cwd());
parseEnvFile(path.join(root, ".env"));
parseEnvFile(path.join(root, ".env.local"));

const { Client } = pg;

const DATABASE_URL = process.env.DATABASE_URL;

const DEFAULT_SUPER_ADMIN = {
  name: "Rishab Khanna",
  phone: "8708767499",
  email: "rishabkhanna26@gmail.com",
};

const SMTP_EMAIL = process.env.SMTP_EMAIL || "";
const SMTP_PASSWORD = process.env.SMTP_PASSWORD || "";

function generatePassword() {
  return randomBytes(10).toString("hex");
}

async function sendPasswordEmail(to, password) {
  if (!SMTP_EMAIL || !SMTP_PASSWORD) {
    console.warn("⚠️ SMTP_EMAIL or SMTP_PASSWORD not set. Skipping password email.");
    return false;
  }

  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: SMTP_EMAIL,
      pass: SMTP_PASSWORD,
    },
  });

  await transporter.sendMail({
    from: SMTP_EMAIL,
    to,
    subject: "Your AlgoChat Super Admin Account",
    text: `Your super admin account has been created.\n\nEmail: ${to}\nTemporary Password: ${password}\n\nPlease log in and change your password.`,
  });

  return true;
}

async function ensureDefaultSuperAdmin(client) {
  const { rows: superAdmins } = await client.query(
    `SELECT id, email, password_hash
     FROM admins
     WHERE admin_tier = 'super_admin'
     ORDER BY id ASC`
  );

  // If any super admin has no password, set one and send exactly one temporary email.
  if (superAdmins.length > 0) {
    const target = superAdmins.find((row) => !row.password_hash);
    if (!target) {
      console.log("✅ Super admin already existed with a password.");
      return;
    }

    const plainPassword = generatePassword();
    await client.query(
      `UPDATE admins SET password_hash = $1 WHERE id = $2`,
      [hashPassword(plainPassword), target.id]
    );

    const recipientEmail = target.email || DEFAULT_SUPER_ADMIN.email;
    try {
      const sent = await sendPasswordEmail(recipientEmail, plainPassword);
      if (sent) {
        console.log("✅ Super admin password emailed.");
      } else {
        console.warn("⚠️ Email not sent. Temporary super admin password:", plainPassword);
      }
    } catch (err) {
      console.error("❌ Failed to send super admin email:", err.message);
      console.warn("⚠️ Temporary super admin password:", plainPassword);
    }
    return;
  }

  const { rows: existing } = await client.query(
    `SELECT id, email, password_hash, admin_tier
     FROM admins
     WHERE email = $1 OR phone = $2
     LIMIT 1`,
    [DEFAULT_SUPER_ADMIN.email, DEFAULT_SUPER_ADMIN.phone]
  );

  let recipientEmail = DEFAULT_SUPER_ADMIN.email;
  let passwordToSend = null;

  if (existing.length > 0) {
    const record = existing[0];
    recipientEmail = record.email || recipientEmail;
    const updates = [];
    const values = [];

    if (record.admin_tier !== "super_admin") {
      updates.push(`admin_tier = 'super_admin'`);
    }

    if (!record.password_hash) {
      passwordToSend = generatePassword();
      values.push(hashPassword(passwordToSend));
      updates.push(`password_hash = $${values.length}`);
    }

    if (updates.length > 0) {
      values.push(record.id);
      await client.query(
        `UPDATE admins SET ${updates.join(", ")} WHERE id = $${values.length}`,
        values
      );
    }
  } else {
    passwordToSend = generatePassword();
    await client.query(
      `INSERT INTO admins (name, phone, email, password_hash, admin_tier, status)
       VALUES ($1, $2, $3, $4, 'super_admin', 'active')`,
      [
        DEFAULT_SUPER_ADMIN.name,
        DEFAULT_SUPER_ADMIN.phone,
        DEFAULT_SUPER_ADMIN.email,
        hashPassword(passwordToSend),
      ]
    );
  }

  if (!passwordToSend) {
    console.log("✅ Super admin already existed with a password.");
    return;
  }

  try {
    const sent = await sendPasswordEmail(recipientEmail, passwordToSend);
    if (sent) {
      console.log("✅ Super admin password emailed.");
    } else {
      console.warn("⚠️ Email not sent. Temporary super admin password:", passwordToSend);
    }
  } catch (err) {
    console.error("❌ Failed to send super admin email:", err.message);
    console.warn("⚠️ Temporary super admin password:", passwordToSend);
  }
}

async function createUpdatedAtInfrastructure(client) {
  await client.query(`
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = NOW();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  const triggerTables = [
    "admins",
    "admin_billing_settings",
    "business_type_change_requests",
    "signup_verifications",
    "contacts",
    "leads",
    "tasks",
    "appointments",
    "orders",
    "order_revenue",
    "order_payment_link_timers",
    "admin_payment_links",
    "broadcasts",
    "templates",
    "catalog_items",
  ];

  for (const table of triggerTables) {
    const triggerName = `${table}_set_updated_at`;
    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
    await client.query(`
      CREATE TRIGGER ${triggerName}
      BEFORE UPDATE ON ${table}
      FOR EACH ROW
      EXECUTE FUNCTION set_updated_at()
    `);
  }
}

async function recreateSchema(client) {
  const dropStatements = [
    "DROP TABLE IF EXISTS admin_payment_links CASCADE",
    "DROP TABLE IF EXISTS admin_ai_usage CASCADE",
    "DROP TABLE IF EXISTS admin_billing_settings CASCADE",
    "DROP TABLE IF EXISTS appointment_payments CASCADE",
    "DROP TABLE IF EXISTS appointment_billing CASCADE",
    "DROP TABLE IF EXISTS order_payments CASCADE",
    "DROP TABLE IF EXISTS order_billing CASCADE",
    "DROP TABLE IF EXISTS order_payment_link_timers CASCADE",
    "DROP TABLE IF EXISTS order_revenue CASCADE",
    "DROP TABLE IF EXISTS tasks CASCADE",
    "DROP TABLE IF EXISTS leads CASCADE",
    "DROP TABLE IF EXISTS messages CASCADE",
    "DROP TABLE IF EXISTS business_type_change_requests CASCADE",
    "DROP TABLE IF EXISTS catalog_items CASCADE",
    "DROP TABLE IF EXISTS appointments CASCADE",
    "DROP TABLE IF EXISTS orders CASCADE",
    "DROP TABLE IF EXISTS broadcasts CASCADE",
    "DROP TABLE IF EXISTS templates CASCADE",
    "DROP TABLE IF EXISTS signup_verifications CASCADE",
    "DROP TABLE IF EXISTS contacts CASCADE",
    "DROP TABLE IF EXISTS admins CASCADE",
    "DROP TABLE IF EXISTS requirements CASCADE",
    "DROP TABLE IF EXISTS contact_messages CASCADE",
    "DROP TABLE IF EXISTS services_products CASCADE",
    "DROP TABLE IF EXISTS user_needs CASCADE",
    "DROP TABLE IF EXISTS user_requirements CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
    "DROP TABLE IF EXISTS admin_accounts CASCADE",
    "DROP TABLE IF EXISTS message_templates CASCADE",
    "DROP TABLE IF EXISTS admin_catalog_items CASCADE",
  ];

  for (const sql of dropStatements) {
    await client.query(sql);
  }
}

async function createSchema(client) {
  const queries = [
    `
    CREATE TABLE IF NOT EXISTS admins (
      id SERIAL PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      phone VARCHAR(20) UNIQUE NOT NULL,
      email VARCHAR(150) UNIQUE,
      password_hash TEXT,
      admin_tier VARCHAR(20) NOT NULL DEFAULT 'client_admin'
        CHECK (admin_tier IN ('super_admin', 'client_admin')),
      status VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'inactive')),
      business_name VARCHAR(140),
      business_category VARCHAR(120),
      business_type VARCHAR(20) DEFAULT 'both'
        CHECK (business_type IN ('product', 'service', 'both')),
      service_label VARCHAR(60),
      product_label VARCHAR(60),
      dashboard_subscription_expires_at TIMESTAMPTZ,
      booking_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      business_address TEXT,
      business_hours VARCHAR(160),
      business_map_url TEXT,
      access_expires_at TIMESTAMPTZ,
      automation_enabled BOOLEAN NOT NULL DEFAULT TRUE,
      automation_trigger_mode VARCHAR(20) NOT NULL DEFAULT 'any'
        CHECK (automation_trigger_mode IN ('any', 'keyword')),
      automation_trigger_keyword VARCHAR(40),
      whatsapp_number VARCHAR(20),
      whatsapp_name VARCHAR(100),
      whatsapp_connected_at TIMESTAMPTZ,
      ai_enabled BOOLEAN DEFAULT FALSE,
      ai_prompt TEXT,
      ai_blocklist TEXT,
      appointment_start_hour SMALLINT NOT NULL DEFAULT 9
        CHECK (appointment_start_hour >= 0 AND appointment_start_hour <= 23),
      appointment_end_hour SMALLINT NOT NULL DEFAULT 20
        CHECK (appointment_end_hour >= 1 AND appointment_end_hour <= 24),
      appointment_slot_minutes SMALLINT NOT NULL DEFAULT 60
        CHECK (appointment_slot_minutes >= 15 AND appointment_slot_minutes <= 240),
      appointment_window_months SMALLINT NOT NULL DEFAULT 3
        CHECK (appointment_window_months >= 1 AND appointment_window_months <= 24),
      free_delivery_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      free_delivery_min_amount NUMERIC(10,2),
      free_delivery_scope VARCHAR(30) NOT NULL DEFAULT 'combined',
      whatsapp_service_limit SMALLINT NOT NULL DEFAULT 3,
      whatsapp_product_limit SMALLINT NOT NULL DEFAULT 3,
      free_input_tokens INT NOT NULL DEFAULT 100000,
      free_output_tokens INT NOT NULL DEFAULT 100000,
      paid_input_tokens INT NOT NULL DEFAULT 0,
      paid_output_tokens INT NOT NULL DEFAULT 0,
      free_tokens_reset_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      reset_token_hash TEXT,
      reset_expires_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS admins_tier_idx ON admins (admin_tier)`,
    `CREATE INDEX IF NOT EXISTS admins_phone_idx ON admins (phone)`,
    `CREATE INDEX IF NOT EXISTS admins_email_lower_idx ON admins (LOWER(email))`,

    `
    CREATE TABLE IF NOT EXISTS signup_verifications (
      id SERIAL PRIMARY KEY,
      email VARCHAR(150) UNIQUE NOT NULL,
      code_hash TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      attempts INT NOT NULL DEFAULT 0,
      expires_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS signup_verifications_expires_idx ON signup_verifications (expires_at)`,

    `
    CREATE TABLE IF NOT EXISTS contacts (
      id SERIAL PRIMARY KEY,
      phone VARCHAR(20) UNIQUE NOT NULL,
      name VARCHAR(100),
      email VARCHAR(150),
      assigned_admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      previous_owner_admin VARCHAR(180),
      automation_disabled BOOLEAN NOT NULL DEFAULT FALSE,
      automation_activated BOOLEAN NOT NULL DEFAULT TRUE,
      automation_activated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS contacts_admin_idx ON contacts (assigned_admin_id)`,
    `CREATE INDEX IF NOT EXISTS contacts_created_idx ON contacts (created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS contacts_email_lower_idx ON contacts (LOWER(email))`,

    `
    CREATE TABLE IF NOT EXISTS admin_billing_settings (
      admin_id INT PRIMARY KEY REFERENCES admins(id) ON DELETE CASCADE,
      razorpay_key_id VARCHAR(120),
      razorpay_key_secret VARCHAR(160),
      charge_enabled BOOLEAN NOT NULL DEFAULT TRUE,
      free_until TIMESTAMPTZ,
      input_price_usd_per_1m NUMERIC(12,4),
      output_price_usd_per_1m NUMERIC(12,4),
      dashboard_charge_enabled BOOLEAN NOT NULL DEFAULT TRUE,
      dashboard_service_inr NUMERIC(12,2),
      dashboard_product_inr NUMERIC(12,2),
      dashboard_both_inr NUMERIC(12,2),
      dashboard_booking_inr NUMERIC(12,2),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS admin_billing_settings_charge_idx ON admin_billing_settings (charge_enabled)`,

    `
    CREATE TABLE IF NOT EXISTS admin_ai_usage (
      id SERIAL PRIMARY KEY,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      contact_id INT REFERENCES contacts(id) ON DELETE SET NULL,
      model VARCHAR(80),
      input_tokens INT NOT NULL DEFAULT 0,
      output_tokens INT NOT NULL DEFAULT 0,
      billable_input_tokens INT NOT NULL DEFAULT 0,
      billable_output_tokens INT NOT NULL DEFAULT 0,
      input_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
      output_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
      total_cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
      usd_to_inr_rate NUMERIC(10,4),
      total_cost_inr NUMERIC(12,4) NOT NULL DEFAULT 0,
      is_billable BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS admin_ai_usage_admin_created_idx ON admin_ai_usage (admin_id, created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS admin_payment_links (
      id SERIAL PRIMARY KEY,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      payment_link_id VARCHAR(120) UNIQUE NOT NULL,
      amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      base_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      maintenance_fee NUMERIC(12,2) NOT NULL DEFAULT 0,
      currency VARCHAR(10) NOT NULL DEFAULT 'INR',
      purpose VARCHAR(30) NOT NULL DEFAULT 'payg',
      input_tokens INT NOT NULL DEFAULT 0,
      output_tokens INT NOT NULL DEFAULT 0,
      subscription_months INT NOT NULL DEFAULT 0,
      discount_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
      dashboard_monthly_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      status VARCHAR(20) NOT NULL DEFAULT 'created'
        CHECK (status IN ('created', 'paid', 'failed', 'cancelled', 'expired', 'pending')),
      paid_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      paid_at TIMESTAMPTZ,
      raw_json JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS admin_payment_links_admin_idx ON admin_payment_links (admin_id, created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS business_type_change_requests (
      id SERIAL PRIMARY KEY,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      current_business_type VARCHAR(20) NOT NULL
        CHECK (current_business_type IN ('product', 'service', 'both')),
      requested_business_type VARCHAR(20) NOT NULL
        CHECK (requested_business_type IN ('product', 'service', 'both')),
      reason TEXT,
      status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'cancelled')),
      monthly_current_inr NUMERIC(12,2),
      monthly_requested_inr NUMERIC(12,2),
      monthly_delta_inr NUMERIC(12,2),
      payment_required BOOLEAN NOT NULL DEFAULT FALSE,
      payment_status VARCHAR(20) NOT NULL DEFAULT 'unpaid'
        CHECK (payment_status IN ('unpaid', 'paid', 'waived')),
      payment_link_id VARCHAR(120),
      payment_link_url TEXT,
      payment_paid_amount NUMERIC(12,2),
      payment_paid_at TIMESTAMPTZ,
      resolved_by INT REFERENCES admins(id) ON DELETE SET NULL,
      resolved_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS business_type_change_requests_admin_idx ON business_type_change_requests (admin_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS business_type_change_requests_status_idx ON business_type_change_requests (status, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS business_type_change_requests_payment_idx ON business_type_change_requests (payment_link_id)`,

    `
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      message_text TEXT NOT NULL,
      message_type VARCHAR(20) NOT NULL CHECK (message_type IN ('incoming', 'outgoing')),
      status VARCHAR(20) NOT NULL DEFAULT 'sent' CHECK (status IN ('sent', 'delivered', 'read')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS messages_user_created_idx ON messages (user_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS messages_admin_created_idx ON messages (admin_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS messages_type_created_idx ON messages (message_type, created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS leads (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
      requirement_text TEXT NOT NULL,
      category VARCHAR(100),
      reason_of_contacting TEXT,
      status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_progress', 'completed')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS leads_user_status_idx ON leads (user_id, status)`,
    `CREATE INDEX IF NOT EXISTS leads_created_idx ON leads (created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS tasks (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
      need_text TEXT NOT NULL,
      priority VARCHAR(20) NOT NULL DEFAULT 'medium'
        CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
      status VARCHAR(20) NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'assigned', 'completed')),
      assigned_to INT REFERENCES admins(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS tasks_user_status_idx ON tasks (user_id, status)`,
    `CREATE INDEX IF NOT EXISTS tasks_created_idx ON tasks (created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS appointments (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      appointment_type VARCHAR(100),
      appointment_kind VARCHAR(20) NOT NULL DEFAULT 'service'
        CHECK (appointment_kind IN ('service', 'booking')),
      start_time TIMESTAMPTZ NOT NULL,
      end_time TIMESTAMPTZ NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'booked'
        CHECK (status IN ('booked', 'completed', 'cancelled')),
      payment_total NUMERIC(10,2),
      payment_paid NUMERIC(10,2) DEFAULT 0,
      payment_method VARCHAR(30),
      payment_currency VARCHAR(10) DEFAULT 'INR',
      payment_notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT appointments_payment_total_nonneg CHECK (payment_total IS NULL OR payment_total >= 0),
      CONSTRAINT appointments_payment_paid_nonneg CHECK (payment_paid IS NULL OR payment_paid >= 0)
    )
    `,
    `CREATE UNIQUE INDEX IF NOT EXISTS appointments_admin_start_idx ON appointments (admin_id, start_time)`,
    `CREATE INDEX IF NOT EXISTS appointments_user_idx ON appointments (user_id, start_time DESC)`,
    `CREATE INDEX IF NOT EXISTS appointments_status_start_idx ON appointments (status, start_time DESC)`,

    `
    CREATE TABLE IF NOT EXISTS orders (
      id SERIAL PRIMARY KEY,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      order_number VARCHAR(50),
      customer_name VARCHAR(150),
      customer_phone VARCHAR(50),
      customer_email VARCHAR(150),
      channel VARCHAR(50) DEFAULT 'WhatsApp',
      status VARCHAR(20) NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'confirmed', 'processing', 'packed', 'out_for_delivery', 'fulfilled', 'cancelled', 'refunded')),
      fulfillment_status VARCHAR(30) NOT NULL DEFAULT 'unfulfilled'
        CHECK (fulfillment_status IN ('unfulfilled', 'packed', 'shipped', 'delivered', 'cancelled')),
      delivery_method VARCHAR(30),
      delivery_address TEXT,
      items JSONB DEFAULT '[]'::jsonb,
      notes JSONB DEFAULT '[]'::jsonb,
      assigned_to VARCHAR(100),
      placed_at TIMESTAMPTZ,
      payment_total NUMERIC(10,2),
      payment_paid NUMERIC(10,2) DEFAULT 0,
      payment_status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (payment_status IN ('pending', 'paid', 'failed', 'refunded')),
      payment_method VARCHAR(30),
      payment_currency VARCHAR(10) DEFAULT 'INR',
      payment_notes TEXT,
      payment_transaction_id VARCHAR(120),
      payment_gateway_payment_id VARCHAR(120),
      payment_link_id VARCHAR(120),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT orders_payment_total_nonneg CHECK (payment_total IS NULL OR payment_total >= 0),
      CONSTRAINT orders_payment_paid_nonneg CHECK (payment_paid IS NULL OR payment_paid >= 0)
    )
    `,
    `CREATE INDEX IF NOT EXISTS orders_admin_created_idx ON orders (admin_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status)`,
    `CREATE INDEX IF NOT EXISTS orders_fulfillment_idx ON orders (fulfillment_status)`,
    `CREATE INDEX IF NOT EXISTS orders_payment_status_idx ON orders (payment_status)`,
    `ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_transaction_id VARCHAR(120)`,
    `ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_gateway_payment_id VARCHAR(120)`,
    `ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_link_id VARCHAR(120)`,

    `
    CREATE TABLE IF NOT EXISTS order_revenue (
      id SERIAL PRIMARY KEY,
      order_id INT NOT NULL UNIQUE REFERENCES orders(id) ON DELETE CASCADE,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      channel VARCHAR(50) DEFAULT 'WhatsApp',
      payment_currency VARCHAR(10) DEFAULT 'INR',
      booked_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      collected_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      outstanding_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
      payment_status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (payment_status IN ('pending', 'paid', 'failed', 'refunded')),
      payment_method VARCHAR(30),
      revenue_date DATE NOT NULL DEFAULT CURRENT_DATE,
      placed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT order_revenue_booked_nonneg CHECK (booked_amount >= 0),
      CONSTRAINT order_revenue_collected_nonneg CHECK (collected_amount >= 0),
      CONSTRAINT order_revenue_outstanding_nonneg CHECK (outstanding_amount >= 0)
    )
    `,
    `CREATE INDEX IF NOT EXISTS order_revenue_admin_date_idx ON order_revenue (admin_id, revenue_date DESC)`,
    `CREATE INDEX IF NOT EXISTS order_revenue_channel_idx ON order_revenue (channel)`,
    `CREATE INDEX IF NOT EXISTS order_revenue_payment_status_idx ON order_revenue (payment_status)`,

    `
    CREATE TABLE IF NOT EXISTS order_payment_link_timers (
      order_id INT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      scheduled_for TIMESTAMPTZ NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'scheduled'
        CHECK (status IN ('scheduled', 'processing', 'sent', 'failed', 'cancelled')),
      attempts INT NOT NULL DEFAULT 0,
      max_attempts INT NOT NULL DEFAULT 3,
      last_error TEXT,
      payload_json JSONB,
      last_payment_link_id VARCHAR(120),
      created_by INT REFERENCES admins(id) ON DELETE SET NULL,
      processing_started_at TIMESTAMPTZ,
      sent_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT order_payment_link_timers_attempts_nonneg CHECK (attempts >= 0),
      CONSTRAINT order_payment_link_timers_max_attempts_positive CHECK (max_attempts >= 1)
    )
    `,
    `CREATE INDEX IF NOT EXISTS order_payment_link_timers_status_due_idx ON order_payment_link_timers (status, scheduled_for ASC)`,
    `CREATE INDEX IF NOT EXISTS order_payment_link_timers_admin_idx ON order_payment_link_timers (admin_id, status)`,

    `
    CREATE TABLE IF NOT EXISTS broadcasts (
      id SERIAL PRIMARY KEY,
      title VARCHAR(150) NOT NULL,
      message TEXT NOT NULL,
      target_audience_type VARCHAR(50) DEFAULT 'all',
      scheduled_at TIMESTAMPTZ,
      status VARCHAR(20) NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'scheduled', 'sent', 'failed')),
      sent_count INT NOT NULL DEFAULT 0,
      delivered_count INT NOT NULL DEFAULT 0,
      created_by INT REFERENCES admins(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS broadcasts_status_idx ON broadcasts (status)`,
    `CREATE INDEX IF NOT EXISTS broadcasts_created_by_idx ON broadcasts (created_by)`,
    `CREATE INDEX IF NOT EXISTS broadcasts_created_idx ON broadcasts (created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS templates (
      id SERIAL PRIMARY KEY,
      name VARCHAR(150) NOT NULL,
      category VARCHAR(100) NOT NULL,
      content TEXT NOT NULL,
      variables_json TEXT,
      created_by INT REFERENCES admins(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS templates_category_idx ON templates (category)`,
    `CREATE INDEX IF NOT EXISTS templates_created_by_idx ON templates (created_by)`,
    `CREATE INDEX IF NOT EXISTS templates_created_idx ON templates (created_at DESC)`,

    `
    CREATE TABLE IF NOT EXISTS catalog_items (
      id SERIAL PRIMARY KEY,
      admin_id INT NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      item_type VARCHAR(20) NOT NULL CHECK (item_type IN ('service', 'product')),
      name VARCHAR(150) NOT NULL,
      category VARCHAR(100),
      description TEXT,
      price_label VARCHAR(60),
      duration_value NUMERIC(10,2),
      duration_unit VARCHAR(20),
      duration_minutes INT,
      quantity_value NUMERIC(10,3),
      quantity_unit VARCHAR(40),
      details_prompt TEXT,
      keywords TEXT,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      sort_order INT NOT NULL DEFAULT 0,
      is_bookable BOOLEAN NOT NULL DEFAULT FALSE,
      is_booking_item BOOLEAN NOT NULL DEFAULT FALSE,
      payment_required BOOLEAN NOT NULL DEFAULT FALSE,
      free_delivery_eligible BOOLEAN NOT NULL DEFAULT FALSE,
      show_on_whatsapp BOOLEAN NOT NULL DEFAULT TRUE,
      whatsapp_sort_order INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `,
    `CREATE INDEX IF NOT EXISTS catalog_items_admin_idx ON catalog_items (admin_id)`,
    `CREATE INDEX IF NOT EXISTS catalog_items_admin_type_idx ON catalog_items (admin_id, item_type)`,
    `CREATE INDEX IF NOT EXISTS catalog_items_admin_active_idx ON catalog_items (admin_id, is_active)`,
    `CREATE INDEX IF NOT EXISTS catalog_items_admin_sort_idx ON catalog_items (admin_id, sort_order, name)`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS duration_value NUMERIC(10,2)`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS duration_unit VARCHAR(20)`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS is_booking_item BOOLEAN NOT NULL DEFAULT FALSE`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS payment_required BOOLEAN NOT NULL DEFAULT FALSE`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS free_delivery_eligible BOOLEAN NOT NULL DEFAULT FALSE`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS booking_enabled BOOLEAN NOT NULL DEFAULT FALSE`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS automation_trigger_mode VARCHAR(20) NOT NULL DEFAULT 'any'`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS automation_trigger_keyword VARCHAR(40)`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_enabled BOOLEAN NOT NULL DEFAULT FALSE`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_min_amount NUMERIC(10,2)`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS free_delivery_scope VARCHAR(30) NOT NULL DEFAULT 'combined'`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS whatsapp_service_limit SMALLINT NOT NULL DEFAULT 3`,
    `ALTER TABLE admins ADD COLUMN IF NOT EXISTS whatsapp_product_limit SMALLINT NOT NULL DEFAULT 3`,
    `ALTER TABLE contacts ADD COLUMN IF NOT EXISTS automation_activated BOOLEAN NOT NULL DEFAULT TRUE`,
    `ALTER TABLE contacts ADD COLUMN IF NOT EXISTS automation_activated_at TIMESTAMPTZ`,
    `ALTER TABLE appointments ADD COLUMN IF NOT EXISTS appointment_kind VARCHAR(20) NOT NULL DEFAULT 'service'`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS quantity_value NUMERIC(10,3)`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(40)`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS show_on_whatsapp BOOLEAN NOT NULL DEFAULT TRUE`,
    `ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS whatsapp_sort_order INT NOT NULL DEFAULT 0`,
    `UPDATE catalog_items
     SET duration_value = duration_minutes,
         duration_unit = COALESCE(NULLIF(duration_unit, ''), 'minutes')
     WHERE duration_minutes IS NOT NULL
       AND duration_value IS NULL`,
  ];

  for (const sql of queries) {
    await client.query(sql);
  }

  await createUpdatedAtInfrastructure(client);
}

export async function initDatabase({ recreate = false } = {}) {
  let client = null;
  try {
    if (!DATABASE_URL) {
      throw new Error("DATABASE_URL is required for Postgres");
    }

    client = new Client({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });

    await client.connect();
    console.log("✅ Postgres connected");

    await client.query("BEGIN");

    if (recreate) {
      await recreateSchema(client);
      console.log("🧱 Existing schema dropped.");
    }

    await createSchema(client);
    await ensureDefaultSuperAdmin(client);

    await client.query("COMMIT");

    console.log("✅ Database schema is ready");
  } catch (err) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {
        // Ignore rollback failures to preserve original error.
      }
    }
    console.error("❌ Database init failed:", err.message);
    throw err;
  } finally {
    if (client) {
      await client.end().catch(() => {});
    }
  }
}

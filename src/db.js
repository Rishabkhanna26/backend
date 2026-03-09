import pg from "pg";
import dotenv from "dotenv";

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const formatQuery = (text, params = []) => {
  if (!params.length) return text;
  let index = 0;
  return text.replace(/\?/g, () => `$${++index}`);
};

export const db = {
  query: async (text, params = []) => {
    const sql = formatQuery(text, params);
    const result = await pool.query(sql, params);
    return [result.rows, result];
  },
};

console.log("✅ Postgres pool initialized");

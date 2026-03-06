import "dotenv/config";
import { initDatabase } from "./init.js";

const recreate = process.env.DB_RECREATE === "true";

console.log(
  recreate
    ? "🚀 Recreating database schema (no dummy data)..."
    : "🚀 Initializing database schema (no dummy data)..."
);

initDatabase({ recreate })
  .then(() => {
    console.log("✅ Database ready (no dummy data seeded).");
    process.exit(0);
  })
  .catch((error) => {
    console.error("❌ Database setup failed:", error?.message || error);
    process.exit(1);
  });

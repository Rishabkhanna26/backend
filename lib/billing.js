export const DEFAULT_OPENAI_MODEL = "gpt-4o";
export const DEFAULT_INPUT_USD_PER_1M = 5;
export const DEFAULT_OUTPUT_USD_PER_1M = 10;
export const DEFAULT_USD_TO_INR_RATE = 85;
export const DEFAULT_FREE_TOKENS = 200000;
export const FREE_INPUT_TOKENS_PER_MONTH = 100000;
export const FREE_OUTPUT_TOKENS_PER_MONTH = 100000;
export const FREE_INPUT_TOKEN_CAP = 500000;
export const FREE_OUTPUT_TOKEN_CAP = 500000;
export const MIN_PREPAID_TOPUP_INR = 500;
export const SERVICE_FEE_RATE = 0.05;
export const RAZORPAY_FEE_RATE = 0.02;
export const MAINTENANCE_FEE_RATE = 0.12;

const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const roundMoney = (value, digits = 2) => {
  const num = toNumber(value, 0);
  return Number(num.toFixed(digits));
};

export const estimateTokens = (text = "") => {
  const normalized = String(text || "");
  const estimate = Math.ceil(normalized.length / 4);
  return Math.max(1, estimate);
};

export const computeUsageCosts = ({
  inputTokens = 0,
  outputTokens = 0,
  inputUsdPer1M = DEFAULT_INPUT_USD_PER_1M,
  outputUsdPer1M = DEFAULT_OUTPUT_USD_PER_1M,
  usdToInrRate = DEFAULT_USD_TO_INR_RATE,
} = {}) => {
  const safeInputTokens = Math.max(0, toNumber(inputTokens, 0));
  const safeOutputTokens = Math.max(0, toNumber(outputTokens, 0));
  const inputUsd = (safeInputTokens / 1_000_000) * toNumber(inputUsdPer1M, DEFAULT_INPUT_USD_PER_1M);
  const outputUsd = (safeOutputTokens / 1_000_000) * toNumber(outputUsdPer1M, DEFAULT_OUTPUT_USD_PER_1M);
  const totalUsd = inputUsd + outputUsd;
  const totalInr = totalUsd * toNumber(usdToInrRate, DEFAULT_USD_TO_INR_RATE);
  return {
    inputCostUsd: roundMoney(inputUsd, 6),
    outputCostUsd: roundMoney(outputUsd, 6),
    totalCostUsd: roundMoney(totalUsd, 6),
    totalCostInr: roundMoney(totalInr, 4),
  };
};

export const computePaymentTotals = ({
  baseInr = 0,
  serviceFeeRate = SERVICE_FEE_RATE,
  razorpayFeeRate = RAZORPAY_FEE_RATE,
} = {}) => {
  const base = Math.max(0, toNumber(baseInr, 0));
  const serviceFee = base * toNumber(serviceFeeRate, SERVICE_FEE_RATE);
  const subtotal = base + serviceFee;
  const razorpayFee = subtotal * toNumber(razorpayFeeRate, RAZORPAY_FEE_RATE);
  const total = subtotal + razorpayFee;
  return {
    baseInr: roundMoney(base, 2),
    serviceFeeInr: roundMoney(serviceFee, 2),
    razorpayFeeInr: roundMoney(razorpayFee, 2),
    subtotalInr: roundMoney(subtotal, 2),
    totalInr: roundMoney(total, 2),
  };
};

export const computeMaintenanceTotals = ({
  baseInr = 0,
  maintenanceRate = MAINTENANCE_FEE_RATE,
} = {}) => {
  const base = Math.max(0, toNumber(baseInr, 0));
  const fee = base * toNumber(maintenanceRate, MAINTENANCE_FEE_RATE);
  const total = base + fee;
  return {
    baseInr: roundMoney(base, 2),
    maintenanceFeeInr: roundMoney(fee, 2),
    totalInr: roundMoney(total, 2),
  };
};

const APPEARANCE_STORAGE_KEY = 'aa:accent-color';
const APPEARANCE_THEME_KEY = 'aa:theme';
const DEFAULT_ACCENT_COLOR = '#FF6B00';
const DEFAULT_THEME = 'light';
const ACCENT_COLORS = [
  '#FF6B00',
  '#0A1F44',
  '#4CAF50',
  '#2196F3',
  '#9C27B0',
  '#F44336',
];
const THEMES = ['light', 'dark'];

const getThemeStorageKey = (userId) => {
  if (userId === null || userId === undefined || userId === '') {
    return APPEARANCE_THEME_KEY;
  }
  return `${APPEARANCE_THEME_KEY}:${userId}`;
};

const normalizeHex = (value) => {
  if (!value || typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  let hex = trimmed.startsWith('#') ? trimmed.slice(1) : trimmed;
  if (!/^[0-9a-fA-F]{3}$|^[0-9a-fA-F]{6}$/.test(hex)) return null;
  if (hex.length === 3) {
    hex = hex
      .split('')
      .map((char) => char + char)
      .join('');
  }
  return `#${hex.toUpperCase()}`;
};

const hexToRgbChannels = (hex) => {
  const normalized = normalizeHex(hex);
  if (!normalized) return null;
  const raw = normalized.slice(1);
  const red = parseInt(raw.slice(0, 2), 16);
  const green = parseInt(raw.slice(2, 4), 16);
  const blue = parseInt(raw.slice(4, 6), 16);
  return `${red} ${green} ${blue}`;
};

const applyAccentColor = (hex) => {
  if (typeof document === 'undefined') return;
  const channels = hexToRgbChannels(hex);
  if (!channels) return;
  document.documentElement.style.setProperty('--aa-orange', channels);
};

const applyTheme = (theme) => {
  if (typeof document === 'undefined') return;
  const normalized = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.dataset.theme = normalized;
};

const storeAccentColor = (hex) => {
  if (typeof window === 'undefined') return;
  const normalized = normalizeHex(hex);
  if (!normalized) return;
  window.localStorage.setItem(APPEARANCE_STORAGE_KEY, normalized);
};

const storeTheme = (theme, userId) => {
  if (typeof window === 'undefined') return;
  const normalized = theme === 'dark' ? 'dark' : 'light';
  const key = getThemeStorageKey(userId);
  window.localStorage.setItem(key, normalized);
};

const getStoredAccentColor = () => {
  if (typeof window === 'undefined') return null;
  return normalizeHex(window.localStorage.getItem(APPEARANCE_STORAGE_KEY));
};

const getStoredTheme = (userId) => {
  if (typeof window === 'undefined') return null;
  const key = getThemeStorageKey(userId);
  const stored = window.localStorage.getItem(key);
  if (THEMES.includes(stored)) return stored;
  return null;
};

export {
  APPEARANCE_STORAGE_KEY,
  APPEARANCE_THEME_KEY,
  DEFAULT_ACCENT_COLOR,
  DEFAULT_THEME,
  ACCENT_COLORS,
  THEMES,
  getThemeStorageKey,
  applyAccentColor,
  applyTheme,
  storeAccentColor,
  storeTheme,
  getStoredAccentColor,
  getStoredTheme,
};

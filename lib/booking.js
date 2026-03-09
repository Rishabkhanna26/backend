export const BOOKING_CATEGORY_PRESETS = Object.freeze([
  {
    id: 'hotel',
    label: 'Hotel',
    description: 'Use this for rooms, suites, stay offers, dining add-ons, and cabanas.',
    examples: ['Deluxe Room', 'Family Suite', 'Rooftop Dinner', 'Poolside Cabana'],
    searchTerms: [
      'hotel',
      'room',
      'rooms',
      'deluxe room',
      'super deluxe',
      'suite',
      'family suite',
      'honeymoon',
      'honeymoon suite',
      'stay',
      'night stay',
      'reservation',
      'rooftop dinner',
      'cabana',
      'poolside cabana',
    ],
  },
  {
    id: 'restaurant',
    label: 'Restaurant',
    description: 'Use this for table booking, dining reservations, and private table options.',
    examples: ['2-Seater Table', '6-Seater Family Table', 'Private Event Table'],
    searchTerms: [
      'restaurant',
      'table',
      'table booking',
      'dining',
      'dinner',
      'lunch',
      'reservation',
      '2-seater table',
      '2 seater table',
      '6-seater family table',
      '6 seater family table',
      'family table',
      'private event table',
      'private table',
    ],
  },
  {
    id: 'events',
    label: 'Events',
    description: 'Use this for banquet halls, conference spaces, party bookings, and venues.',
    examples: ['Banquet Slot', 'Conference Hall', 'Wedding Venue Booking'],
    searchTerms: [
      'event',
      'events',
      'banquet',
      'banquet slot',
      'conference',
      'conference hall',
      'meeting hall',
      'venue',
      'wedding',
      'wedding venue',
      'party hall',
      'hall booking',
      'venue booking',
    ],
  },
]);

const normalizeText = (value) => String(value || '').trim();

export const normalizeBookingCategoryKey = (value) => normalizeText(value).toLowerCase();

const PRESET_MAP = new Map(
  BOOKING_CATEGORY_PRESETS.map((preset) => [normalizeBookingCategoryKey(preset.label), preset])
);

const addTerm = (set, value) => {
  const text = normalizeText(value).toLowerCase();
  if (!text) return;
  set.add(text);
  text
    .split(/[^\p{L}\p{N}]+/u)
    .map((part) => part.trim())
    .filter((part) => part.length > 2)
    .forEach((part) => set.add(part));
};

export const getBookingCategoryPreset = (value) => {
  const key = normalizeBookingCategoryKey(value);
  if (!key) return null;
  return PRESET_MAP.get(key) || null;
};

export const resolveBookingCategoryLabel = (value, fallback = 'Booking') => {
  const raw = normalizeText(value);
  if (!raw) return fallback;
  if (normalizeBookingCategoryKey(raw) === 'booking') return 'Booking';
  const preset = getBookingCategoryPreset(raw);
  return preset ? preset.label : raw;
};

export const getBookingCategoryTerms = (value) => {
  const preset = getBookingCategoryPreset(value);
  if (!preset) return [];
  const terms = new Set();
  addTerm(terms, preset.label);
  preset.examples.forEach((example) => addTerm(terms, example));
  preset.searchTerms.forEach((term) => addTerm(terms, term));
  return Array.from(terms);
};

export const getBookingCustomCategories = (values = []) => {
  const presetKeys = new Set(
    BOOKING_CATEGORY_PRESETS.map((preset) => normalizeBookingCategoryKey(preset.label))
  );
  const seen = new Set();
  const customCategories = [];

  values.forEach((value) => {
    const label = resolveBookingCategoryLabel(value, '');
    const key = normalizeBookingCategoryKey(label);
    if (!label || key === 'booking' || presetKeys.has(key) || seen.has(key)) return;
    seen.add(key);
    customCategories.push(label);
  });

  return customCategories.sort((a, b) => a.localeCompare(b));
};

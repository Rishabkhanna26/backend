const BUSINESS_TYPES = new Set(['product', 'service', 'both']);

export const normalizeBusinessType = (value, fallback = 'both') => {
  const normalized = String(value || '').trim().toLowerCase();
  if (BUSINESS_TYPES.has(normalized)) return normalized;
  return fallback;
};

export const resolveBusinessType = (valueOrUser) => {
  if (typeof valueOrUser === 'string') {
    return normalizeBusinessType(valueOrUser);
  }
  if (valueOrUser && typeof valueOrUser === 'object') {
    if (valueOrUser.business_type) {
      return normalizeBusinessType(valueOrUser.business_type);
    }
    return 'both';
  }
  return 'both';
};

export const hasProductAccess = (valueOrUser) => {
  if (valueOrUser && typeof valueOrUser === 'object' && valueOrUser.admin_tier === 'super_admin') {
    return true;
  }
  const type = resolveBusinessType(valueOrUser);
  return type === 'product' || type === 'both';
};

export const hasServiceAccess = (valueOrUser) => {
  if (valueOrUser && typeof valueOrUser === 'object' && valueOrUser.admin_tier === 'super_admin') {
    return true;
  }
  const type = resolveBusinessType(valueOrUser);
  return type === 'service' || type === 'both';
};

export const getBusinessTypeLabel = (valueOrUser) => {
  const type = resolveBusinessType(valueOrUser);
  if (type === 'product') return 'Product-based';
  if (type === 'service') return 'Service-based';
  return 'Product + Service';
};

export const getCatalogLabel = (valueOrUser) => {
  const type = resolveBusinessType(valueOrUser);
  if (type === 'product') return 'Products';
  if (type === 'service') return 'Services';
  return 'Products & Services';
};

export const canUseCatalogItemType = (valueOrUser, itemType) => {
  const normalizedType = String(itemType || '').trim().toLowerCase();
  if (!['product', 'service'].includes(normalizedType)) return false;
  if (normalizedType === 'product') return hasProductAccess(valueOrUser);
  return hasServiceAccess(valueOrUser);
};

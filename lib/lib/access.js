export const PUBLIC_PATHS = ['/login', '/signup', '/payment/success'];

export const ROLE_PATHS = {
  super_admin: null,
  client_admin: null,
};

export function normalizePath(pathname = '') {
  if (pathname.length > 1 && pathname.endsWith('/')) {
    return pathname.slice(0, -1);
  }
  return pathname;
}

export function isPathAllowed(role, pathname) {
  const current = normalizePath(pathname);
  if (PUBLIC_PATHS.includes(current)) return true;
  if (current === '/') return true;
  return Boolean(role);
}

export function filterMenuItems(role, items) {
  if (!role) return [];
  return items.filter((item) => {
    if (!item?.roles || item.roles.length === 0) return true;
    return item.roles.includes(role);
  });
}

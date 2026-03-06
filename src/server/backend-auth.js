export const parseAdminId = (value) => {
  const normalized = String(value ?? "").trim();
  if (!normalized) return null;
  const adminId = Number(normalized);
  return Number.isInteger(adminId) && adminId > 0 ? adminId : null;
};

export const extractBearerToken = (value) => {
  const header = String(value || "");
  if (!header.toLowerCase().startsWith("bearer ")) return "";
  return header.slice(7).trim();
};

export const verifyBackendAuthPayload = (payload, backendScope = "backend") => {
  const adminId = parseAdminId(payload?.id);
  if (!payload || payload?.scope !== backendScope || adminId == null) {
    return null;
  }
  return payload;
};

export const getScopedAdminIdFromRequest = (req) => {
  const queryAdminId = parseAdminId(req.query?.adminId);
  if (queryAdminId != null) return queryAdminId;

  const bodyAdminId = parseAdminId(req.body?.adminId);
  if (bodyAdminId != null) return bodyAdminId;

  const authAdminId = parseAdminId(req.backendAuth?.id);
  return authAdminId != null ? authAdminId : undefined;
};

export const createRequireBackendAuth = ({ verifyAuthToken, backendScope = "backend" }) => {
  return (req, res, next) => {
    const token = extractBearerToken(req.headers.authorization);
    const payload = verifyBackendAuthPayload(verifyAuthToken(token), backendScope);

    if (!payload) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }

    const requestedAdminId = parseAdminId(req.query?.adminId ?? req.body?.adminId);
    if (
      requestedAdminId != null &&
      payload.admin_tier !== "super_admin" &&
      requestedAdminId !== parseAdminId(payload.id)
    ) {
      res.status(403).json({ error: "Forbidden" });
      return;
    }

    req.backendAuth = payload;
    next();
  };
};

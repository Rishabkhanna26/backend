import rateLimit from 'express-rate-limit';
import helmet from 'helmet';
import logger from '../config/logger.js';

// Rate limiting configuration
export const createRateLimiter = (options = {}) => {
  const defaults = {
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // Limit each IP to 100 requests per windowMs
    message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
    legacyHeaders: false, // Disable the `X-RateLimit-*` headers
    handler: (req, res) => {
      logger.warn('Rate limit exceeded', {
        ip: req.ip,
        path: req.path,
        method: req.method,
      });
      res.status(429).json({
        error: 'Too many requests',
        message: 'Please try again later',
      });
    },
  };

  return rateLimit({ ...defaults, ...options });
};

// Strict rate limiter for authentication endpoints
export const authRateLimiter = createRateLimiter({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 5, // 5 attempts per 15 minutes
  message: 'Too many authentication attempts, please try again later.',
  skipSuccessfulRequests: true, // Don't count successful requests
});

// API rate limiter
export const apiRateLimiter = createRateLimiter({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 60, // 60 requests per minute
});

// WhatsApp endpoint rate limiter
export const whatsappRateLimiter = createRateLimiter({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 30, // 30 requests per minute
});

// Helmet security headers
export const securityHeaders = helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      scriptSrc: ["'self'"],
      imgSrc: ["'self'", 'data:', 'https:'],
      connectSrc: ["'self'"],
      fontSrc: ["'self'"],
      objectSrc: ["'none'"],
      mediaSrc: ["'self'"],
      frameSrc: ["'none'"],
    },
  },
  crossOriginEmbedderPolicy: false, // Allow embedding for WhatsApp QR
  crossOriginResourcePolicy: { policy: 'cross-origin' },
});

// Request size limiter
export const requestSizeLimiter = (maxSize = '10mb') => {
  return (req, res, next) => {
    const contentLength = req.headers['content-length'];
    if (contentLength && parseInt(contentLength) > parseSize(maxSize)) {
      logger.warn('Request size exceeded', {
        ip: req.ip,
        path: req.path,
        size: contentLength,
      });
      return res.status(413).json({
        error: 'Request entity too large',
        message: `Maximum request size is ${maxSize}`,
      });
    }
    next();
  };
};

// Helper to parse size strings
function parseSize(size) {
  const units = { b: 1, kb: 1024, mb: 1024 * 1024, gb: 1024 * 1024 * 1024 };
  const match = size.toLowerCase().match(/^(\d+)(b|kb|mb|gb)?$/);
  if (!match) return 10 * 1024 * 1024; // Default 10MB
  return parseInt(match[1]) * (units[match[2]] || 1);
}

// IP whitelist middleware (for admin endpoints)
export const ipWhitelist = (allowedIPs = []) => {
  return (req, res, next) => {
    if (allowedIPs.length === 0) return next(); // No whitelist configured
    
    const clientIP = req.ip || req.connection.remoteAddress;
    if (!allowedIPs.includes(clientIP)) {
      logger.warn('IP not whitelisted', {
        ip: clientIP,
        path: req.path,
      });
      return res.status(403).json({
        error: 'Forbidden',
        message: 'Access denied',
      });
    }
    next();
  };
};

// Request logging middleware
export const requestLogger = (req, res, next) => {
  const start = Date.now();
  
  res.on('finish', () => {
    const duration = Date.now() - start;
    const logData = {
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration,
      ip: req.ip,
      userAgent: req.get('user-agent'),
    };
    
    if (res.statusCode >= 400) {
      logger.warn('HTTP request failed', logData);
    } else if (duration > 1000) {
      logger.warn('Slow HTTP request', logData);
    } else {
      logger.info('HTTP request', logData);
    }
  });
  
  next();
};

export default {
  createRateLimiter,
  authRateLimiter,
  apiRateLimiter,
  whatsappRateLimiter,
  securityHeaders,
  requestSizeLimiter,
  ipWhitelist,
  requestLogger,
};

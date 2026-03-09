import * as Sentry from '@sentry/node';

const SENTRY_DSN = process.env.SENTRY_DSN;
const NODE_ENV = process.env.NODE_ENV || 'development';

export function initSentry(app) {
  if (!SENTRY_DSN) {
    console.warn('⚠️ SENTRY_DSN not configured. Error tracking disabled.');
    return;
  }

  const integrations = [];
  if (typeof Sentry.httpIntegration === 'function') {
    integrations.push(Sentry.httpIntegration());
  } else if (Sentry.Integrations?.Http) {
    integrations.push(new Sentry.Integrations.Http({ tracing: true }));
  }

  if (typeof Sentry.expressIntegration === 'function') {
    integrations.push(Sentry.expressIntegration());
  }

  Sentry.init({
    dsn: SENTRY_DSN,
    environment: NODE_ENV,
    tracesSampleRate: NODE_ENV === 'production' ? 0.1 : 1.0,
    integrations,
  });

  // Legacy SDK support (v7/v8)
  if (app && Sentry.Handlers?.requestHandler) {
    app.use(Sentry.Handlers.requestHandler());
    if (Sentry.Handlers?.tracingHandler) {
      app.use(Sentry.Handlers.tracingHandler());
    }
  }

  console.log('✅ Sentry error tracking initialized');
}

export function sentryErrorHandler() {
  if (!SENTRY_DSN) {
    return (err, req, res, next) => next(err);
  }

  if (typeof Sentry.expressErrorHandler === 'function') {
    return Sentry.expressErrorHandler();
  }

  if (Sentry.Handlers?.errorHandler) {
    return Sentry.Handlers.errorHandler();
  }

  return (err, req, res, next) => next(err);
}

export { Sentry };

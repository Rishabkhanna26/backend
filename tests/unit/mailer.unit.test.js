import { describe, it, expect } from 'vitest';
import { normalizeSmtpPassword, normalizeSmtpEmail } from '../../lib/mailer.js';

describe('mailer normalization', () => {
  it('trims smtp email', () => {
    expect(normalizeSmtpEmail('  test@example.com  ')).toBe('test@example.com');
  });

  it('removes whitespace in smtp password', () => {
    expect(normalizeSmtpPassword('ab cd\nef\tgh')).toBe('abcdefgh');
  });

  it('removes surrounding quotes in smtp password', () => {
    expect(normalizeSmtpPassword('"ab cd"')).toBe('abcd');
    expect(normalizeSmtpPassword("'ab cd'")).toBe('abcd');
  });
});


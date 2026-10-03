import { getPasswordHash } from './database';
import { verifyPassword } from '../utils/crypto';
import { Errors } from '../utils/errors';
import { isObject } from './federation-events';

// Sensitive operations always require the current account's password. A valid
// access token or an arbitrary/completed UIA session is not a password proof.
export async function requirePasswordConfirmation(db: D1Database, userId: string, auth: unknown): Promise<Response | null> {
  if (!isObject(auth) || auth.type !== 'm.login.password') {
    return Response.json({ flows: [{ stages: ['m.login.password'] }], params: {}, session: crypto.randomUUID() }, { status: 401 });
  }
  if (typeof auth.password !== 'string' || !auth.password.length) return Errors.forbidden('Invalid password').toResponse();
  const localpart = userId.slice(1, userId.indexOf(':'));
  const matches = (value: unknown) => value === userId || value === localpart;
  if ((auth.identifier !== undefined && (!isObject(auth.identifier) || auth.identifier.type !== 'm.id.user' || !matches(auth.identifier.user))) ||
      (auth.user !== undefined && !matches(auth.user))) return Errors.forbidden('Authentication identity does not match account').toResponse();
  const hash = await getPasswordHash(db, userId);
  if (!hash) return Errors.forbidden('Password authentication is unavailable for this account').toResponse();
  let valid = false;
  try { valid = await verifyPassword(auth.password, hash); } catch { /* Fail closed for invalid hashes. */ }
  return valid ? null : Errors.forbidden('Invalid password').toResponse();
}

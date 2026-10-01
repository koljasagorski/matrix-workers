import { randomBytes, pbkdf2Sync } from 'node:crypto';
import { mkdirSync, mkdtempSync, writeFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';

const [username, target] = process.argv.slice(2);
if (!username || !/^[a-z0-9._=-]+$/.test(username) || !['--local', '--remote'].includes(target)) {
  throw new Error('Usage: npm run admin:create -- <username> --local|--remote');
}
const { unstable_readConfig } = await import('wrangler');
const config = unstable_readConfig({ config: 'wrangler.jsonc' });
const server = config.vars.SERVER_NAME;
const userId = `@${username}:${server}`;
const credentialsFile = resolve(`.local/${username}-${target.slice(2)}.json`);
if (existsSync(credentialsFile)) throw new Error(`Credentials already exist: ${credentialsFile}`);
const password = randomBytes(24).toString('base64url');
const salt = randomBytes(16);
const digest = pbkdf2Sync(password, salt, 100000, 32, 'sha256');
const hash = `$pbkdf2-sha256$100000$${salt.toString('base64')}$${digest.toString('base64')}`;
const dir = mkdtempSync(join(tmpdir(), 'matrix-admin-'));
try {
  const sqlPath = join(dir, 'admin.sql');
  const quote = value => "'" + value.replaceAll("'", "''") + "'";
  writeFileSync(sqlPath, `INSERT INTO users (user_id, localpart, password_hash, admin) VALUES (${quote(userId)}, ${quote(username)}, ${quote(hash)}, 1);`, { mode: 0o600 });
  execFileSync(process.execPath, ['node_modules/wrangler/bin/wrangler.js', 'd1', 'execute', 'DB', target, '--file', sqlPath], { stdio: 'inherit' });
  mkdirSync('.local', { recursive: true, mode: 0o700 });
  writeFileSync(credentialsFile, JSON.stringify({ homeserver: `https://${server}`, userId, password }, null, 2) + '\n', { mode: 0o600 });
  console.log(`Created ${userId}. Credentials saved to ${credentialsFile}`);
} finally {
  rmSync(dir, { recursive: true, force: true });
}

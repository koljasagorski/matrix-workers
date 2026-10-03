import { afterEach, beforeEach, expect, it } from 'vitest';
import push, { evaluatePushRules } from '../src/api/push';
import { testEnv } from './federation-helpers';

let ctx: Awaited<ReturnType<typeof testEnv>>;
const user = '@alice:local.example';
const room = '!muted:local.example';
const sender = '@muted:remote.example';
beforeEach(async () => { ctx = await testEnv(); });
afterEach(() => ctx.sqlite.close());
async function put(kind: string, id: string, pattern?: string) {
  const response = await push.request(`/_matrix/client/v3/pushrules/global/${kind}/${encodeURIComponent(id)}`, {
    method: 'PUT', headers: { Authorization: 'Bearer token' }, body: JSON.stringify({ actions: ['dont_notify'], ...(pattern ? { pattern } : {}) }),
  }, ctx.env);
  expect(response.status).toBe(200);
}
async function notify(body = 'Hello', roomId = room, eventSender = sender) {
  return (await evaluatePushRules(ctx.env.DB, user, { type: 'm.room.message', room_id: roomId,
    sender: eventSender, content: { body, msgtype: 'm.text' } }, 2)).notify;
}

it('applies a room rule only to messages in that room', async () => {
  await put('room', room);
  expect(await notify()).toBe(false);
  expect(await notify('Hello', '!other:local.example')).toBe(true);
});

it('applies a sender rule only to messages from that sender', async () => {
  await put('sender', sender);
  expect(await notify()).toBe(false);
  expect(await notify('Hello', room, '@other:remote.example')).toBe(true);
});

it('persists content patterns across requests and matches only the requested text', async () => {
  await put('content', 'mute-urgent', '*urgent*');
  const response = await push.request('/_matrix/client/v3/pushrules', { headers: { Authorization: 'Bearer token' } }, ctx.env);
  const data = await response.json() as any;
  expect(data.global.content.find((rule: any) => rule.rule_id === 'mute-urgent').pattern).toBe('*urgent*');
  expect(await notify('An urgent request')).toBe(false);
  expect(await notify('An ordinary request')).toBe(true);
});

it('disables legacy content rows whose pattern was discarded instead of muting every message', async () => {
  ctx.sqlite.prepare(`INSERT INTO push_rules(user_id,rule_id,kind,priority,enabled,actions)
    VALUES (?, 'legacy', 'content', 0, 1, '["dont_notify"]')`).run(user);
  expect(await notify()).toBe(true);
});

it('rejects malformed rule bodies before writing unread-count inputs', async () => {
  for (const body of [null, [], { actions: {} }, { actions: ['notify'], conditions: {} }, { actions: [null] }]) {
    const response = await push.request('/_matrix/client/v3/pushrules/global/override/custom', {
      method: 'PUT', headers: { Authorization: 'Bearer token' }, body: JSON.stringify(body),
    }, ctx.env);
    expect(response.status).toBe(400);
  }
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM push_rules').get()).toEqual({ n: 0 });
});

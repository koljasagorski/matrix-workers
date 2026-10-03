import { afterEach, expect, it, vi } from 'vitest';
import { testEnv } from './federation-helpers';

vi.mock('cloudflare:workers', () => ({ DurableObject: class {
  ctx: any; env: any;
  constructor(ctx: any, env: any) { this.ctx = ctx; this.env = env; }
} }));
import { FederationDurableObject } from '../src/durable-objects/FederationDurableObject';

afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

function memoryStorage() {
  const values = new Map<string, unknown>();
  let alarm: number | null = null;
  const storage = {
    get: async (key: string | string[]) => Array.isArray(key)
      ? new Map(key.filter(k => values.has(k)).map(k => [k, structuredClone(values.get(k))]))
      : structuredClone(values.get(key)),
    put: async (key: string, value: unknown) => { values.set(key, structuredClone(value)); },
    delete: async (key: string | string[]) => Array.isArray(key)
      ? key.reduce((count, k) => count + Number(values.delete(k)), 0) : values.delete(key),
    list: async ({ prefix }: { prefix: string }) => new Map([...values].filter(([k]) => k.startsWith(prefix))
      .sort(([a], [b]) => a.localeCompare(b)).map(([k, value]) => [k, structuredClone(value)])),
    setAlarm: async (time: number) => { alarm = time; },
    getAlarm: async () => alarm,
    transaction: async <T>(callback: (transaction: typeof storage) => Promise<T>): Promise<T> => {
      const before = structuredClone(values);
      const previousAlarm = alarm;
      try { return await callback(storage); }
      catch (error) {
        values.clear(); for (const [key, value] of before) values.set(key, value);
        alarm = previousAlarm; throw error;
      }
    },
  };
  return { storage, values, get alarm() { return alarm; },
    runAlarm: async (object: FederationDurableObject) => { alarm = null; await object.alarm(); } };
}

async function fixture(...destinations: string[]) {
  const { env, sqlite } = await testEnv();
  for (const destination of destinations.length ? destinations : ['remote.example']) {
    await env.CACHE.put(`discovery:${destination}`, JSON.stringify({ host: destination, port: 443, tlsHostname: destination }));
  }
  const state = memoryStorage();
  const object = new FederationDurableObject({ storage: state.storage } as any, env);
  return { env, sqlite, state, object };
}

function enqueueEvent(object: FederationDurableObject, id: string, destination = 'remote.example') {
  return object.fetch(new Request('https://internal/send', { method: 'POST', body: JSON.stringify({
    destination, event_id: id, room_id: '!room:local.example', pdu: { type: 'm.room.message', content: { body: id } },
  }) }));
}

function enqueueEdu(object: FederationDurableObject, id: string, destination = 'remote.example') {
  return object.fetch(new Request('https://internal/send-edu', { method: 'POST', body: JSON.stringify({
    destination, edu_type: 'm.receipt', content: { message_id: id },
  }) }));
}

function retryNow(state: ReturnType<typeof memoryStorage>, destination = 'remote.example') {
  const key = `server:${destination}`;
  const target = state.values.get(key) as Record<string, unknown>;
  state.values.set(key, { ...target, nextRetry: Date.now() - 1 });
}

it('persists EDU-only delivery and retries the identical signed transaction after eviction and new arrivals', async () => {
  const { env, sqlite, state, object } = await fixture();
  const sent: { url: string; body: string }[] = [];
  vi.stubGlobal('fetch', vi.fn(async (input: string, init: RequestInit) => {
    expect(new Headers(init.headers).get('Authorization')).toMatch(/^X-Matrix /);
    expect(init.redirect).toBe('manual');
    sent.push({ url: input, body: String(init.body) });
    return sent.length === 1 ? new Response('{}', { status: 503 }) : Response.json({ pdus: {} });
  }));
  try {
    expect((await enqueueEdu(object, 'original')).status).toBe(200);
    expect(fetch).not.toHaveBeenCalled(); expect(state.alarm).not.toBeNull();
    await state.runAlarm(object);
    expect([...state.values.keys()].some(key => key.startsWith('edu:'))).toBe(true);
    expect(state.alarm).toBeGreaterThanOrEqual(Date.now() + 59000);
    const restarted = new FederationDurableObject({ storage: state.storage } as any, env);
    await enqueueEvent(restarted, '$new-message');
    await enqueueEdu(restarted, 'new-receipt');
    retryNow(state);
    await state.runAlarm(restarted);
    expect(sent).toHaveLength(2); expect(sent[0]).toEqual(sent[1]);
    expect(JSON.parse(sent[1].body).edus).toEqual([{ edu_type: 'm.receipt', content: { message_id: 'original' } }]);
    expect(state.alarm).not.toBeNull();
    await state.runAlarm(restarted);
    expect(sent).toHaveLength(3);
    expect(JSON.parse(sent[2].body).pdus[0].content.body).toBe('$new-message');
    expect(JSON.parse(sent[2].body).edus[0].content.message_id).toBe('new-receipt');
    expect([...state.values.keys()].some(key => /^(edu|queue|transaction):/.test(key))).toBe(false);
  } finally { sqlite.close(); }
});

it('acknowledges partial success and receipts despite a rejected PDU, then drains subsequent batches', async () => {
  const { sqlite, state, object } = await fixture();
  const sent: { pdus: { content: { body: string } }[]; edus: { content: { message_id: string } }[] }[] = [];
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.stubGlobal('fetch', vi.fn(async (_input: string, init: RequestInit) => {
    const payload = JSON.parse(String(init.body)); sent.push(payload);
    return Response.json({ pdus: Object.fromEntries(payload.pdus.map((event: { content: { body: string } }) =>
      [event.content.body, event.content.body === '$poison' ? { error: 'Signature check failed' } : {}])) });
  }));
  try {
    for (let i = 0; i < 55; i++) {
      const id = i === 0 ? '$poison' : `$message-${i}`;
      await state.storage.put(`queue:remote.example:${id}`, { event_id: id, destination: 'remote.example',
        room_id: '!room:local.example', pdu: { content: { body: id } }, created_at: i, retry_count: 0 });
    }
    // Deliberately make lexical key order the reverse of creation order.
    for (let i = 0; i < 105; i++) await state.storage.put(`edu:remote.example:${String(105 - i).padStart(3, '0')}`,
      { destination: 'remote.example', edu_type: 'm.receipt', content: { message_id: String(i) }, created_at: i });
    await state.runAlarm(object);
    expect(sent[0].pdus).toHaveLength(50); expect(sent[0].edus).toHaveLength(100);
    expect(sent[0].edus.map(edu => edu.content.message_id)).toEqual(Array.from({ length: 100 }, (_, i) => String(i)));
    expect([...state.values.keys()].filter(key => key.startsWith('queue:'))).toHaveLength(5);
    expect([...state.values.keys()].filter(key => key.startsWith('edu:'))).toHaveLength(5);
    const rejected = [...state.values].filter(([key]) => key.startsWith('rejected:'));
    expect(rejected).toHaveLength(1);
    expect(rejected[0][1]).toMatchObject({ event_id: '$poison', error: 'Signature check failed' });
    expect(state.values.get('server:remote.example')).toMatchObject({ retryCount: 0, nextRetry: null, rejectedEvents: 1 });
    await state.runAlarm(object);
    expect(sent).toHaveLength(2);
    expect(sent[1].pdus.map(event => event.content.body)).toEqual(Array.from({ length: 5 }, (_, i) => `$message-${50 + i}`));
    expect(sent[1].edus.map(edu => edu.content.message_id)).toEqual(['100', '101', '102', '103', '104']);
    expect([...state.values.keys()].some(key => /^(edu|queue|transaction):/.test(key))).toBe(false);
    expect(state.alarm).toBeNull();
  } finally { sqlite.close(); }
});

it('keeps new arrivals during network delivery and ignores duplicate writes to the in-flight PDU', async () => {
  const { sqlite, state, object } = await fixture();
  let deliver!: () => void; let started!: () => void;
  const networkStarted = new Promise<void>(resolve => { started = resolve; });
  const blocked = new Promise<void>(resolve => { deliver = resolve; });
  const sent: string[] = [];
  vi.stubGlobal('fetch', vi.fn(async (_input: string, init: RequestInit) => {
    sent.push(String(init.body));
    if (sent.length === 1) { started(); await blocked; }
    return Response.json({ pdus: {} });
  }));
  try {
    await enqueueEvent(object, '$first');
    const running = state.runAlarm(object); await networkStarted;
    await object.fetch(new Request('https://internal/send', { method: 'POST', body: JSON.stringify({ destination: 'remote.example',
      event_id: '$first', room_id: '!room:local.example', pdu: { content: { body: 'replaced' } } }) }));
    await enqueueEvent(object, '$second');
    await enqueueEdu(object, 'receipt');
    deliver(); await running;
    expect(JSON.parse(sent[0]).pdus[0].content.body).toBe('$first');
    expect([...state.values.keys()].filter(key => key.startsWith('queue:'))).toEqual(['queue:remote.example:$second']);
    expect(state.alarm).not.toBeNull();
    await state.runAlarm(object);
    expect(JSON.parse(sent[1]).pdus[0].content.body).toBe('$second');
    expect(JSON.parse(sent[1]).edus[0].content.message_id).toBe('receipt');
  } finally { sqlite.close(); }
});

it('schedules the earliest retry across failed destinations and preserves it when later work arrives', async () => {
  const { sqlite, state, object } = await fixture('first.example', 'second.example');
  vi.stubGlobal('fetch', vi.fn(async () => new Response('{}', { status: 503 })));
  try {
    await enqueueEdu(object, 'first', 'first.example');
    await enqueueEdu(object, 'second', 'second.example');
    await state.storage.put('server:second.example', { serverName: 'second.example', retryCount: 1, nextRetry: null, lastContact: 0 });
    await state.runAlarm(object);
    const first = state.values.get('server:first.example') as { nextRetry: number };
    const second = state.values.get('server:second.example') as { nextRetry: number };
    expect(second.nextRetry).toBeGreaterThan(first.nextRetry);
    expect(state.alarm).toBe(first.nextRetry);
    await enqueueEdu(object, 'later', 'second.example');
    expect(state.alarm).toBe(first.nextRetry);
    await state.runAlarm(object);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(state.alarm).toBe(first.nextRetry);
  } finally { sqlite.close(); }
});

it('bounds retained rejected-event diagnostics without dropping pending deliveries', async () => {
  const { sqlite, state, object } = await fixture();
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.stubGlobal('fetch', vi.fn(async (_input: string, init: RequestInit) => {
    const { pdus } = JSON.parse(String(init.body));
    return Response.json({ pdus: Object.fromEntries(pdus.map((pdu: { content: { body: string } }) => [pdu.content.body, { error: 'Invalid event' }])) });
  }));
  try {
    for (let i = 0; i < 110; i++) await state.storage.put(`rejected:remote.example:${String(i).padStart(13, '0')}:$old-${i}`, { event_id: `$old-${i}` });
    await enqueueEvent(object, '$new-rejected');
    await state.runAlarm(object);
    const rejected = [...state.values.keys()].filter(key => key.startsWith('rejected:'));
    expect(rejected).toHaveLength(100);
    expect(rejected.some(key => key.endsWith(':$new-rejected'))).toBe(true);
    expect(rejected.some(key => key.endsWith(':$old-0'))).toBe(false);
    expect([...state.values.keys()].some(key => /^(queue|transaction):/.test(key))).toBe(false);
  } finally { sqlite.close(); }
});

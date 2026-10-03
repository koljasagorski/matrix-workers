// Federation Durable Object for server-to-server communication

import { DurableObject } from 'cloudflare:workers';
import { sha256 } from '../utils/crypto';
import { readFederationJson } from '../services/federation-http';
import { federationPut } from '../services/federation-keys';
import type { Env } from '../types';

interface FederationTarget {
  serverName: string;
  lastContact: number;
  retryCount: number;
  nextRetry: number | null;
  lastError?: string;
  rejectedEvents?: number;
}

interface OutboundEvent {
  event_id: string;
  room_id: string;
  destination: string;
  pdu: Record<string, unknown>;
  created_at: number;
  retry_count: number;
}

interface OutboundEdu {
  edu_type: string;
  destination: string;
  content: Record<string, unknown>;
  created_at: number;
}

interface PendingTransaction {
  transactionId: string;
  destination: string;
  eventKeys: string[];
  eduKeys: string[];
  originServerTs: number;
}

interface RejectedEvent extends OutboundEvent {
  transaction_id: string;
  rejected_at: number;
  error: string;
}

const MAX_REJECTED_EVENTS = 100;

export class FederationDurableObject extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === '/send') {
      return this.handleSend(request);
    }

    if (path === '/receive') {
      return this.handleReceive(request);
    }

    if (path === '/status') {
      return this.handleStatus(request);
    }

    if (path === '/keys') {
      return this.handleKeys(request);
    }

    if (path === '/send-edu') {
      return this.handleSendEdu(request);
    }

    return new Response('Not found', { status: 404 });
  }

  // Queue an event for federation to a remote server
  private async handleSend(request: Request): Promise<Response> {
    const data = await request.json() as {
      destination: string;
      event_id: string;
      room_id: string;
      pdu: Record<string, unknown>;
    };

    const outboundEvent: OutboundEvent = {
      event_id: data.event_id,
      room_id: data.room_id,
      destination: data.destination,
      pdu: data.pdu,
      created_at: Date.now(),
      retry_count: 0,
    };

    // Store in queue
    const key = `queue:${data.destination}:${data.event_id}`;
    await this.ctx.storage.transaction(async storage => {
      // Queue entries referenced by an in-flight transaction must remain immutable.
      if (!await storage.get(key)) await storage.put(key, outboundEvent);
      await this.scheduleDestination(storage, data.destination);
    });

    return new Response('Queued');
  }

  // Handle incoming federation request
  private async handleReceive(request: Request): Promise<Response> {
    const origin = request.headers.get('X-Matrix-Origin');
    if (!origin) {
      return new Response(JSON.stringify({
        errcode: 'M_MISSING_PARAM',
        error: 'Missing origin header',
      }), { status: 400 });
    }

    // Verify request signature (simplified)
    // In production, verify against the server's signing keys

    const data = await request.json() as {
      pdus: any[];
      edus?: any[];
    };

    // Process incoming PDUs
    const processedPdus: string[] = [];
    for (const pdu of data.pdus || []) {
      // Store the event
      await this.ctx.storage.put(`received:${pdu.event_id}`, {
        pdu,
        origin,
        received_at: Date.now(),
      });
      processedPdus.push(pdu.event_id);
    }

    // Update server status
    const target: FederationTarget = {
      serverName: origin,
      lastContact: Date.now(),
      retryCount: 0,
      nextRetry: null,
    };
    await this.ctx.storage.put(`server:${origin}`, target);

    return new Response(JSON.stringify({
      pdus: processedPdus.reduce((acc, id) => ({ ...acc, [id]: {} }), {}),
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Get federation status for a server
  private async handleStatus(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const serverName = url.searchParams.get('server');

    if (serverName) {
      const target = await this.ctx.storage.get(`server:${serverName}`) as FederationTarget | undefined;
      return new Response(JSON.stringify(target || { serverName, status: 'unknown' }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // List all known servers
    const servers: FederationTarget[] = [];
    const allKeys = await this.ctx.storage.list({ prefix: 'server:' });
    for (const [, value] of allKeys) {
      servers.push(value as FederationTarget);
    }

    return new Response(JSON.stringify({ servers }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Handle server key requests
  private async handleKeys(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const serverName = url.searchParams.get('server');

    if (!serverName) {
      return new Response(JSON.stringify({
        errcode: 'M_MISSING_PARAM',
        error: 'Missing server parameter',
      }), { status: 400 });
    }

    // Get cached keys
    const cachedKeys = await this.ctx.storage.get(`keys:${serverName}`);
    if (cachedKeys) {
      const keys = cachedKeys as { data: any; expires: number };
      if (keys.expires > Date.now()) {
        return new Response(JSON.stringify(keys.data), {
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    // Fetch keys from remote server
    try {
      const response = await fetch(`https://${serverName}/_matrix/key/v2/server`, {
        headers: {
          'Accept': 'application/json',
        },
      });

      if (response.ok) {
        const data = await response.json();

        // Cache for 24 hours
        await this.ctx.storage.put(`keys:${serverName}`, {
          data,
          expires: Date.now() + (24 * 60 * 60 * 1000),
        });

        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json' },
        });
      }
    } catch (e) {
      console.error(`Failed to fetch keys from ${serverName}:`, e);
    }

    return new Response(JSON.stringify({
      errcode: 'M_NOT_FOUND',
      error: 'Server keys not found',
    }), { status: 404 });
  }

  // Queue an EDU for federation to a remote server
  private async handleSendEdu(request: Request): Promise<Response> {
    const data = await request.json() as {
      destination: string;
      edu_type: string;
      content: Record<string, unknown>;
    };

    const edu: OutboundEdu = {
      edu_type: data.edu_type,
      destination: data.destination,
      content: data.content,
      created_at: Date.now(),
    };

    // Store in EDU queue
    const key = `edu:${data.destination}:${edu.created_at}:${crypto.randomUUID()}`;
    await this.ctx.storage.transaction(async storage => {
      await storage.put(key, edu);
      await this.scheduleDestination(storage, data.destination);
    });

    return new Response('Queued');
  }

  private async scheduleDestination(storage: DurableObjectTransaction, destination: string): Promise<void> {
    const target = await storage.get<FederationTarget>(`server:${destination}`);
    const due = Math.max(Date.now() + 1, target?.nextRetry ?? 0);
    const current = await storage.getAlarm();
    if (current === null || due < current) await storage.setAlarm(due);
  }

  private async getPendingTransaction(destination: string): Promise<PendingTransaction | undefined> {
    const key = `transaction:${destination}`;
    const existing = await this.ctx.storage.get<PendingTransaction>(key);
    if (existing) return existing;

    // Sort entries together with their keys so the exact transmitted EDUs are acknowledged.
    const events = [...await this.ctx.storage.list<OutboundEvent>({ prefix: `queue:${destination}:` })]
      .sort(([aKey, a], [bKey, b]) => a.created_at - b.created_at || aKey.localeCompare(bKey)).slice(0, 50);
    const edus = [...await this.ctx.storage.list<OutboundEdu>({ prefix: `edu:${destination}:` })]
      .sort(([aKey, a], [bKey, b]) => a.created_at - b.created_at || aKey.localeCompare(bKey)).slice(0, 100);
    if (!events.length && !edus.length) return;

    const transaction: PendingTransaction = {
      transactionId: await sha256(JSON.stringify([events.map(([, event]) => event.event_id), edus.map(([eduKey]) => eduKey)])),
      destination,
      eventKeys: events.map(([eventKey]) => eventKey),
      eduKeys: edus.map(([eduKey]) => eduKey),
      originServerTs: events[0]?.[1].created_at ?? edus[0][1].created_at,
    };
    // Persist the batch before network I/O. New arrivals cannot change a retried transaction.
    // Store references rather than a duplicate payload to stay below the per-value storage limit.
    await this.ctx.storage.put(key, transaction);
    return transaction;
  }

  private async processFederationQueue(destination: string): Promise<void> {
    const transaction = await this.getPendingTransaction(destination);
    if (!transaction) return;
    const storedEvents = transaction.eventKeys.length
      ? await this.ctx.storage.get<OutboundEvent>(transaction.eventKeys) : new Map<string, OutboundEvent>();
    const storedEdus = transaction.eduKeys.length
      ? await this.ctx.storage.get<OutboundEdu>(transaction.eduKeys) : new Map<string, OutboundEdu>();
    const events = transaction.eventKeys.map(key => storedEvents.get(key));
    const edus = transaction.eduKeys.map(key => storedEdus.get(key));
    if (events.some(event => !event) || edus.some(edu => !edu)) {
      throw new Error('Federation transaction references a missing queue entry');
    }
    const batchEvents = events as OutboundEvent[];
    const batchEdus = edus as OutboundEdu[];

    try {
      const response = await federationPut(destination,
        `/_matrix/federation/v1/send/${transaction.transactionId}`,
        { origin: this.env.SERVER_NAME, origin_server_ts: transaction.originServerTs,
          pdus: batchEvents.map(event => event.pdu),
          edus: batchEdus.map(edu => ({ edu_type: edu.edu_type, content: edu.content })) },
        this.env.SERVER_NAME, this.env.DB, this.env.CACHE);

      if (!response.ok) {
        await response.body?.cancel();
        await this.scheduleRetry(destination, `HTTP ${response.status}`);
        return;
      }

      const result = await readFederationJson(response) as { pdus?: Record<string, { error?: string }> };
      const rejected = batchEvents.filter(event => typeof result.pdus?.[event.event_id]?.error === 'string');
      await this.ctx.storage.transaction(async storage => {
        const now = Date.now();
        for (const event of rejected) {
          const error = result.pdus![event.event_id].error!.slice(0, 2048);
          const entry: RejectedEvent = { ...event, transaction_id: transaction.transactionId, rejected_at: now, error };
          await storage.put(`rejected:${destination}:${String(now).padStart(13, '0')}:${event.event_id}`, entry);
        }
        const rejectedKeys = await storage.list({ prefix: `rejected:${destination}:` });
        const excess = [...rejectedKeys.keys()].slice(0, Math.max(0, rejectedKeys.size - MAX_REJECTED_EVENTS));
        if (excess.length) await storage.delete(excess);
        // HTTP 200 completes a transaction even if individual PDUs were rejected. Retrying
        // that transaction returns the same result and blocks all subsequent messages/EDUs.
        await storage.delete([...transaction.eventKeys, `transaction:${destination}`]);
        if (transaction.eduKeys.length) await storage.delete(transaction.eduKeys);
        const previous = await storage.get<FederationTarget>(`server:${destination}`);
        await storage.put(`server:${destination}`, {
          serverName: destination, lastContact: now, retryCount: 0, nextRetry: null,
          rejectedEvents: (previous?.rejectedEvents ?? 0) + rejected.length,
          ...(rejected.length ? { lastError: `${rejected.length} PDU(s) rejected` } : {}),
        } satisfies FederationTarget);
      });
      for (const event of rejected) {
        console.warn(JSON.stringify({ event: 'federation_pdu_rejected', destination, event_id: event.event_id,
          transaction_id: transaction.transactionId, error: result.pdus![event.event_id].error!.slice(0, 2048) }));
      }
    } catch (error) {
      console.error(`Federation send to ${destination} failed:`, error);
      await this.scheduleRetry(destination, error instanceof Error ? error.message : 'Federation request failed');
    }
  }

  private async scheduleRetry(destination: string, error: string): Promise<void> {
    const target = await this.ctx.storage.get(`server:${destination}`) as FederationTarget | undefined;
    const retryCount = (target?.retryCount || 0) + 1;

    // Exponential backoff: 1min, 2min, 4min, 8min, 16min, max 1hour
    const delay = Math.min(60000 * Math.pow(2, retryCount - 1), 3600000);
    const nextRetry = Date.now() + delay;

    // Update server status
    const newTarget: FederationTarget = {
      serverName: destination,
      lastContact: target?.lastContact || 0,
      retryCount,
      nextRetry,
      lastError: error.slice(0, 2048),
      rejectedEvents: target?.rejectedEvents ?? 0,
    };
    await this.ctx.storage.put(`server:${destination}`, newTarget);

    // The alarm handler selects the earliest pending destination after processing all batches.
  }

  async alarm(): Promise<void> {
    const pending = [...(await this.ctx.storage.list<OutboundEvent>({ prefix:'queue:' })).values(),
      ...(await this.ctx.storage.list<OutboundEdu>({ prefix:'edu:' })).values()];
    for (const destination of new Set(pending.map(item => item.destination))) {
      const target = await this.ctx.storage.get<FederationTarget>(`server:${destination}`);
      if (!target?.nextRetry || target.nextRetry <= Date.now()) await this.processFederationQueue(destination);
    }

    // Re-read after delivery: arrivals during network I/O and batches beyond 50/100 remain queued.
    const remaining = [...(await this.ctx.storage.list<OutboundEvent>({ prefix: 'queue:' })).values(),
      ...(await this.ctx.storage.list<OutboundEdu>({ prefix: 'edu:' })).values()];
    let next: number | undefined;
    for (const destination of new Set(remaining.map(item => item.destination))) {
      const target = await this.ctx.storage.get<FederationTarget>(`server:${destination}`);
      next = Math.min(next ?? Infinity, Math.max(Date.now() + 1000, target?.nextRetry ?? 0));
    }
    if (next !== undefined) await this.ctx.storage.setAlarm(next);
  }
}

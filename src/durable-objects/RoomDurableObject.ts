// Room Durable Object for real-time room coordination

import { migrationExport } from './migration-export';
import { frozenResponse } from '../middleware/migration-freeze';
import { DurableObject } from 'cloudflare:workers';
import type { Env } from '../types';
import { notifyReceiptUsers, parseFederatedReceipts, queueReadReceipt, receiptAdvances, receiptEventDepths, recordReadReceipts, type ReadReceipt } from '../services/read-receipts';
import { isServerAllowedInRoom } from '../services/server-acl';

interface RoomSession {
  id: string;
  userId: string;
  deviceId: string | null;
  roomId?: string;
}

interface TypingState {
  expiresAt: number;
}

interface ReceiptData {
  user_id: string;  // Added for proper response building
  event_id: string;
  receipt_type: string;
  ts: number;
  thread_id?: string;
}

export class RoomDurableObject extends DurableObject<Env> {
  private sessions: Map<WebSocket, RoomSession> = new Map();
  private roomId: string = '';
  private roomIdStored: boolean = false;

  // In-memory typing state - Map of userId -> expiration timestamp
  private typingUsers: Map<string, TypingState> = new Map();

  // In-memory receipts cache (also persisted to durable storage)
  // Map of `${userId}:${receiptType}:${threadContext}` -> ReceiptData
  // threadContext is thread_id ?? 'unthreaded'
  private receiptsCache: Map<string, ReceiptData> = new Map();
  private receiptsCacheLoaded: boolean = false;
  private receiptsCacheMirrored: boolean = false;
  private receiptImportPromise?: Promise<void>;
  private receiptWork: Promise<void> = Promise.resolve();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    for (const ws of ctx.getWebSockets()) {
      const session = ws.deserializeAttachment() as RoomSession | null;
      if (session?.roomId) { this.roomId = session.roomId; break; }
    }
  }

  // Load receipts from durable storage into cache
  private async loadReceiptsCache(): Promise<void> {
    if (!this.roomId) this.roomId = await this.ctx.storage.get<string>('room-id') ?? '';
    if (this.roomId && !this.roomIdStored) {
      await this.ctx.storage.put('room-id', this.roomId);
      this.roomIdStored = true;
    }
    if (!this.receiptsCacheLoaded) {
      const stored = await this.ctx.storage.list<ReceiptData>({ prefix: 'receipt:' });
      const normalized: ReceiptData[] = [];
      for (const [key, value] of stored) {
        let userId = value.user_id;
        if (!userId) {
          // Legacy keys did not include user_id in the stored value.
          const keyWithoutPrefix = key.slice('receipt:'.length);
          const typeIndex = keyWithoutPrefix.lastIndexOf(`:${value.receipt_type}`);
          if (typeIndex <= 0) continue;
          userId = keyWithoutPrefix.slice(0, typeIndex);
        }
        normalized.push({ ...value, user_id: userId });
      }
      const depths = this.roomId
        ? await receiptEventDepths(this.env.DB, this.roomId, normalized.map(receipt => receipt.event_id)) : new Map<string, number>();
      for (const receipt of normalized) {
        const key = `${receipt.user_id}:${receipt.receipt_type}:${receipt.thread_id ?? 'unthreaded'}`;
        if (receiptAdvances(receipt, this.receiptsCache.get(key), depths)) this.receiptsCache.set(key, receipt);
      }
      this.receiptsCacheLoaded = true;
    }
    // Previous releases already set receipt-import:v1 but only had a DO copy.
    // Keep this migration independent of archived EDU recovery and retry failures.
    if (this.roomId && !this.receiptsCacheMirrored) {
      if (!await this.ctx.storage.get('receipt-mirror:v1')) {
        await recordReadReceipts(this.env.DB, this.roomId, [...this.receiptsCache.values()] as ReadReceipt[]);
        await this.ctx.storage.put('receipt-mirror:v1', true);
      }
      this.receiptsCacheMirrored = true;
    }
  }

  // D1 reads yield to other requests. Serialize receipt work so progress checked
  // before a write remains current when durable storage and the cache are updated.
  private serializeReceiptWork<T>(work: () => Promise<T>): Promise<T> {
    const result = this.receiptWork.then(work);
    this.receiptWork = result.then(() => {}, () => {});
    return result;
  }

  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === '/migration-export') return migrationExport(request, this.ctx);
    if (this.env.MIGRATION_FREEZE === '1') return frozenResponse();
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === '/websocket') {
      return this.handleWebSocket(request);
    }

    if (path === '/broadcast') {
      return this.handleBroadcast(request);
    }

    if (path === '/state') {
      return this.handleState(request);
    }

    // Typing endpoints
    if (path === '/typing') {
      if (request.method === 'PUT') {
        return this.handleSetTyping(request);
      }
      if (request.method === 'GET') {
        return this.handleGetTyping();
      }
    }

    // Receipt endpoints
    if (path === '/receipt') {
      if (request.method === 'PUT') {
        return this.handleSetReceipt(request);
      }
    }
    if (path === '/receipts') {
      if (request.method === 'GET') {
        return this.handleGetReceipts(request);
      }
    }

    return new Response('Not found', { status: 404 });
  }

  // Set a read receipt for a user
  private async handleSetReceipt(request: Request): Promise<Response> {
    const body = await request.json() as {
      user_id: string;
      event_id: string;
      receipt_type: string;
      thread_id?: string;
      ts?: number;
      room_id?: string;
    };

    return this.serializeReceiptWork(async () => {
      const { user_id, event_id, receipt_type, thread_id } = body;
      const ts = body.ts ?? Date.now();
      if (body.room_id) this.roomId = body.room_id;
      await this.loadReceiptsCache();

      // Determine thread context for storage key
      // - undefined/absent = "unthreaded" (room-level receipt)
      // - "main" = main timeline only
      // - event_id = specific thread
      const threadContext = thread_id ?? 'unthreaded';

      // Store in durable storage with thread-aware key
      const storageKey = `receipt:${user_id}:${receipt_type}:${threadContext}`;
      const previous = this.receiptsCache.get(`${user_id}:${receipt_type}:${threadContext}`);
      const receiptData: ReceiptData = { user_id, event_id, receipt_type, ts, thread_id };
      const depths = previous && this.roomId
        ? await receiptEventDepths(this.env.DB, this.roomId, [event_id, previous.event_id]) : new Map<string, number>();
      if (!receiptAdvances(receiptData, previous, depths)) {
        if (this.roomId) await recordReadReceipts(this.env.DB, this.roomId, [previous as ReadReceipt]);
        return Response.json({ updated: false, receipt: previous });
      }
      await this.ctx.storage.put(storageKey, receiptData);

      // Update cache
      this.receiptsCache.set(`${user_id}:${receipt_type}:${threadContext}`, receiptData);
      if (this.roomId) await recordReadReceipts(this.env.DB, this.roomId, [receiptData as ReadReceipt]);

      // Broadcast to WebSocket clients
      const message = JSON.stringify({
        type: 'receipt',
        user_id,
        event_id,
        receipt_type,
        ts,
        room_id: this.roomId,
        thread_id,
      });

      const webSockets = this.ctx.getWebSockets();
      for (const ws of webSockets) {
        const session = ws.deserializeAttachment() as RoomSession | null;
        if (session && (receipt_type !== 'm.read.private' || session.userId === user_id)) {
          try {
            ws.send(message);
          } catch {
            // WebSocket may be closed
          }
        }
      }

      return Response.json({ updated: true, receipt: receiptData });
    });
  }

  // Earlier releases acknowledged receipt EDUs but only archived them. Recover
  // those authenticated records once per room, without replacing newer receipts.
  private async importRecordedReceipts(roomId: string): Promise<void> {
    if (await this.ctx.storage.get('receipt-import:v1')) return;
    const members = await this.env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id=? AND membership='join'")
      .bind(roomId).all<{ user_id: string }>();
    const joined = new Set(members.results.map(member => member.user_id));
    const rows = await this.env.DB.prepare(`SELECT origin,content FROM processed_edus
      WHERE edu_type='m.receipt' AND EXISTS (SELECT 1 FROM json_each(processed_edus.content) room WHERE room.key=?)
      ORDER BY processed_at DESC LIMIT 1000`).bind(roomId).all<{ origin: string; content: string }>();
    const candidates: ReadReceipt[] = [];
    const origins = new Map<string, boolean>();
    for (const row of rows.results) {
      if (!origins.has(row.origin)) origins.set(row.origin, await isServerAllowedInRoom(this.env.DB, roomId, row.origin));
      if (!origins.get(row.origin)) continue;
      for (const update of parseFederatedReceipts(JSON.parse(row.content), row.origin)) {
        const receipt = update.receipt;
        if (update.roomId !== roomId || !joined.has(receipt.user_id)) continue;
        candidates.push(receipt);
      }
    }
    const depths = await receiptEventDepths(this.env.DB, roomId,
      [...candidates, ...this.receiptsCache.values()].map(receipt => receipt.event_id));
    const imported = new Map<string, ReceiptData>();
    for (const receipt of candidates) {
      const key = `${receipt.user_id}:${receipt.receipt_type}:${receipt.thread_id ?? 'unthreaded'}`;
      if (receiptAdvances(receipt, imported.get(key) ?? this.receiptsCache.get(key), depths)) imported.set(key, receipt);
    }
    if (imported.size) {
      await recordReadReceipts(this.env.DB, roomId, [...imported.values()] as ReadReceipt[]);
      await this.ctx.storage.put(Object.fromEntries([...imported].map(([key, receipt]) => [`receipt:${key}`, receipt])));
      for (const [key, receipt] of imported) this.receiptsCache.set(key, receipt);
    }
    await this.ctx.storage.put('receipt-import:v1', true);
  }

  // Get all receipts for this room
  private async handleGetReceipts(request: Request): Promise<Response> {
    return this.serializeReceiptWork(async () => {
      const roomId = new URL(request.url).searchParams.get('room_id');
      if (roomId) this.roomId = roomId;
      await this.loadReceiptsCache();
      if (roomId) {
        this.receiptImportPromise ??= this.importRecordedReceipts(roomId).catch(error => {
          this.receiptImportPromise = undefined;
          throw error;
        });
        await this.receiptImportPromise;
      }

      // Build Matrix receipt format: { eventId: { receiptType: { userId: { ts, thread_id? } } } }
      const receipts: Record<string, Record<string, Record<string, { ts: number; thread_id?: string }>>> = {};

      for (const [_key, data] of this.receiptsCache.entries()) {
        const { user_id, event_id, receipt_type, ts, thread_id } = data;

        if (!receipts[event_id]) {
          receipts[event_id] = {};
        }
        if (!receipts[event_id][receipt_type]) {
          receipts[event_id][receipt_type] = {};
        }

        // Include thread_id in response if present and not 'unthreaded'
        const userData: { ts: number; thread_id?: string } = { ts };
        if (thread_id && thread_id !== 'unthreaded') {
          userData.thread_id = thread_id;
        }
        receipts[event_id][receipt_type][user_id] = userData;
      }

      return new Response(JSON.stringify({ receipts }), {
        headers: { 'Content-Type': 'application/json' },
      });
    });
  }

  // Set typing state for a user
  private async handleSetTyping(request: Request): Promise<Response> {
    const body = await request.json() as {
      user_id: string;
      typing: boolean;
      timeout?: number;
    };

    const { user_id, typing, timeout = 30000 } = body;

    if (typing) {
      // User started typing - set expiration
      const expiresAt = Date.now() + Math.min(timeout, 120000); // Max 2 minutes
      this.typingUsers.set(user_id, { expiresAt });

      // Broadcast to WebSocket clients
      await this.broadcastTyping(user_id, true);
    } else {
      // User stopped typing
      this.typingUsers.delete(user_id);

      // Broadcast to WebSocket clients
      await this.broadcastTyping(user_id, false);
    }

    return new Response('OK');
  }

  // Get current typing users (with cleanup of expired entries)
  private handleGetTyping(): Response {
    const now = Date.now();
    const activeTypingUsers: string[] = [];

    // Clean up expired entries and collect active ones
    for (const [userId, state] of this.typingUsers.entries()) {
      if (state.expiresAt > now) {
        activeTypingUsers.push(userId);
      } else {
        // Expired - remove it
        this.typingUsers.delete(userId);
      }
    }

    return new Response(JSON.stringify({
      user_ids: activeTypingUsers,
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleWebSocket(request: Request): Promise<Response> {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Expected websocket upgrade', { status: 426 });
    }

    // Get session info from query params
    const url = new URL(request.url);
    const userId = url.searchParams.get('user_id');
    const deviceId = url.searchParams.get('device_id');
    const roomId = url.searchParams.get('room_id');

    if (!userId || !roomId) {
      return new Response('Missing user_id or room_id', { status: 400 });
    }

    this.roomId = roomId;
    await this.ctx.storage.put('room-id', roomId);
    this.roomIdStored = true;

    // Create WebSocket pair
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    // Accept the WebSocket with hibernation support
    this.ctx.acceptWebSocket(server, [userId, roomId]);

    // Store session data
    const session: RoomSession = {
      id: crypto.randomUUID(),
      userId,
      deviceId,
      roomId,
    };

    // Serialize session for hibernation
    server.serializeAttachment(session);

    this.sessions.set(server, session);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async handleBroadcast(request: Request): Promise<Response> {
    const message = await request.json();

    // Broadcast to all connected WebSockets
    const webSockets = this.ctx.getWebSockets();
    for (const ws of webSockets) {
      try {
        ws.send(JSON.stringify(message));
      } catch (e) {
        // WebSocket may be closed
      }
    }

    return new Response('OK');
  }

  private async handleState(_request: Request): Promise<Response> {
    // Return current room state
    const webSockets = this.ctx.getWebSockets();
    const users: string[] = [];

    for (const ws of webSockets) {
      const session = ws.deserializeAttachment() as RoomSession | null;
      if (session) {
        users.push(session.userId);
      }
    }

    return new Response(JSON.stringify({
      room_id: this.roomId,
      connected_users: [...new Set(users)],
      connection_count: webSockets.length,
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // WebSocket hibernation handlers
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (this.env.MIGRATION_FREEZE === '1') { ws.close(1013, 'Migration maintenance'); return; }
    const session = ws.deserializeAttachment() as RoomSession | null;
    if (!session) return;

    try {
      if (!this.roomId) this.roomId = session.roomId ?? await this.ctx.storage.get<string>('room-id') ?? '';
      const data = typeof message === 'string' ? JSON.parse(message) : null;
      if (!data) return;

      // Handle different message types
      switch (data.type) {
        case 'typing':
          // Broadcast typing notification to other users
          await this.broadcastTyping(session.userId, data.typing);
          break;

        case 'read':
          // Handle read receipt
          await this.handleReadReceipt(session.userId, data.event_id);
          break;

        case 'ping':
          ws.send(JSON.stringify({ type: 'pong' }));
          break;

        default:
          // Unknown message type
          break;
      }
    } catch (e) {
      console.error('Error handling WebSocket message:', e);
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string, _wasClean: boolean): Promise<void> {
    const session = ws.deserializeAttachment() as RoomSession | null;
    if (session) {
      this.sessions.delete(ws);

      // Notify other users that this user disconnected
      const webSockets = this.ctx.getWebSockets();
      for (const otherWs of webSockets) {
        if (otherWs !== ws) {
          try {
            otherWs.send(JSON.stringify({
              type: 'user_disconnected',
              user_id: session.userId,
            }));
          } catch (e) {
            // WebSocket may be closed
          }
        }
      }
    }

    ws.close(code, reason);
  }

  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('WebSocket error:', error);
    const session = ws.deserializeAttachment() as RoomSession | null;
    if (session) {
      this.sessions.delete(ws);
    }
  }

  private async broadcastTyping(userId: string, isTyping: boolean): Promise<void> {
    const message = JSON.stringify({
      type: 'typing',
      user_id: userId,
      typing: isTyping,
      room_id: this.roomId,
    });

    const webSockets = this.ctx.getWebSockets();
    for (const ws of webSockets) {
      const session = ws.deserializeAttachment() as RoomSession | null;
      if (session && session.userId !== userId) {
        try {
          ws.send(message);
        } catch (e) {
          // WebSocket may be closed
        }
      }
    }
  }

  private async handleReadReceipt(userId: string, eventId: string, threadId?: string): Promise<void> {
    const receipt = { user_id: userId, event_id: eventId, receipt_type: 'm.read' as const, ts: Date.now(), thread_id: threadId };
    const response = await this.handleSetReceipt(new Request('https://room/receipt', { method: 'PUT', body: JSON.stringify(receipt) }));
    const effective = (await response.json() as { receipt: ReadReceipt }).receipt;
    if (this.roomId) {
      await queueReadReceipt(this.env, this.roomId, effective);
      await notifyReceiptUsers(this.env, this.roomId);
    }
  }
}

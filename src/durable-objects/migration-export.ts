// Read-only, bounded storage pages. The public caller is separately restricted to
// password-confirmed administrators and an operator-approved object inventory.
export async function migrationExport(request: Request, state: DurableObjectState): Promise<Response> {
  const headers = { 'Cache-Control': 'no-store', 'Content-Type': 'application/json' };
  if (request.method !== 'GET') return Response.json({ error: 'Read-only export' }, { status: 405, headers });
  const url = new URL(request.url);
  const cursor = url.searchParams.get('cursor');
  const limit = Number(url.searchParams.get('limit') ?? 16);
  if (!Number.isInteger(limit) || limit < 1 || limit > 16 || (cursor !== null && new TextEncoder().encode(cursor).length > 2048)) {
    return Response.json({ error: 'Invalid export page' }, { status: 400, headers });
  }
  const page = await state.storage.transaction(async storage => {
    const values = await storage.list<unknown>({ ...(cursor !== null ? { startAfter: cursor } : {}), limit: limit + 1 });
    const rows = [...values];
    const more = rows.length > limit;
    const entries = rows.slice(0, limit);
    // Storage supports structured clone; refusing unsupported values is safer
    // than silently losing typed data in a JSON backup.
    for (const [, value] of entries) assertJsonValue(value);
    return { entries, next_cursor: more ? entries.at(-1)![0] : null, alarm: await storage.getAlarm() };
  });
  return Response.json({ format: 'matrix-workers-do-v1', object_id: state.id.toString(), exported_at: Date.now(), ...page }, { headers });
}

function assertJsonValue(value: unknown, seen = new Set<object>()): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value))) return;
  if (typeof value !== 'object' || seen.has(value)) throw new Error('Migration export contains a non-JSON storage value');
  const prototype = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) throw new Error('Migration export contains a typed storage value');
  seen.add(value);
  for (const child of Object.values(value)) assertJsonValue(child, seen);
  seen.delete(value);
}

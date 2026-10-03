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
    return { entries: entries.map(([key, value]) => [key, encodeStoredValue(value)]),
      next_cursor: more ? entries.at(-1)![0] : null, alarm: await storage.getAlarm() };
  });
  return Response.json({ format: 'matrix-workers-do-v2', encoding: 'structured-clone-v1', object_id: state.id.toString(), exported_at: Date.now(), ...page }, { headers });
}

// Every node is tagged, including ordinary objects. User keys named "type" or
// "id" cannot collide with codec metadata. References preserve cycles/aliasing.
export function encodeStoredValue(value: unknown): unknown {
  const references = new Map<object, number>();
  const encode = (item: unknown): unknown => {
    if (item === null) return { type: 'null' };
    if (typeof item === 'undefined') return { type: 'undefined' };
    if (typeof item === 'string' || typeof item === 'boolean') return { type: typeof item, value: item };
    if (typeof item === 'number') return { type: 'number', value: Object.is(item, -0) ? '-0' : String(item) };
    if (typeof item === 'bigint') return { type: 'bigint', value: String(item) };
    if (typeof item !== 'object') throw new Error('Unsupported migration storage value');
    const existing = references.get(item);
    if (existing !== undefined) return { type: 'reference', id: existing };
    const id = references.size;
    references.set(item, id);
    if (Array.isArray(item)) return { type: 'array', id, length: item.length, value: Object.entries(item).map(([key, child]) => [key, encode(child)]) };
    if (item instanceof Date) return { type: 'date', id, value: String(item.getTime()) };
    if (item instanceof Map) return { type: 'map', id, value: [...item].map(([key, child]) => [encode(key), encode(child)]) };
    if (item instanceof Set) return { type: 'set', id, value: [...item].map(encode) };
    if (item instanceof RegExp) return { type: 'regexp', id, source: item.source, flags: item.flags };
    if (ArrayBuffer.isView(item)) return { type: 'view', id, kind: item.constructor.name, buffer: encode(item.buffer), byte_offset: item.byteOffset, byte_length: item.byteLength };
    if (item instanceof ArrayBuffer) {
      const bytes = new Uint8Array(item);
      let binary = '';
      for (let offset = 0; offset < bytes.length; offset += 8192) binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
      return { type: 'array-buffer', id, value: btoa(binary) };
    }
    if (item instanceof Error) return { type: 'error', id, name: item.name, message: item.message, stack: item.stack, cause: encode(item.cause) };
    const prototype = Object.getPrototypeOf(item);
    if (prototype !== Object.prototype && prototype !== null) throw new Error('Unsupported migration storage prototype');
    return { type: 'object', id, null_prototype: prototype === null, value: Object.entries(item).map(([key, child]) => [key, encode(child)]) };
  };
  return encode(value);
}

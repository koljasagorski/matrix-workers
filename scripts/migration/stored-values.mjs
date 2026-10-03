// Decoder for the fully tagged Durable Object archive, including references.
export function decodeStoredValue(node) {
  const references = new Map();
  const decode = item => {
    switch (item.type) {
      case 'null': return null;
      case 'undefined': return undefined;
      case 'string': case 'boolean': return item.value;
      case 'number': return Number(item.value);
      case 'bigint': return BigInt(item.value);
      case 'reference': if (!references.has(item.id)) throw new Error('Unknown archive reference'); return references.get(item.id);
    }
    let value;
    switch (item.type) {
      case 'object': value = item.null_prototype ? Object.create(null) : {}; break;
      case 'array': value = new Array(item.length); break;
      case 'map': value = new Map(); break;
      case 'set': value = new Set(); break;
      case 'date': value = new Date(Number(item.value)); break;
      case 'regexp': value = new RegExp(item.source, item.flags); break;
      case 'error': value = new Error(item.message); value.name = item.name; value.stack = item.stack; break;
      case 'array-buffer': value = Uint8Array.from(Buffer.from(item.value, 'base64')).buffer; break;
      case 'view': {
        const buffer = decode(item.buffer);
        const constructors = { Uint8Array, Uint8ClampedArray, Int8Array, Uint16Array, Int16Array, Uint32Array, Int32Array, Float32Array, Float64Array, BigInt64Array, BigUint64Array, DataView };
        if (item.kind === 'DataView') value = new DataView(buffer, item.byte_offset, item.byte_length);
        else if (constructors[item.kind]) value = new constructors[item.kind](buffer, item.byte_offset, item.byte_length / constructors[item.kind].BYTES_PER_ELEMENT);
        else throw new Error('Unknown archive binary type');
        break;
      }
      default: throw new Error('Unknown archive node type');
    }
    if (!Number.isInteger(item.id) || references.has(item.id)) throw new Error('Invalid archive object identifier');
    references.set(item.id, value);
    if (item.type === 'object' || item.type === 'array') for (const [key, child] of item.value) Object.defineProperty(value, key, { value: decode(child), enumerable: true, writable: true, configurable: true });
    else if (item.type === 'map') for (const [key, child] of item.value) value.set(decode(key), decode(child));
    else if (item.type === 'set') for (const child of item.value) value.add(decode(child));
    else if (item.type === 'error') value.cause = decode(item.cause);
    return value;
  };
  return decode(node);
}

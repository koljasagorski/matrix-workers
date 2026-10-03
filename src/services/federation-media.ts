// Federation media requires two MIME parts, not a bare image response (Matrix 1.11).
// Stream the R2 body so uploads do not consume the Worker's memory limit.
export function federationMediaResponse(body: ReadableStream<Uint8Array>, contentType: string, filename?: string | null): Response {
  const boundary = `matrix-${crypto.randomUUID()}`;
  const safeType = /^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+(?:;[^\r\n]*)?$/i.test(contentType) ? contentType : 'application/octet-stream';
  const disposition = filename ? `Content-Disposition: inline; filename*=UTF-8''${encodeURIComponent(filename).replace(/'/g,'%27')}\r\n` : '';
  const encoder = new TextEncoder();
  const reader = body.getReader();
  let started = false;
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        if (!started) {
          started = true;
          controller.enqueue(encoder.encode(`--${boundary}\r\nContent-Type: application/json\r\n\r\n{}\r\n--${boundary}\r\nContent-Type: ${safeType}\r\n${disposition}\r\n`));
          return;
        }
        const { done, value } = await reader.read();
        if (done) { controller.enqueue(encoder.encode(`\r\n--${boundary}--\r\n`)); controller.close(); reader.releaseLock(); }
        else controller.enqueue(value);
      } catch (error) { await reader.cancel().catch(() => {}); controller.error(error); }
    },
    cancel: reason => reader.cancel(reason),
  });
  return new Response(stream, { headers: { 'Content-Type': `multipart/mixed; boundary=${boundary}`, 'X-Content-Type-Options':'nosniff' } });
}

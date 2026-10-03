// Room events, direct device messages and device keys use independent streams.
export function parseSyncPosition(token?: string): { events: number; toDevice: number; keys: number; receipts: number; accountData: number } {
  const composite = token?.match(/^s?(\d+)(?:_td(\d+))?(?:_dk(\d+))?(?:_rr(\d+))?(?:_ad(\d+))?$/);
  if (!composite) return { events: 0, toDevice: 0, keys: 0, receipts: 0, accountData: 0 };
  return { events:Number(composite[1]), toDevice:Number(composite[2] ?? 0), keys:Number(composite[3] ?? 0), receipts:Number(composite[4] ?? 0),
    accountData:Number(composite[5] ?? 0) };
}
export function syncPosition(events: number, toDevice: number, keys: number, receipts = 0, accountData = 0): string {
  return `s${events}_td${toDevice}_dk${keys}${receipts ? `_rr${receipts}` : ''}${accountData ? `_ad${accountData}` : ''}`;
}
export async function deviceKeyPosition(db: D1Database): Promise<number> {
  const row = await db.prepare("SELECT position FROM stream_positions WHERE stream_name='device_keys'").first<{position:number}>();
  return row?.position ?? 0;
}
export async function changedDeviceUsers(db: D1Database, userId: string, from: number, to: number): Promise<string[]> {
  const rows = await db.prepare(`SELECT DISTINCT dkc.user_id FROM device_key_changes dkc
    WHERE dkc.stream_position > ? AND dkc.stream_position <= ? AND
      (dkc.user_id = ? OR EXISTS (SELECT 1 FROM room_memberships a JOIN room_memberships b ON a.room_id=b.room_id
        WHERE a.user_id=? AND a.membership='join' AND b.user_id=dkc.user_id AND b.membership='join'))`)
    .bind(from,to,userId,userId).all<{user_id:string}>();
  // A deleted device changes its user's key list; it does not mean that user
  // has left every shared room.
  return rows.results.map(row=>row.user_id);
}

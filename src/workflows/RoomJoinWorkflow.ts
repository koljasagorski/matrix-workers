// Kept as a durable entrypoint for callers outside the Client-Server routes.
// Client joins await the same implementation before reporting success.
import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import type { Env } from '../types';
import { joinRemoteRoom, locateRoom } from '../services/remote-rooms';

export interface JoinParams {
  roomId: string; userId: string; isRemote: boolean; remoteServer?: string;
  displayName?: string; avatarUrl?: string; reason?: string;
}
export interface JoinResult { eventId: string; roomId: string; success: boolean; error?: string }
export class RoomJoinWorkflow extends WorkflowEntrypoint<Env, JoinParams> {
  async run(event: WorkflowEvent<JoinParams>, step: WorkflowStep): Promise<JoinResult> {
    return step.do('verified-remote-join', { retries: { limit: 0, delay: '1 second' }, timeout: '10 minutes' }, async () => {
      const p = event.payload;
      if (!p.isRemote) throw new Error('Local joins must use the room API');
      const location = await locateRoom(this.env, p.roomId, p.remoteServer ? [p.remoteServer] : []);
      await joinRemoteRoom(this.env, location, p.userId, p.reason);
      const membership = await this.env.DB.prepare('SELECT event_id FROM room_memberships WHERE room_id = ? AND user_id = ?')
        .bind(p.roomId, p.userId).first<{ event_id: string }>();
      return { eventId: membership!.event_id, roomId: p.roomId, success: true };
    });
  }
}

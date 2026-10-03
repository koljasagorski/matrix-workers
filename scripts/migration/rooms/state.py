"""Reconstruct known historic state with the pinned native state resolver.

Saved full snapshots and current state are authoritative. For older events the
source never retained complete state; only known DAG/auth-chain state is used.
The report distinguishes those groups rather than claiming missing history.
"""
import asyncio


async def reconstruct(plan):
    # Synapse normally imports event_auth during homeserver startup. Import it
    # first here as well, before state.v2's reciprocal module imports.
    from synapse import event_auth  # noqa: F401
    from synapse.api.room_versions import KNOWN_ROOM_VERSIONS
    from synapse.events import make_event_from_dict
    from synapse.state.v2 import resolve_events_with_store
    from synapse.storage.databases.main.event_federation import StateDifference

    records = plan["native_events"]
    parsed = {eid: make_event_from_dict(record["wire"],
        KNOWN_ROOM_VERSIONS[plan["rooms"][record["room_id"]]["room_version"]])
        for eid, record in records.items()}

    def closure(ids):
        found = set(ids)
        todo = list(ids)
        while todo:
            for auth in records[todo.pop()]["payload"].get("auth_events", []):
                if auth not in found:
                    found.add(auth)
                    todo.append(auth)
        return found

    class Clock:
        async def sleep(self, duration):
            await asyncio.sleep(0)

    class Store:
        async def get_events(self, ids, allow_rejected=False):
            return {eid: parsed[eid] for eid in ids if eid in parsed}

        async def get_auth_chain_difference(self, room_id, sets, conflicted_state,
                                            additional_backwards_reachable_conflicted_events):
            chains = [closure(ids) for ids in sets]
            # Rooms were persisted without chain-cover indices: the native v2.1
            # resolver also falls back to the entire auth difference in this case.
            return StateDifference(set.union(*chains) - set.intersection(*chains), None)

    states, active, reconstructed = {}, set(), set()
    async def visit(eid):
        if eid in states:
            return states[eid]
        if eid in active:
            raise ValueError("Cyclic event graph")
        active.add(eid)
        record = records[eid]
        event = record["payload"]
        if eid in plan["snapshots"]:
            state = dict(plan["snapshots"][eid])
        else:
            parents = [parent for parent in event.get("prev_events", []) if parent in records]
            parent_states = [await visit(parent) for parent in parents]
            auth_state = {(records[auth]["payload"]["type"], records[auth]["payload"]["state_key"]): auth
                for auth in event.get("auth_events", [])}
            if parent_states:
                room_parsed = {key: value for key, value in parsed.items() if value.room_id == record["room_id"]}
                state = dict(await resolve_events_with_store(Clock(), record["room_id"],
                    KNOWN_ROOM_VERSIONS[plan["rooms"][record["room_id"]]["room_version"]],
                    parent_states, room_parsed, Store()))
                # Auth events fill missing state at gaps, never replace state
                # resolved from the known previous-event graph.
                for key, auth in auth_state.items():
                    state.setdefault(key, auth)
            else:
                state = auth_state
            if record["stream_ordering"] is not None:
                reconstructed.add(eid)
        if "state_key" in event:
            state[(event["type"], event["state_key"])] = eid
        states[eid] = state
        active.remove(eid)
        return state

    for eid in records:
        await visit(eid)
    return states, reconstructed


def build_states(plan):
    return asyncio.run(reconstruct(plan))

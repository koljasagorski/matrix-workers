"""Loopback HTTP proxy mapping legacy hashed sessions to native Synapse tokens.

Mapping never creates sessions at request time: native logout/device deletion
revokes the imported token permanently. No access logs or request-body logging.
"""
import argparse
import importlib
import json
import sqlite3
import re
import ipaddress
from pathlib import Path
from urllib.parse import quote

from aiohttp import ClientError, ClientSession, ClientTimeout, web
from multidict import CIMultiDict
from yarl import URL

from .common import LEGACY_POSITION, native_token, token_hash

HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
               "te", "trailer", "transfer-encoding", "upgrade", "content-length"}
SLIDING_PATHS = {"/_matrix/client/v4/sync", "/_matrix/client/unstable/org.matrix.msc3575/sync",
                 "/_matrix/client/unstable/org.matrix.msc4186/sync"}
NATIVE_SLIDING_PATH = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
FILTER_PATH = re.compile(r"^/_matrix/client/(?:v3|r0)/user/([^/]+)/filter/([^/]+)$")


def private_health_probe(request):
    if request.query.get("maintenance_probe") != "true":
        return False
    try:
        return ipaddress.ip_address(request.remote or "").is_loopback
    except ValueError:
        return False


class TokenBridge:
    def __init__(self, database, secret):
        self.database = sqlite3.connect(f"file:{quote(str(Path(database).resolve()))}?mode=ro", uri=True)
        self.database.row_factory = sqlite3.Row
        self.secret = secret
        if len(secret) != 32:
            raise ValueError("Compatibility secret must contain exactly 32 bytes")

    def access(self, raw):
        hashed = token_hash(raw)
        row = self.database.execute("SELECT user_id,device_id FROM access_tokens WHERE token_hash=?", (hashed,)).fetchone()
        return (native_token(self.secret, hashed), dict(row)) if row else (raw, None)

    def refresh(self, raw):
        hashed = token_hash(raw)
        row = self.database.execute("SELECT 1 FROM refresh_tokens WHERE token_hash=?", (hashed,)).fetchone()
        return native_token(self.secret, hashed, "refresh") if row else raw

    def filter(self, user_id, filter_id):
        row = self.database.execute("SELECT filter_json FROM client_filters WHERE user_id=? AND filter_id=?", (user_id, filter_id)).fetchone()
        return json.loads(row[0]) if row else None


def normalize_request(path, query, body):
    """Only recognize the old exact grammar. Native opaque positions pass through."""
    query = list(query)
    legacy = False
    sliding = path in SLIDING_PATHS or path == NATIVE_SLIDING_PATH
    classic = path in ("/_matrix/client/v3/sync", "/_matrix/client/r0/sync")
    parameter = "pos" if sliding else "since"
    if classic or sliding:
        filtered = []
        for key, value in query:
            if key == parameter and LEGACY_POSITION.fullmatch(value):
                legacy = True
            else:
                filtered.append((key, value))
        query = filtered
    if sliding and body:
        parsed = json.loads(body)
        if not isinstance(parsed, dict):
            raise ValueError("Sliding sync body must be an object")
        position = parsed.pop("pos", None)
        if isinstance(position, str):
            if LEGACY_POSITION.fullmatch(position):
                legacy = True
            elif not any(key == "pos" for key, _ in query):
                query.append(("pos", position))
        # Synapse's native to-device cursor is also a decimal string. Only
        # reset that independent cursor when the room position proves this is
        # an old sync request; otherwise acknowledgements must pass through.
        extensions = parsed.get("extensions", {})
        if not isinstance(extensions, dict):
            raise ValueError("Extensions must be an object")
        to_device = extensions.get("to_device", {})
        if legacy and isinstance(to_device, dict) and isinstance(to_device.get("since"), str) and LEGACY_POSITION.fullmatch(to_device["since"]):
            to_device.pop("since")
        body = json.dumps(parsed, separators=(",", ":")).encode()
        path = NATIVE_SLIDING_PATH
    return path, query, body, legacy


async def create_app(config):
    bridge = TokenBridge(config["token_database"], Path(config["secret_file"]).read_bytes())
    upstream = config.get("upstream", "http://127.0.0.1:8008").rstrip("/")
    hook = getattr(importlib.import_module(config["archive_module"]), "handle") if config.get("archive_module") else None
    session = ClientSession(timeout=ClientTimeout(total=None, sock_connect=10, sock_read=180), auto_decompress=False)
    app = web.Application(client_max_size=config.get("max_body_bytes", 64 * 1024 * 1024))

    def maintenance():
        marker = config.get("maintenance_file")
        if not marker:
            return False
        try:
            return Path(marker).exists()
        except OSError:
            return True

    def maintenance_response(health=False):
        payload = {"status": "maintenance"} if health else {"errcode": "M_UNKNOWN", "error": "Homeserver migration in progress"}
        return web.json_response(payload, status=503, headers={"Retry-After": "30", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"})

    async def health(request):
        if maintenance() and not private_health_probe(request):
            return maintenance_response(health=True)
        try:
            async with session.get(upstream + "/_matrix/client/versions", headers={"Accept-Encoding": "identity"}, timeout=ClientTimeout(total=5)) as response:
                payload = await response.json()
                if response.status == 200 and isinstance(payload, dict) and isinstance(payload.get("versions"), list) and payload["versions"]:
                    return web.json_response({"status": "ok"}, headers={"Cache-Control": "no-store"})
        except (ClientError, ConnectionError, TimeoutError, ValueError):
            pass
        return web.json_response({"status": "unavailable"}, status=503, headers={"Cache-Control": "no-store"})

    cors = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400"}

    async def well_known(request):
        if request.method == "OPTIONS":
            return web.Response(status=204, headers=cors)
        server_name = config.get("server_name", "m.sgr.ski")
        if request.path.endswith("/server"):
            payload = {"m.server": config.get("federation_server", server_name + ":443")}
        else:
            payload = {"m.homeserver": {"base_url": config.get("public_base_url", "https://" + server_name)}}
        return web.json_response(payload, headers=cors)

    async def prepare_matrix_cors(request, response):
        if request.path.startswith("/_matrix/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, POST, PUT, DELETE, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Authorization"

    app.on_response_prepare.append(prepare_matrix_cors)

    async def proxy_response(request, url, headers, body, query=None):
        async with session.request(request.method, url, params=query, headers=headers, data=body, allow_redirects=False) as response:
            out = web.StreamResponse(status=response.status, headers=CIMultiDict((k, v) for k, v in response.headers.items() if k.lower() not in HOP_HEADERS))
            await out.prepare(request)
            async for chunk in response.content.iter_chunked(65536):
                await out.write(chunk)
            await out.write_eof()
            return out

    async def handler(request):
        if request.method == "OPTIONS" and request.path.startswith("/_matrix/"):
            return web.Response(status=204)
        if maintenance():
            return maintenance_response()
        # Administration is available only via the private native listener.
        # The public bridge does not expose either legacy or native admin APIs.
        if request.path == "/admin" or request.path.startswith(("/admin/", "/_synapse/admin", "/_matrix/client/v3/admin", "/_matrix/client/r0/admin")):
            return web.json_response({"errcode": "M_UNRECOGNIZED", "error": "Unrecognized request"}, status=404)
        headers = CIMultiDict((k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS | {"host"})
        authorization = request.headers.get("Authorization", "")
        if request.path.startswith("/_matrix/federation/") or (
                request.path.startswith("/_matrix/") and authorization.lower().startswith("x-matrix ")):
            # Matrix signatures cover the original escaped URI including its raw
            # query, plus the original JSON body. No client token/filter handling
            # or URL canonicalization may alter these server-to-server requests.
            body = await request.read()
            try:
                return await proxy_response(request, URL(upstream + request.raw_path, encoded=True), headers, body)
            except (ClientError, ConnectionError, TimeoutError):
                return web.json_response({"errcode": "M_UNKNOWN", "error": "Homeserver temporarily unavailable"}, status=502)
        query = [(k, v) for k, v in request.query.items() if k != "access_token"]
        raw = authorization[7:] if authorization.lower().startswith("bearer ") else request.query.get("access_token")
        mapped_identity = None
        if raw:
            bearer, mapped_identity = bridge.access(raw)
            headers["Authorization"] = "Bearer " + bearer
        body = await request.read()
        try:
            path, query, body, legacy = normalize_request(request.path, query, body)
            if path.endswith("/refresh") and body:
                refresh = json.loads(body)
                if isinstance(refresh, dict) and isinstance(refresh.get("refresh_token"), str):
                    refresh["refresh_token"] = bridge.refresh(refresh["refresh_token"])
                    body = json.dumps(refresh, separators=(",", ":")).encode()
        except (ValueError, TypeError):
            return web.json_response({"errcode": "M_BAD_JSON", "error": "Invalid request body"}, status=400)
        request["compat_query"] = query
        request["compat_body"] = body
        request["compat_legacy_sync"] = legacy
        request["compat_path"] = path
        try:
            identity = None
            filter_endpoint = FILTER_PATH.fullmatch(path)
            filter_id = next((value for key, value in query if key == "filter" and not value.startswith("{")), None)
            if hook or filter_endpoint or filter_id:
                if raw:
                    async with session.get(upstream + "/_matrix/client/v3/account/whoami", headers={"Authorization": headers["Authorization"], "Accept-Encoding": "identity"}) as check:
                        if check.status == 200:
                            identity = await check.json()
                            identity["access_token"] = headers["Authorization"][7:]
                            if mapped_identity and (identity.get("user_id") != mapped_identity["user_id"] or identity.get("device_id") != mapped_identity["device_id"]):
                                return web.json_response({"errcode": "M_UNKNOWN_TOKEN", "error": "Session identity mismatch"}, status=401)
                if filter_id and identity:
                    saved_filter = bridge.filter(identity["user_id"], filter_id)
                    if saved_filter is not None:
                        query = [(key, json.dumps(saved_filter, separators=(",", ":")) if key == "filter" else value) for key, value in query]
                        request["compat_query"] = query
                if filter_endpoint and identity and request.method == "GET":
                    if identity["user_id"] != filter_endpoint[1]:
                        return web.json_response({"errcode": "M_FORBIDDEN", "error": "Filter belongs to another user"}, status=403)
                    saved_filter = bridge.filter(identity["user_id"], filter_endpoint[2])
                    if saved_filter is not None:
                        return web.json_response(saved_filter)
            if hook:
                result = await hook(request, identity, session, upstream, headers)
                if result is not None:
                    return result
            return await proxy_response(request, upstream + path, headers, body, query)
        except (ClientError, ConnectionError, TimeoutError):
            return web.json_response({"errcode": "M_UNKNOWN", "error": "Homeserver temporarily unavailable"}, status=502)

    app.router.add_get("/health", health)
    for path in ("/.well-known/matrix/server", "/.well-known/matrix/client"):
        app.router.add_get(path, well_known)
        app.router.add_options(path, well_known)
    app.router.add_route("*", "/{path:.*}", handler)

    async def cleanup(_):
        await session.close()
        bridge.database.close()

    app.on_cleanup.append(cleanup)
    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    web.run_app(create_app(config), host=config.get("host", "127.0.0.1"), port=config.get("port", 8009), access_log=None)


if __name__ == "__main__":
    main()

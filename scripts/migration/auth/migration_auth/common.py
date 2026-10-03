import base64
import hashlib
import hmac
import re
import zlib

LEGACY_POSITION = re.compile(r"^s?\d+(?:_td\d+)?(?:_dk\d+)?(?:_rr\d+)?(?:_ad\d+)?$")
TOKEN_HASH = re.compile(r"^[A-Za-z0-9_-]{43}$")


def token_hash(token: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(token.encode()).digest()).decode().rstrip("=")


def native_token(secret: bytes, hashed_token: str, kind: str = "access") -> str:
    if len(secret) != 32 or kind not in ("access", "refresh") or not TOKEN_HASH.fullmatch(hashed_token):
        raise ValueError("Invalid compatibility token parameters")
    digest = hmac.new(secret, f"matrix-workers:{kind}:{hashed_token}".encode(), hashlib.sha256).digest()
    if kind == "refresh":
        # Synapse 1.162 checks this envelope/CRC before looking up a refresh
        # token. Use hex for the HMAC so underscores cannot split its fields.
        base = "syr_bXdjb21wYXQ_" + digest.hex()
        number = zlib.crc32(base.encode("ascii"))
        alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        crc = ""
        while number:
            number, remainder = divmod(number, 62)
            crc = alphabet[remainder] + crc
        return base + "_" + crc.rjust(6, "0")
    return "mwcompat_" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def verify_legacy_password(password: str, encoded: str) -> bool:
    if not isinstance(password, str) or not isinstance(encoded, str):
        return False
    parts = encoded.split("$")
    if len(parts) != 5 or parts[:2] != ["", "pbkdf2-sha256"] or parts[2] != "100000":
        return False
    try:
        salt = base64.b64decode(parts[3], validate=True)
        expected = base64.b64decode(parts[4], validate=True)
        if len(salt) != 16 or len(expected) != 32:
            return False
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000, dklen=32)
        return hmac.compare_digest(actual, expected)
    except (ValueError, UnicodeError):
        return False

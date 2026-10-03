"""Official Synapse password-provider callback; no private Synapse interfaces."""
from .common import verify_legacy_password


class LegacyPasswordProvider:
    def __init__(self, config, api):
        self.api = api
        api.register_password_auth_provider_callbacks(
            auth_checkers={("m.login.password", ("password",)): self.check_auth}
        )

    @staticmethod
    def parse_config(config):
        return config or {}

    async def check_auth(self, user, login_type, login_dict):
        if login_type != "m.login.password" or not isinstance(login_dict.get("password"), str):
            return None
        user_id = self.api.get_qualified_user_id(user)

        def credentials(txn):
            txn.execute(
                "SELECT u.password_hash,u.deactivated,u.locked,u.suspended,l.password_hash "
                "FROM users u JOIN migration_legacy_passwords l ON l.user_id=u.name WHERE u.name=?",
                (user_id,),
            )
            return txn.fetchone()

        row = await self.api.run_db_interaction("migration_legacy_password", credentials)
        # A native password reset takes precedence immediately. Never re-register
        # deleted accounts or keep a previous password valid after a reset.
        if not row or row[0] or row[1] or row[2] or row[3]:
            return None
        if verify_legacy_password(login_dict["password"], row[4]):
            return user_id, None
        return None

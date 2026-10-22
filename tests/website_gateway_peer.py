"""Opt-in real Gateway peer. CP identity is synthetic; private HTTPS is real."""

import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
spec = importlib.util.spec_from_file_location("fixture_network", config["network_guard"])
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
guard.install_guard([("127.0.0.1", config["conversation_port"])], allow_bound_loopback=True)
assert guard.self_check() == 3
if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
sys.path.insert(0, config["gateway_source"])

from aiohttp import web  # noqa: E402
from services.control_plane_client import WidgetValidation  # noqa: E402
from services.website_session_client import WebsiteSessionClient  # noqa: E402
from services.widget_key_manager import WidgetKeyManager  # noqa: E402

from main import create_app, create_runner  # noqa: E402


class SyntheticControlPlane:
    async def validate_widget_key(self, key):
        if key != config["widget_key"]:
            return None
        return WidgetValidation(
            config["integration_id"],
            config["agent_id"],
            config["owner_id"],
            {},
            True,
            config["binding_generation"],
        )


async def serve():
    private = WebsiteSessionClient(
        config["conversation_url"],
        config["service_token"],
        expected_peer="127.0.0.1",
        trusted_ca_file=config["ca_file"],
    )
    app = create_app(WidgetKeyManager(SyntheticControlPlane()), None, common_sessions=private)
    runner = create_runner(app)
    try:
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", config["gateway_port"]).start()
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
        await private.close()


asyncio.run(serve())

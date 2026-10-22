"""Opt-in actual Widget→Gateway→TLS→Conversation/PG owner handoff composition."""

import asyncio
import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from models import DeliveryAttempt
from services.website_worker import WebsiteWorker
from tests.test_website_v3_execution import WebsiteV3Port
from tests.website_common_fixture import due
from tests.website_cross_service.credential_gateway_http import composed as gateway_fixture
from tests.website_cross_service.credential_gateway_http import revision
from tests.website_cross_service.credential_gateway_http import sh as session_fixture
from tests.website_cross_service.credential_gateway_http import suppliers as supplier_fixture
from tests.website_cross_service.credential_gateway_http import wh as provision_fixture

composed, sh, suppliers, wh = gateway_fixture, session_fixture, supplier_fixture, provision_fixture


async def test_actual_widget_handoff_human_return_new_ai_and_reload(composed, tmp_path):
    peer = composed
    source = json.loads(Path(os.environ["TRACK_WEBSITE_FIXTURE_INPUT"]).read_text(encoding="utf-8"))
    widget = Path(source["widget_source"])
    assert revision(widget, "rev-parse", "HEAD") == source["widget_revision"]
    assert not revision(widget, "status", "--porcelain") and not (widget / ".env").exists()
    config = dict(
        widget_script=str(widget / "public/widget.js"),
        gateway_url=str(peer.public.base_url),
        widget_key=peer.headers["X-Widget-Key"],
        playwright_module=source["playwright_module"],
        browser=source["browser"],
        result_file=str(tmp_path / "browser-result.json"),
        screenshot_prefix=str(tmp_path / "composed-widget"),
    )
    path = tmp_path / "browser-input.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    env = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "COMSPEC"}
    }
    with (tmp_path / "private-browser.log").open("xb") as log:
        process = subprocess.Popen(
            [source["node"], str(Path(__file__).with_name("widget_peer.mjs")), str(path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            env=env,
            text=True,
            encoding="utf-8",
        )

        async def event(expected):
            line = await asyncio.wait_for(asyncio.to_thread(process.stdout.readline), 20)
            value = json.loads(line) if line else {}
            assert value.get("event") == expected, {
                "event": value.get("event"),
                "error_type": value.get("error_type"),
            }
            return value

        def command(value):
            process.stdin.write(json.dumps(dict(command=value)) + "\n")
            process.stdin.flush()

        def inbox_command(case, **extra):
            return dict(
                command_id=str(uuid4()),
                expected_authority_revision=case["authority_revision"],
                expected_case_revision=case["case_revision"],
                **extra,
            )

        try:
            first = await event("CUSTOMER_ADMITTED")
            await due(peer.wh)
            assert await WebsiteWorker(
                peer.wh.database, peer.wh.settings, WebsiteV3Port("HANDOFF")
            ).run_once()
            response = await peer.owner.get("/public/v1/inbox/cases")
            assert response.status_code == 200 and len(response.json()["items"]) == 1
            case = response.json()["items"][0]
            assert case["provider_thread_id"] == first["session_id"]
            base = "/public/v1/inbox/cases/" + case["case_id"]
            claimed = await peer.owner.post(base + "/claim", json=inbox_command(case))
            assert claimed.status_code == 200
            case = claimed.json()["case"]
            sent = await peer.owner.post(
                base + "/messages",
                json=inbox_command(
                    case,
                    expected_assignment_revision=case["assignment_revision"],
                    text="Ответ владельца",
                ),
            )
            assert sent.status_code == 200
            command("EXPECT_HUMAN")
            await event("HUMAN_VISIBLE")
            for _ in range(100):
                async with peer.wh.database.session_factory() as db:
                    attempt = await db.scalar(select(DeliveryAttempt))
                    if attempt is not None and attempt.writer_state == "ENDED":
                        break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("Original Gateway writer end was not committed")
            returned = await peer.owner.post(
                base + "/return-to-agent", json=inbox_command(sent.json()["case"])
            )
            assert returned.status_code == 200
            port = WebsiteV3Port("DRAFT")
            worker = WebsiteWorker(peer.wh.database, peer.wh.settings, port)
            assert not await worker.run_once()
            command("SEND_NEW_INPUT")
            await event("NEW_INPUT_ADMITTED")
            await due(peer.wh)
            assert await worker.run_once() and len(port.requests) == 1
            command("EXPECT_AGENT")
            await event("PASS")
            process.stdin.close()
            assert await asyncio.to_thread(process.wait, timeout=5) == 0
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=5)
            process.stdin.close()
            process.stdout.close()
    assert revision(widget, "rev-parse", "HEAD") == source["widget_revision"]
    assert not revision(widget, "status", "--porcelain")
    async with peer.wh.database.session_factory() as db:
        attempts = list(await db.scalars(select(DeliveryAttempt)))
        assert len(attempts) == 2 and all(item.writer_state == "ENDED" for item in attempts)

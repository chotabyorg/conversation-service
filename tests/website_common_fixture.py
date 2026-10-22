"""Private Website enrollment, real credential ingress and common selection fixture."""
import secrets
from uuid import UUID

import pytest_asyncio
from sqlalchemy import func, select

from core.config import ManagedBinding, SettlingPolicy
from models import SettlingBurst
from schemas.contracts import Channel
from services.settling import select_settled_turn
from tests.test_website_credential_ingress import post, request
from tests.test_website_grant import enroll
from tests.test_website_session import issued


@pytest_asyncio.fixture
async def common_website(sh, suppliers, test_database_clock):
    binding = sh.settings.website_bindings[0]
    settings = sh.settings.model_copy(update=dict(
        managed_ai_base_url="http://127.0.0.1:1",
        managed_ai_bindings=(ManagedBinding(owner_id=binding.owner_id,
            integration_id=binding.integration_id, channel="WEBSITE_WIDGET",
            agent_id=binding.agent_id, service_token=secrets.token_urlsafe(32)),),
        turn_settling_policies=(SettlingPolicy(integration_id=binding.integration_id,
            channel="WEBSITE_WIDGET", version="common-website-v1",
            quiet_window_ms=100, max_wait_ms=1000),),
        website_worker_enabled=True, website_worker_principal_id="worker",
        website_worker_lease_seconds=5,
    ))
    sh.settings = settings
    sh.client._transport.app.state.settings = settings
    sh.principal = settings.auth_principals[1].principal()
    sh.agent_id, sh.clock = str(binding.agent_id), test_database_clock
    await enroll(sh, suppliers)
    sh.created = await issued(sh)
    yield sh


async def append(wh, text="customer input"):
    value = request(wh, wh.created)
    value["text"] = text
    response = await post(wh, value)
    assert response.status_code == 200, response.status_code
    return response.json()


async def due(wh):
    async with wh.database.session_factory() as session:
        ready = await session.scalar(select(func.max(SettlingBurst.ready_at)))
    wh.clock.advance_to(max(ready, wh.clock.current))


async def select_turn(wh):
    await append(wh)
    await due(wh)
    selected = await select_settled_turn(wh.database, wh.principal, wh.settings,
                                          (Channel.WEBSITE_WIDGET,))
    assert isinstance(selected, UUID)
    return selected

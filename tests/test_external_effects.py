"""Conversation authority and immutable opaque permits over real PostgreSQL."""

from __future__ import annotations

import ast
import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from schemas.effects import (
    EffectReason,
    ExternalEffectEligibilityRequest,
    ExternalEffectEligibilityResult,
    ExternalEffectPermitRequest,
    ExternalEffectPermitResult,
    request_fingerprint,
)
from tests.conftest import observed_waiters
from tests.test_database import violation

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "external_effects" / "contract-v1.json"
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
FIXTURE_AGENT = UUID(FIXTURE["context"]["agent_id"])
FIXTURE_MANAGED_FINGERPRINT = FIXTURE["context"]["managed_request_fingerprint"]


def context_for(conversation: dict, current: dict, origin: dict) -> dict:
    return {
        "contract_version": 1,
        "conversation_id": conversation["id"],
        "turn_id": current["id"],
        "origin_turn_id": origin["id"],
        "owner_id": conversation["owner_id"],
        "integration_id": conversation["integration_id"],
        "channel": conversation["channel"],
        "agent_id": str(FIXTURE_AGENT),
        "authority_revision": current["authority_revision"],
        "managed_request_fingerprint": FIXTURE_MANAGED_FINGERPRINT,
    }


def permit_request(context: dict, *, operation_id: UUID | None = None) -> dict:
    return {
        **context,
        "operation_namespace": "main-chota.ai-business.v1",
        "operation_id": str(operation_id or uuid4()),
        "operation_fingerprint": "b" * 64,
    }


async def select_at(h, accepted: dict, sequence: int, *, principal: str = "a") -> dict:
    conversation = accepted["conversation"]
    payload = {
        "selection_key": str(uuid4()),
        "input_first_sequence": sequence,
        "input_last_sequence": sequence,
        "expected_input_revision": conversation["input_revision"],
        "expected_authority_revision": conversation["authority_revision"],
    }
    response = await h.call(
        "POST", f"/conversations/{conversation['id']}/turns", payload, principal=principal
    )
    assert response.status_code == 201, response.text
    return response.json()["turn"]


async def complete_no_response(
    h, sql, turn: dict, *, managed_agent_id: UUID = FIXTURE_AGENT
) -> dict:
    claim = await h.claim(turn)
    await stamp_managed_dispatch(sql, turn, agent_id=managed_agent_id)
    response = await h.call(
        "POST",
        f"/turns/{turn['id']}/complete",
        dict(h.proof(claim), outcome="NO_RESPONSE"),
    )
    assert response.status_code == 200, response.text
    return response.json()["turn"]


async def stamp_managed_dispatch(
    sql, turn: dict, *, agent_id: UUID = FIXTURE_AGENT
) -> None:
    """Use a database-valid frozen marker without making any upstream call."""
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "UPDATE agent_turns SET managed_agent_id=:agent, "
                "managed_dispatch_epoch=claim_epoch, "
                "managed_dispatch_started_at=clock_timestamp(), "
                "managed_request_fingerprint=:fingerprint WHERE id=:id"
            ),
            {"agent": str(agent_id), "fingerprint": FIXTURE_MANAGED_FINGERPRINT,
             "id": turn["id"]},
        )


async def claim_and_stamp(h, sql, turn: dict) -> dict:
    claim = await h.claim(turn)
    await stamp_managed_dispatch(sql, turn)
    return claim


async def test_shared_contract_fixture_hashes_and_strict_dtos():
    context = ExternalEffectEligibilityRequest.model_validate(FIXTURE["context"])
    request = ExternalEffectPermitRequest.model_validate(FIXTURE["permit_request"])
    assert request_fingerprint(context) == FIXTURE["eligibility_result"]["request_fingerprint"]
    assert request_fingerprint(request) == FIXTURE["permit_result"]["request_fingerprint"]
    assert FIXTURE["eligibility_result"]["reason"] == EffectReason.ELIGIBLE.value
    invalid = dict(FIXTURE["permit_request"], customer_text="must not cross the boundary")
    with pytest.raises(ValidationError):
        ExternalEffectPermitRequest.model_validate(invalid)


def test_authority_revision_writes_are_fenced_by_the_shared_transition_helper():
    services_root = Path(__file__).resolve().parents[1] / "services"
    increment_sites = []
    for path in sorted(services_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (isinstance(node, ast.AugAssign)
                    and isinstance(node.target, ast.Attribute)
                    and node.target.attr == "authority_revision"):
                increment_sites.append((path.relative_to(services_root).as_posix(), node.lineno))
    assert len(increment_sites) == 1
    assert increment_sites[0][0] == "authority.py"


@pytest.mark.parametrize("invalid_version", [True, 1.0])
async def test_contract_version_rejects_bool_and_float(invalid_version):
    for dto, payload in (
        (ExternalEffectEligibilityRequest, FIXTURE["context"]),
        (ExternalEffectPermitRequest, FIXTURE["permit_request"]),
        (ExternalEffectEligibilityResult, FIXTURE["eligibility_result"]),
        (ExternalEffectPermitResult, FIXTURE["permit_result"]),
    ):
        with pytest.raises(ValidationError):
            dto.model_validate({**payload, "contract_version": invalid_version})


async def test_business_effect_role_is_confined_to_sidecar_endpoints(h, sql):
    principal = next(item for item in h.settings.auth_principals if item.principal_id == "a")
    h.client._transport.app.state.settings = h.settings.model_copy(
        update={"auth_principals": (principal.model_copy(update={"role": "BUSINESS_EFFECT"}),)}
    )
    denied = await h.call("POST", "/inbound-events", h.event())
    assert denied.status_code == 404
    assert denied.json()["detail"]["code"] == "NOT_FOUND"
    for method, path in (
        ("POST", f"/conversations/{uuid4()}/control"),
        ("POST", f"/turns/{uuid4()}/claim"),
        ("GET", "/internal/openapi.json"),
    ):
        restricted = await h.call(method, path, {})
        assert restricted.status_code == 404
        assert restricted.json()["detail"]["code"] == "NOT_FOUND"
    # The path is admitted by the narrow role and then fails at entity scope.
    result = await h.call("POST", "/external-effect-eligibility", FIXTURE["context"])
    assert result.status_code == 404
    assert result.json()["detail"]["code"] == "NOT_FOUND"


async def test_latest_accepted_customer_membership_defeats_stale_selection_revision(h, sql):
    accepted = await h.ingest(provider_thread_id="p0-latest")
    origin = await select_at(h, accepted, 1)
    await complete_no_response(h, sql, origin)
    latest = accepted
    for sequence in (2, 3, 4):
        latest = await h.ingest(provider_thread_id="p0-latest", text=f"message-{sequence}")

    for sequence in (2, 3):
        current = await select_at(h, latest, sequence)
        assert current["input_revision"] == latest["conversation"]["input_revision"]
        claim = await claim_and_stamp(h, sql, current)
        context = context_for(latest["conversation"], current, origin)
        eligibility = await h.call("POST", "/external-effect-eligibility", context)
        assert eligibility.status_code == 200, eligibility.text
        assert eligibility.json()["eligible"] is False
        assert eligibility.json()["reason"] == "INPUT_SUPERSEDED"
        denied = await h.call("POST", "/external-effect-permits", permit_request(context))
        assert denied.status_code == 409, denied.text
        assert denied.json()["detail"] == {
            "code": "EXTERNAL_EFFECT_DENIED", "reason": "INPUT_SUPERSEDED"
        }
        completed = await h.call(
            "POST", f"/turns/{current['id']}/complete",
            dict(h.proof(claim), outcome="NO_RESPONSE"),
        )
        assert completed.status_code == 200, completed.text

    current = await select_at(h, latest, 4)
    await claim_and_stamp(h, sql, current)
    context = context_for(latest["conversation"], current, origin)
    eligible = await h.call("POST", "/external-effect-eligibility", context)
    assert eligible.status_code == 200 and eligible.json()["eligible"] is True
    permit = await h.call("POST", "/external-effect-permits", permit_request(context))
    assert permit.status_code == 200, permit.text


async def test_same_turn_initial_intent_can_receive_a_business_permit(h, sql):
    accepted = await h.ingest(provider_thread_id="same-turn-initial-intent")
    current = await select_at(h, accepted, 1)
    claim = await claim_and_stamp(h, sql, current)
    context = context_for(accepted["conversation"], current, current)

    eligible = await h.call("POST", "/external-effect-eligibility", context)
    assert eligible.status_code == 200, eligible.text
    assert eligible.json()["eligible"] is True
    issued = await h.call("POST", "/external-effect-permits", permit_request(context))
    assert issued.status_code == 200, issued.text
    assert issued.json()["replayed"] is False
    # Retain the claim proof as a sanity check that this remains a current
    # claimed execution rather than permitting a terminal or stale Turn.
    assert claim["turn"]["status"] == "CLAIMED"


async def test_foreign_origin_is_not_visible_inside_scoped_effect_context(h, sql):
    foreign = await h.ingest(principal="b", provider_thread_id="foreign-origin")
    foreign_turn = await select_at(h, foreign, 1, principal="b")

    local = await h.ingest(principal="a", provider_thread_id="local-current")
    current = await select_at(h, local, 1)
    await claim_and_stamp(h, sql, current)
    context = context_for(local["conversation"], current, foreign_turn)

    eligibility = await h.call("POST", "/external-effect-eligibility", context)
    assert eligibility.status_code == 404
    assert eligibility.json()["detail"]["code"] == "NOT_FOUND"
    permit = await h.call(
        "POST", "/external-effect-permits", permit_request(context)
    )
    assert permit.status_code == 404
    assert permit.json()["detail"]["code"] == "NOT_FOUND"
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM external_effect_permits")) == 0


async def test_agent_identity_mismatch_conflicts_before_eligibility_or_permit(h, sql):
    accepted = await h.ingest(provider_thread_id="agent-marker-mismatch")
    current = await select_at(h, accepted, 1)
    await claim_and_stamp(h, sql, current)
    context = context_for(accepted["conversation"], current, current)
    context["agent_id"] = str(uuid4())

    eligibility = await h.call("POST", "/external-effect-eligibility", context)
    assert eligibility.status_code == 409, eligibility.text
    assert eligibility.json()["detail"] == {"code": "EXTERNAL_EFFECT_CONTEXT_CONFLICT"}
    permit = await h.call(
        "POST", "/external-effect-permits", permit_request(context)
    )
    assert permit.status_code == 409, permit.text
    assert permit.json()["detail"] == {"code": "EXTERNAL_EFFECT_CONTEXT_CONFLICT"}
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM external_effect_permits")) == 0


async def test_origin_from_another_managed_agent_is_stale(h, sql):
    first = await h.ingest(provider_thread_id="origin-agent-mismatch")
    origin = await select_at(h, first, 1)
    await complete_no_response(h, sql, origin, managed_agent_id=uuid4())

    latest = await h.ingest(
        provider_thread_id="origin-agent-mismatch", text="current managed input"
    )
    current = await select_at(h, latest, 2)
    await claim_and_stamp(h, sql, current)
    context = context_for(latest["conversation"], current, origin)

    eligibility = await h.call("POST", "/external-effect-eligibility", context)
    assert eligibility.status_code == 200, eligibility.text
    assert eligibility.json()["eligible"] is False
    assert eligibility.json()["reason"] == "ORIGIN_STALE"
    denied = await h.call(
        "POST", "/external-effect-permits", permit_request(context)
    )
    assert denied.status_code == 409, denied.text
    assert denied.json()["detail"] == {
        "code": "EXTERNAL_EFFECT_DENIED", "reason": "ORIGIN_STALE"
    }
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM external_effect_permits")) == 0


async def test_permit_replay_precedes_later_authority_fence_and_is_immutable(h, sql):
    first = await h.ingest(provider_thread_id="replay-fence")
    origin = await select_at(h, first, 1)
    await complete_no_response(h, sql, origin)
    latest = await h.ingest(provider_thread_id="replay-fence", text="fresh confirmation")
    current = await select_at(h, latest, 2)
    await claim_and_stamp(h, sql, current)
    context = context_for(latest["conversation"], current, origin)
    request = permit_request(context)

    issued = await h.call("POST", "/external-effect-permits", request)
    assert issued.status_code == 200, issued.text
    assert issued.json()["replayed"] is False

    changed = await h.call(
        "POST",
        f"/conversations/{latest['conversation']['id']}/control",
        {"expected_authority_revision": 0, "control_mode": "AGENT"},
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["authority_revision"] == 1
    async with sql.connect() as connection:
        floor = await connection.scalar(text(
            "SELECT authority_input_floor_sequence FROM conversations WHERE id=:id"
        ), {"id": latest["conversation"]["id"]})
    assert floor == 2

    replay = await h.call("POST", "/external-effect-permits", request)
    assert replay.status_code == 200, replay.text
    assert replay.json()["replayed"] is True
    assert replay.json()["permit_id"] == issued.json()["permit_id"]

    rebinding = dict(request, operation_fingerprint="c" * 64)
    conflict = await h.call("POST", "/external-effect-permits", rebinding)
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == {"code": "PERMIT_CONFLICT"}

    fresh = permit_request(context)
    denied = await h.call("POST", "/external-effect-permits", fresh)
    assert denied.status_code == 409
    assert denied.json()["detail"] == {
        "code": "EXTERNAL_EFFECT_DENIED", "reason": "AUTHORITY_CHANGED"
    }
    async with sql.connect() as connection:
        rows = await connection.scalar(text("SELECT count(*) FROM external_effect_permits"))
    assert rows == 1
    await violation(
        sql,
        "UPDATE external_effect_permits SET operation_fingerprint=:fingerprint WHERE id=:id",
        {"id": issued.json()["permit_id"], "fingerprint": "c" * 64},
        "23514",
    )
    await violation(
        sql,
        "DELETE FROM external_effect_permits WHERE id=:id",
        {"id": issued.json()["permit_id"]},
        "23514",
    )


async def test_ingress_wins_parent_row_lock_before_fresh_permit(h, sql):
    thread = "permit-ingress-lock-order"
    first = await h.ingest(provider_thread_id=thread)
    origin = await select_at(h, first, 1)
    await complete_no_response(h, sql, origin)
    latest = await h.ingest(provider_thread_id=thread, text="turn input")
    current = await select_at(h, latest, 2)
    await claim_and_stamp(h, sql, current)
    context = context_for(latest["conversation"], current, origin)
    request = permit_request(context)

    async with sql.begin() as blocker:
        blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(
            text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
            {"id": latest["conversation"]["id"]},
        )
        ingress = asyncio.create_task(
            h.ingest(provider_thread_id=thread, text="newer accepted input")
        )
        await observed_waiters(sql, blocker_pid, 1, "Conversation permit versus ingress")
        permit_task = asyncio.create_task(
            h.call("POST", "/external-effect-permits", request)
        )
        await observed_waiters(sql, blocker_pid, 2, "Conversation permit after queued ingress")

    accepted_newer, permit_result = await asyncio.gather(ingress, permit_task)
    assert accepted_newer["message"]["sequence"] == 3
    assert permit_result.status_code == 409, permit_result.text
    assert permit_result.json()["detail"] == {
        "code": "EXTERNAL_EFFECT_DENIED", "reason": "INPUT_SUPERSEDED"
    }


async def test_permit_query_timeout_fails_closed_without_persisting(h, sql):
    accepted = await h.ingest(provider_thread_id="permit-query-timeout")
    current = await select_at(h, accepted, 1)
    await claim_and_stamp(h, sql, current)
    context = context_for(accepted["conversation"], current, current)
    request = permit_request(context)

    async with sql.begin() as blocker:
        blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(
            text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
            {"id": accepted["conversation"]["id"]},
        )
        pending = asyncio.create_task(h.call("POST", "/external-effect-permits", request))
        await observed_waiters(sql, blocker_pid, 1, "Permit statement-timeout fail-closed")
        response = await asyncio.wait_for(pending, timeout=6)

    assert response.status_code == 503, response.text
    assert response.json()["detail"]["code"] == "SERVICE_UNAVAILABLE"
    async with sql.connect() as connection:
        count = await connection.scalar(text("SELECT count(*) FROM external_effect_permits"))
    assert count == 0

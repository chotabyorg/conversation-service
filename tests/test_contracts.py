import ast
import json
import logging
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from core.logging import SafeJsonFormatter
from schemas.contracts import (
    MAX_COUNTER,
    ControlTransition,
    OperationReference,
    TurnClaim,
    TurnComplete,
    TurnCreate,
    validate_owner,
)


@pytest.mark.parametrize("invalid", [True, False, -1, 1.0, "1", MAX_COUNTER + 1])
def test_cas_counter_is_bounded_strict_integer(invalid):
    with pytest.raises(ValidationError):
        ControlTransition(expected_authority_revision=invalid, control_mode="AGENT")
    assert ControlTransition(expected_authority_revision=MAX_COUNTER, control_mode="AGENT")


@pytest.mark.parametrize("invalid", [True, False, 0, 301, 1.0, "1"])
def test_claim_ttl_is_bounded_strict_integer(invalid):
    with pytest.raises(ValidationError):
        TurnClaim(worker_ref="worker", lease_seconds=invalid)


@pytest.mark.parametrize(
    "changes",
    [
        {"origin": "BUSINESS_SERVICE"},
        {"operation_id": "https://example.com/receipt"},
        {"operation_id": "x" * 129},
        {"operation_id": "private body with spaces"},
        {"outcome": "PENDING"},
        {"request_body": "sensitive"},
    ],
)
def test_operation_refs_are_bounded_references_only(changes):
    value = dict(origin="AI_AGENT_SERVICE", operation_id="operation:123", outcome="UNKNOWN")
    value.update(changes)
    with pytest.raises(ValidationError):
        OperationReference.model_validate(value)


def test_no_duplicate_operation_refs_no_reasoning_fields_and_claim_proof_repr_redacted():
    token = "z" * 43
    payload = dict(claim_epoch=1, claim_token=token, outcome="NO_RESPONSE")
    reference = dict(origin="AI_AGENT_SERVICE", operation_id="same", outcome="UNKNOWN")
    with pytest.raises(ValidationError):
        TurnComplete(**dict(payload, operation_refs=[reference, reference]))
    with pytest.raises(ValidationError):
        TurnComplete(
            **dict(
                payload, operation_refs=[dict(reference, operation_id=str(i)) for i in range(33)]
            )
        )
    for field in ["prompt", "reasoning", "business_receipt"]:
        with pytest.raises(ValidationError):
            TurnComplete(**dict(payload, **{field: "private"}))
    assert token not in repr(TurnComplete(**payload))
    assert TurnComplete(**payload).model_dump(mode="json")["claim_token"] == "**********"


def test_owner_exact_opaque_unicode_and_input_boundary_not_crossed():
    assert validate_owner("  opaque owner é  ") == "  opaque owner é  "
    assert validate_owner("é") != validate_owner("e\u0301")
    with pytest.raises(ValidationError):
        TurnCreate(
            selection_key=uuid4(),
            input_first_sequence=2,
            input_last_sequence=1,
            expected_input_revision=1,
            expected_authority_revision=0,
        )


def test_safe_log_formatter_drops_arguments_exceptions_payload_and_unknown_extras():
    record = logging.LogRecord(
        "sqlalchemy.engine",
        logging.ERROR,
        "source",
        1,
        "sensitive message %s",
        ("sensitive token",),
        None,
    )
    record.stage = "journal"
    record.code = "EVENT_CONFLICT"
    record.conversation_id = uuid4()
    record.provider_event_id = "sensitive provider reference"
    record.claim_token = "sensitive claim token"
    record.owner_id = "sensitive owner"
    formatted = SafeJsonFormatter().format(record)
    assert "sensitive" not in formatted
    value = json.loads(formatted)
    assert value["conversation_id"] == str(record.conversation_id)
    assert value["stage"] == "journal" and value["code"] == "EVENT_CONFLICT"


def test_application_source_contains_no_provider_ai_cp_or_broker_clients():
    root = Path(__file__).resolve().parents[1]
    forbidden = {
        "httpx",
        "aiohttp",
        "requests",
        "openai",
        "redis",
        "aio_pika",
        "aiogram",
        "aiosmtplib",
    }
    for folder in ["api", "core", "db", "models", "schemas", "services"]:
        for path in (root / folder).glob("*.py"):
            tree = ast.parse(path.read_text())
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module.split(".")[0])
            allowed = (
                {"httpx"}
                if path
                in {
                    root / "services" / "managed_client.py",
                    root / "services" / "human_auth.py",
                    root / "services" / "inbox_lifecycle.py",
                }
                else set()
            )
            assert not imports & (forbidden - allowed), (
                f"Unexpected runtime network client in {path.name}"
            )

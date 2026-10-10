import json
import secrets
from uuid import uuid4

import pytest
from pydantic import ValidationError

from core.config import Settings
from schemas.managed import ECHO_FIELDS, ManagedTurnRequest, ManagedTurnResult, prepare_request


def wire_request(count=3, first=1, last=2, text="hello"):
    ids = [str(uuid4()) for _ in range(count)]
    return dict(
        conversation_id=str(uuid4()),
        turn_id=str(uuid4()),
        owner_id="  opaque é owner  ",
        integration_id=str(uuid4()),
        agent_id=str(uuid4()),
        channel="TELEGRAM",
        input_revision=count,
        authority_revision=0,
        input_first_sequence=first,
        input_last_sequence=last,
        context_last_sequence=count,
        messages=[
            dict(
                id=message_id,
                sequence=index + 1,
                actor="CUSTOMER",
                direction="INBOUND",
                content_type="TEXT",
                text=text,
            )
            for index, message_id in enumerate(ids)
        ],
        selected_input_message_ids=ids[first - 1 : last],
    )


@pytest.mark.parametrize(
    "name",
    [
        "input_revision",
        "authority_revision",
        "input_first_sequence",
        "input_last_sequence",
        "context_last_sequence",
        "contract_version",
    ],
)
@pytest.mark.parametrize("invalid", [True, False, "1", 1.0, -1, 9223372036854775808])
def test_managed_counters_strict(name, invalid):
    value = wire_request()
    value[name] = invalid
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(value)


@pytest.mark.parametrize(
    "mutation",
    [
        "gap",
        "reorder",
        "duplicate",
        "selected_reorder",
        "selected_missing",
        "foreign",
        "short",
        "channel",
        "prompt",
    ],
)
def test_projection_never_repairs_invalid_order_or_selection(mutation):
    value = wire_request()
    if mutation == "gap":
        value["messages"][1]["sequence"] = 4
    elif mutation == "reorder":
        value["messages"].reverse()
    elif mutation == "duplicate":
        value["messages"][1]["id"] = value["messages"][0]["id"]
    elif mutation == "selected_reorder":
        value["selected_input_message_ids"].reverse()
    elif mutation == "selected_missing":
        value["selected_input_message_ids"].pop()
    elif mutation == "foreign":
        value["selected_input_message_ids"][0] = str(uuid4())
    elif mutation == "short":
        value["messages"].pop()
    elif mutation == "channel":
        value["channel"] = "telegram"
    else:
        value["prompt"] = "caller injection"
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(value)


def test_max_prefix_non_suffix_any_actor_text_byte_and_actual_escape_bounds():
    value = wire_request(256, 1, 2)
    value["messages"][0]["actor"] = "HUMAN"
    value["messages"][1]["actor"] = "SYSTEM"
    request = ManagedTurnRequest.model_validate(value)
    assert prepare_request(request).request == request
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(wire_request(257))
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(wire_request(33, text="x" * 16384))
    # Raw text is within524288, but permitted escaped controls exceed the actual JSON cap.
    with pytest.raises(ValueError):
        prepare_request(ManagedTurnRequest.model_validate(wire_request(20, text="\x01" * 16384)))
    for bad in ["\x00", "\ud800", "😀" * 8193, "x" * 16385]:
        with pytest.raises(ValidationError):
            ManagedTurnRequest.model_validate(wire_request(text=bad))
    preserved = ManagedTurnRequest.model_validate(wire_request(text=" \r\n é e\u0301 "))
    assert preserved.messages[0].text == " \r\n é e\u0301 "
    assert preserved.owner_id == "  opaque é owner  "


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "DRAFT_READY", "draft_text": None},
        {"status": "NO_RESPONSE", "draft_text": "draft"},
        {"status": "FAILED", "error_code": None},
        {"status": "UNKNOWN", "error_code": "raw provider secret"},
        {"reasoning": "forbidden"},
        {"input_revision": True},
        {"operation_refs": [{"origin": "AI_AGENT_SERVICE", "operation_id": "id"}]},
    ],
)
def test_result_closed_shape_and_explicit_nullable_reference_outcome(changes):
    request = ManagedTurnRequest.model_validate(wire_request())
    value = dict(
        request.model_dump(include=set(ECHO_FIELDS)),
        status="DRAFT_READY",
        draft_text="draft",
        error_code=None,
        operation_refs=[],
    )
    value.update(changes)
    with pytest.raises(ValidationError):
        ManagedTurnResult.model_validate(value)


@pytest.mark.parametrize("field", ECHO_FIELDS)
def test_all_result_echoes_are_correlated(field):
    request = ManagedTurnRequest.model_validate(wire_request())
    fields = request.model_dump(include=set(ECHO_FIELDS))
    if field == "contract_version":
        fields[field] = 2
        result = ManagedTurnResult(**fields, status="NO_RESPONSE", draft_text=None, error_code=None)
        with pytest.raises(ValueError):
            result.correlate(request)
        return
    fields[field] = uuid4() if field.endswith("_id") else fields[field] + 1
    result = ManagedTurnResult(**fields, status="NO_RESPONSE", draft_text=None, error_code=None)
    with pytest.raises(ValueError):
        result.correlate(request)


@pytest.mark.parametrize(
    "invalid",
    [
        "https://user:secret@example.test",
        "https://example.test/path",
        "https://example.test?q=1",
        "https://example.test#fragment",
        "file:///tmp/x",
        "",
        "http://localhost:0",
        "http://bad\n.test",
    ],
)
def test_private_origin_configuration_rejects_embedded_authority_or_path(h, invalid):
    with pytest.raises(ValidationError):
        Settings(
            database_url=h.settings.database_url,
            auth_principals=h.settings.auth_principals,
            managed_ai_base_url=invalid,
            _env_file=None,
        )


def test_default_empty_registry_and_exact_unique_bindings_and_redacted_config(h):
    assert not h.settings.managed_ai_bindings and h.settings.managed_ai_base_url is None
    grant = h.grants["a"]
    binding = dict(
        owner_id=grant["owner_id"],
        integration_id=grant["integration_id"],
        channel=grant["channel"],
        agent_id=str(uuid4()),
        service_token=secrets.token_urlsafe(32),
    )

    def settings(bindings, **kwargs):
        return Settings(
            database_url=h.settings.database_url,
            auth_principals=h.settings.auth_principals,
            managed_ai_bindings=bindings,
            _env_file=None,
            **kwargs,
        )

    result = settings(json.dumps([binding]))
    assert binding["service_token"] not in repr(result)
    assert result.managed_ai_bindings[0].owner_id == grant["owner_id"]
    with pytest.raises(ValidationError):
        settings([binding, dict(binding, agent_id=str(uuid4()))])
    with pytest.raises(ValidationError):
        settings([dict(binding, service_token=h.tokens["a"])])
    for bad in [True, 0, 241, float("nan"), float("inf"), "invalid"]:
        with pytest.raises(ValidationError):
            settings([binding], managed_ai_total_seconds=bad)
    for bad in [
        dict(binding, owner_id="*"),
        dict(binding, service_token="!" * 43),
        dict(binding, provider_namespace="unaccepted"),
    ]:
        with pytest.raises(ValidationError):
            settings([bad])


def test_managed_environment_aliases_timeouts_and_source_precedence(h, monkeypatch):
    monkeypatch.setenv("MANAGED_AI_TOTAL_SECONDS", "2.5")
    monkeypatch.setenv("MANAGED_AI_BASE_URL", "http://127.0.0.1:1")
    settings = Settings(
        database_url=h.settings.database_url,
        auth_principals=h.settings.auth_principals,
        _env_file=None,
    )
    assert settings.managed_ai_total_seconds == 2.5
    assert settings.managed_ai_base_url == "http://127.0.0.1:1"
    settings = Settings(
        database_url=h.settings.database_url,
        auth_principals=h.settings.auth_principals,
        managed_ai_total_seconds=3.0,
        MANAGED_AI_TOTAL_SECONDS=4.0,
        _env_file=None,
    )
    assert settings.managed_ai_total_seconds == 4.0


def test_invalid_managed_environment_startup_omits_private_registry(h, monkeypatch):
    from core.config import load_settings

    sentinel = "synthetic-managed-registry-secret"
    registry = [
        dict(
            principal_id=entry.principal_id,
            token=entry.token.get_secret_value(),
            grants=[grant.model_dump(mode="json") for grant in entry.grants],
        )
        for entry in h.settings.auth_principals
    ]
    monkeypatch.setenv("DATABASE_URL", h.settings.database_url.get_secret_value())
    monkeypatch.setenv("CONVERSATION_AUTH_PRINCIPALS", json.dumps(registry))
    monkeypatch.setenv("MANAGED_AI_BINDINGS", sentinel)
    with pytest.raises(RuntimeError) as captured:
        load_settings()
    assert str(captured.value) == "Invalid conversation-service configuration."
    assert sentinel not in str(captured.value) and captured.value.__cause__ is None

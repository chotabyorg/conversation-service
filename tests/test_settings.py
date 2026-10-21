"""Configuration source priority must preserve explicit database and auth authority."""

import json
import os
import subprocess
import sys
from uuid import UUID

import httpx
import pytest
from asgi_lifespan import LifespanManager
from pydantic import ValidationError

from core.config import Settings, SettlingPolicy, load_settings
from main import create_app
from schemas.contracts import Channel
from tests.conftest import ROOT

ALIASES = {
    "database_url": "DATABASE_URL",
    "auth_principals": "CONVERSATION_AUTH_PRINCIPALS",
    "log_level": "LOG_LEVEL",
}


def registry(name):
    return [
        dict(
            principal_id=name,
            token=f"{name}-synthetic-token-" + "x" * 32,
            grants=[
                dict(
                    owner_id=name,
                    integration_id="00000000-0000-4000-8000-000000000001",
                    channel="TELEGRAM",
                    provider_namespace=name,
                )
            ],
        )
    ]


def values(name):
    return dict(
        database_url=f"postgresql+asyncpg://synthetic:local@127.0.0.1:1/{name}_test",
        auth_principals=registry(name),
        log_level="DEBUG",
    )


def constructor_values(data, spelling):
    return {ALIASES[key] if spelling == "alias" else key: value for key, value in data.items()}


def write_dotenv(path, data):
    path.write_text(
        "\n".join(
            f"{ALIASES[key]}={json.dumps(value) if key == 'auth_principals' else value}"
            for key, value in data.items()
        )
        + "\n",
        encoding="utf-8",
    )


def set_environment(monkeypatch, data):
    for key, value in data.items():
        monkeypatch.setenv(ALIASES[key], json.dumps(value) if key == "auth_principals" else value)


def clear_settings_environment(monkeypatch):
    # Isolate only tests specifically proving the dotenv/default source path.
    recognized = {key.lower() for key in [*ALIASES, *ALIASES.values()]}
    for key in list(os.environ):
        if key.lower() in recognized:
            monkeypatch.delenv(key)


@pytest.mark.parametrize("spelling", ["field", "alias"])
@pytest.mark.parametrize("ambient", ["valid", "invalid"])
@pytest.mark.parametrize("dotenv_enabled", [False, True])
def test_explicit_values_outrank_environment_and_dotenv(
    monkeypatch, tmp_path, spelling, ambient, dotenv_enabled
):
    dotenv = tmp_path / ".env"
    write_dotenv(dotenv, dict(values("dotenv"), log_level="WARNING"))
    inherited = dict(values("ambient"), log_level="ERROR")
    if ambient == "invalid":
        inherited.update(database_url="not-url", auth_principals="not-json", log_level="INVALID")
    set_environment(monkeypatch, inherited)
    explicit = values("explicit")
    settings = Settings(
        **constructor_values(explicit, spelling), _env_file=dotenv if dotenv_enabled else None
    )
    assert settings.database_url.get_secret_value() == explicit["database_url"]
    assert settings.auth_principals[0].principal_id == "explicit"
    assert settings.auth_principals[0].grants[0].owner_id == "explicit"
    assert settings.log_level == "DEBUG"
    assert explicit["database_url"] not in repr(settings)
    assert explicit["auth_principals"][0]["token"] not in repr(settings)


@pytest.mark.parametrize("spelling", ["field", "alias"])
@pytest.mark.parametrize(
    "field,invalid",
    [
        ("auth_principals", []),
        ("auth_principals", "not-json"),
        ("auth_principals", registry("duplicate") * 2),
        (
            "auth_principals",
            [
                dict(
                    registry("wildcard")[0],
                    grants=[dict(registry("wildcard")[0]["grants"][0], owner_id="*")],
                )
            ],
        ),
        ("database_url", ""),
        ("database_url", "private-invalid-database-marker"),
        ("database_url", "sqlite:///unsupported"),
        ("log_level", "INVALID"),
    ],
)
def test_invalid_explicit_value_cannot_fall_back_to_valid_environment_or_dotenv(
    monkeypatch, tmp_path, spelling, field, invalid
):
    dotenv = tmp_path / ".env"
    write_dotenv(dotenv, values("dotenv"))
    set_environment(monkeypatch, values("ambient"))
    explicit = dict(values("explicit"), **{field: invalid})
    with pytest.raises(ValidationError):
        Settings(**constructor_values(explicit, spelling), _env_file=dotenv)


def test_both_constructor_spellings_use_uppercase_alias(monkeypatch):
    set_environment(monkeypatch, values("ambient"))
    settings = Settings(
        database_url="invalid-field-value",
        auth_principals=[],
        log_level="INVALID",
        **constructor_values(values("alias"), "alias"),
        _env_file=None,
    )
    assert settings.auth_principals[0].principal_id == "alias"
    assert settings.database_url.get_secret_value() == values("alias")["database_url"]
    assert settings.log_level == "DEBUG"


def test_settling_policy_resolution_is_explicit_and_integration_override_wins():
    integration = "00000000-0000-4000-8000-000000000001"
    configured = Settings(
        **values("policy"),
        managed_ai_bindings=[
            dict(
                owner_id="bound",
                integration_id=integration,
                channel="TELEGRAM",
                agent_id="00000000-0000-4000-8000-000000000002",
                service_token="synthetic-managed-service-token-" + "x" * 32,
            ),
        ],
        turn_settling_policies=[
            dict(
                version="telegram-default-v1",
                channel="TELEGRAM",
                quiet_window_ms=500,
                max_wait_ms=3000,
            ),
            dict(
                version="telegram-fast-v1",
                channel="TELEGRAM",
                integration_id=integration,
                quiet_window_ms=100,
                max_wait_ms=900,
            ),
        ],
        _env_file=None,
    )

    assert (
        configured.settling_policy_for(Channel.TELEGRAM, UUID(integration)).version
        == "telegram-fast-v1"
    )
    assert configured.settling_policy_for(Channel.WEBSITE_WIDGET, UUID(integration)) is None
    assert SettlingPolicy(
        version="boundary", channel=Channel.TELEGRAM, quiet_window_ms=1, max_wait_ms=1
    )


def test_invalid_settling_policy_and_duplicate_scope_fail_closed():
    integration = "00000000-0000-4000-8000-000000000001"
    with pytest.raises(ValidationError):
        SettlingPolicy(
            version="invalid", channel=Channel.TELEGRAM, quiet_window_ms=20, max_wait_ms=19
        )

    policy = dict(
        version="telegram-v1",
        channel="TELEGRAM",
        integration_id=integration,
        quiet_window_ms=100,
        max_wait_ms=1000,
    )
    binding = dict(
        owner_id="bound",
        integration_id=integration,
        channel="TELEGRAM",
        agent_id="00000000-0000-4000-8000-000000000002",
        service_token="synthetic-managed-service-token-" + "x" * 32,
    )
    with pytest.raises(ValidationError):
        Settings(
            **values("duplicate-policy"),
            managed_ai_bindings=[binding],
            turn_settling_policies=[policy, dict(policy)],
            _env_file=None,
        )


@pytest.mark.parametrize(
    "field,invalid",
    [("database_url", "not-url"), ("auth_principals", []), ("log_level", "INVALID")],
)
def test_invalid_uppercase_constructor_alias_is_not_replaced_by_valid_field_name(
    monkeypatch, field, invalid
):
    set_environment(monkeypatch, values("ambient"))
    alias_values = dict(values("alias"), **{field: invalid})
    with pytest.raises(ValidationError):
        Settings(**values("field"), **constructor_values(alias_values, "alias"), _env_file=None)


def test_default_environment_outranks_dotenv(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    write_dotenv(tmp_path / ".env", dict(values("dotenv"), log_level="WARNING"))
    set_environment(monkeypatch, dict(values("ambient"), log_level="ERROR"))
    settings = load_settings()
    assert settings.auth_principals[0].principal_id == "ambient"
    assert settings.database_url.get_secret_value() == values("ambient")["database_url"]
    assert settings.log_level == "ERROR"


def test_default_dotenv_and_log_level_default(monkeypatch, tmp_path):
    clear_settings_environment(monkeypatch)
    monkeypatch.chdir(tmp_path)
    data = values("dotenv")
    del data["log_level"]
    write_dotenv(tmp_path / ".env", data)
    settings = load_settings()
    assert settings.auth_principals[0].principal_id == "dotenv"
    assert settings.database_url.get_secret_value() == data["database_url"]
    assert settings.log_level == "INFO"
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize(
    "field,invalid",
    [
        ("auth_principals", []),
        ("auth_principals", "private-invalid-registry-marker"),
        ("database_url", "private-invalid-database-marker"),
        ("log_level", "INVALID"),
    ],
)
def test_invalid_default_environment_fails_closed_without_dotenv_fallback_or_echo(
    monkeypatch, tmp_path, field, invalid
):
    monkeypatch.chdir(tmp_path)
    write_dotenv(tmp_path / ".env", values("dotenv"))
    set_environment(monkeypatch, dict(values("ambient"), **{field: invalid}))
    with pytest.raises(RuntimeError) as captured:
        load_settings()
    assert str(captured.value) == "Invalid conversation-service configuration."
    assert captured.value.__suppress_context__


@pytest.mark.parametrize("spelling", ["field", "alias"])
@pytest.mark.parametrize("ambient", ["valid", "invalid"])
async def test_explicit_app_uses_selected_database_and_only_selected_credential(
    monkeypatch, database_url, spelling, ambient
):
    inherited = values("ambient")
    if ambient == "invalid":
        inherited.update(database_url="not-url", auth_principals="not-json")
    set_environment(monkeypatch, inherited)
    explicit = dict(values("explicit"), database_url=database_url)
    settings = Settings(**constructor_values(explicit, spelling), _env_file=None)
    app = create_app(settings=settings)
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/ready")).status_code == 200
            intended = await client.get(
                "/internal/openapi.json",
                headers={"X-Service-Token": registry("explicit")[0]["token"]},
            )
            assert intended.status_code == 200
            inherited_response = await client.get(
                "/internal/openapi.json",
                headers={"X-Service-Token": registry("ambient")[0]["token"]},
            )
            assert inherited_response.status_code == 401
    assert app.state.settings.auth_principals[0].principal_id == "explicit"
    selected_database = settings.database_url.get_secret_value() == database_url
    assert selected_database


async def test_environment_only_app_startup_remains_supported(monkeypatch, database_url):
    set_environment(monkeypatch, dict(values("ambient"), database_url=database_url))
    app = create_app()
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/ready")).status_code == 200
            response = await client.get(
                "/internal/openapi.json",
                headers={"X-Service-Token": registry("ambient")[0]["token"]},
            )
            assert response.status_code == 200


@pytest.mark.parametrize("source", ["dotenv", "environment", "empty", "invalid", "interpolation"])
def test_native_migration_database_source_priority_and_no_auth_requirement(
    tmp_path, database_url, source
):
    env = os.environ.copy()
    env.pop("DATABASE_URL", None)
    env["PYTHONPATH"] = str(ROOT)
    dotenv_database = database_url
    if source == "environment":
        env["DATABASE_URL"] = database_url
        dotenv_database = "private-invalid-dotenv-database-marker"
    elif source in {"empty", "invalid"}:
        env["DATABASE_URL"] = "" if source == "empty" else "private-invalid-env-database-marker"
    elif source == "interpolation":
        env["MIGRATION_TEST_DATABASE_URL"] = database_url
        dotenv_database = "${MIGRATION_TEST_DATABASE_URL}"
    (tmp_path / ".env").write_text(
        f"DATABASE_URL={dotenv_database}\n"
        "CONVERSATION_AUTH_PRINCIPALS=private-invalid-registry-marker\n"
        "LOG_LEVEL=INVALID\n",
        encoding="utf-8",
    )
    # Run the documented native command without exporting credentials or requiring auth.
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ROOT / "alembic.ini"), "upgrade", "head"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if source in {"dotenv", "environment"}:
        assert result.returncode == 0
        # A second native invocation verifies dotenv reads do not mutate the process environment.
        smoke = subprocess.run(
            [
                sys.executable,
                "-c",
                "import os; from alembic.config import Config; from alembic import command; "
                "before = os.environ.copy(); "
                f"command.current(Config({str(ROOT / 'alembic.ini')!r})); "
                "assert os.environ == before",
            ],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert smoke.returncode == 0
        assert "0013_writer_retirement" in smoke.stdout
    else:
        assert result.returncode != 0
        assert "DATABASE_URL must be a dedicated PostgreSQL asyncpg URL" in result.stderr
        for marker in [
            "private-invalid-registry-marker",
            "private-invalid-env-database-marker",
            "MIGRATION_TEST_DATABASE_URL",
            database_url,
        ]:
            assert marker not in result.stderr

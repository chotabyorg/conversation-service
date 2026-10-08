"""Test-only process boundary and post-commit crash barriers.

No production fault hooks. Imports the selected repository's actual entry points.
Every outbound connection is limited to the explicitly allocated loopback ports.
"""

import asyncio
import json
import os
import socket
from pathlib import Path
from uuid import uuid4


def install_guard(config):
    endpoints = {(host, int(port)) for host, port in config["allowlist"]}
    if not endpoints or any(host != "127.0.0.1" for host, _ in endpoints):
        raise RuntimeError("Invalid explicit Website runtime allowlist")
    connect = socket.socket.connect
    connect_ex = socket.socket.connect_ex
    resolve = socket.getaddrinfo

    def check(address):
        if tuple(address[:2]) not in endpoints:
            raise RuntimeError("Website fixture network destination forbidden")

    def guarded_connect(sock, address):
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            check(address)
        elif sock.family == socket.AF_UNIX and address:
            raise RuntimeError("Website fixture named Unix socket forbidden")
        return connect(sock, address)

    def guarded_connect_ex(sock, address):
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            check(address)
        return connect_ex(sock, address)

    def guarded_resolve(host, port, *args, **kwargs):
        check((host, int(port)))
        return resolve(host, port, *args, **kwargs)

    socket.socket.connect = guarded_connect
    socket.socket.connect_ex = guarded_connect_ex
    socket.getaddrinfo = guarded_resolve
    denials = 0
    for method in ("connect", "connect_ex"):
        with socket.socket() as probe:
            try:
                getattr(probe, method)(("203.0.113.1", 443))
            except RuntimeError:
                denials += 1
            else:
                raise RuntimeError("Fixture network guard self-check failed")
    Path(config["state_dir"], config["name"] + "-guard.json").write_text(
        json.dumps({"denials": denials})
    )


def patch_barrier(module, method, config, *, before=False):
    original = getattr(module, method)

    async def wrapped(*args, **kwargs):
        result = None
        if not before:
            result = await original(*args, **kwargs)
        Path(config["state_dir"], "barrier.json").write_text(json.dumps({
            "point": config["fault"], "process": config["name"], "pid": os.getpid(),
        }))
        await asyncio.Event().wait()
        return await original(*args, **kwargs) if before else result

    setattr(module, method, wrapped)


def fixture_app(config):
    from fastapi import FastAPI, Request

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    state_path = Path(config["state_dir"], "observations.json")

    def record(kind, value):
        state = json.loads(state_path.read_text())
        state[kind].append(value)
        state_path.write_text(json.dumps(state))

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/v1/internal/integrations")
    async def integrations():
        return [dict(value, credentials={"widget_api_key": key, "widget_config": {}})
                for key, value in config["widgets"].items()]

    @app.get("/v1/widget/internal/validate")
    async def validate(api_key: str):
        from fastapi.responses import JSONResponse
        item = config["widgets"].get(api_key)
        if item is None:
            return JSONResponse(status_code=404, content={"detail": "not_found"})
        return dict(item, config={})

    @app.post("/v1/chat/completions")
    async def legacy(request: Request):
        value = await request.json()
        record("legacy", {"session_id": value["external_client_id"],
                          "agent_id": value["agent_id"]})
        return {"answer": "Legacy fixture reply"}

    @app.post("/internal/v1/managed-turns/execute")
    async def execute(request: Request):
        value = await request.json()
        # One fake effect is deliberately recorded per HTTP execution, without
        # fake idempotency hiding duplicate calls from the actual coordinator.
        operation_id = str(uuid4())
        record("managed", {key: value[key] for key in (
            "turn_id", "conversation_id", "input_first_sequence", "input_last_sequence",
            "context_last_sequence", "selected_input_message_ids",
        )})
        record("effects", {"turn_id": value["turn_id"], "operation_id": operation_id})
        await asyncio.sleep(0.15)
        echo = {key: value[key] for key in (
            "contract_version", "conversation_id", "turn_id", "agent_id", "input_revision",
            "authority_revision", "input_first_sequence", "input_last_sequence",
            "context_last_sequence",
        )}
        return dict(echo, status="DRAFT_READY", draft_text="Managed fixture reply",
                    error_code=None, operation_refs=[{
                        "origin": "AI_AGENT_SERVICE", "operation_id": operation_id,
                        "outcome": "SUCCEEDED",
                    }])

    @app.post("/internal/v1/managed-turns/evidence")
    async def evidence(request: Request):
        value = await request.json()
        record("evidence", {"turn_id": value["turn_id"]})
        state = json.loads(state_path.read_text())
        return {"contract_version": 1, "conversation_id": value["conversation_id"],
                "turn_id": value["turn_id"], "agent_id": value["agent_id"],
                "operation_refs": [{"origin": "AI_AGENT_SERVICE",
                                    "operation_id": row["operation_id"],
                                    "outcome": "SUCCEEDED"}
                                   for row in state["effects"]
                                   if row["turn_id"] == value["turn_id"]]}

    return app


def run():
    config = json.loads(Path(os.environ["GOAL4A_BOOTSTRAP_CONFIG"]).read_text())
    if (Path.cwd() / ".env").exists():
        raise RuntimeError("Website fixtures forbid candidate dotenv")
    install_guard(config)
    name, fault = config["name"], config.get("fault")
    if name.startswith("website"):
        from services.conversation_client import ConversationClient
        if fault == "F":
            patch_barrier(ConversationClient, "begin_response", config, before=True)
        elif fault == "H":
            patch_barrier(ConversationClient, "complete_response", config, before=True)
        import main
        if Path(main.__file__).resolve().parent != Path.cwd().resolve():
            raise RuntimeError("Wrong Website candidate imported")
        asyncio.run(main.main())
        return

    import uvicorn
    if name == "fake":
        app = fixture_app(config)
    else:
        from core.config import Settings
        if name == "cs":
            from services import website
            if fault == "A":
                patch_barrier(website, "admit", config)
            elif fault == "G":
                patch_barrier(website, "begin_response", config)
            from main import create_app
            app = create_app(Settings(_env_file=None))
        else:
            from services import managed_execution, turns, website_worker
            from services.managed_client import ManagedAIClient
            if fault == "B":
                # Only halt a positive selection, not the initial empty queue.
                original = website_worker.select_immediate

                async def positive(*args, **kwargs):
                    result = await original(*args, **kwargs)
                    if result is not None:
                        Path(config["state_dir"], "barrier.json").write_text(json.dumps({
                            "point": fault, "process": name, "pid": os.getpid(),
                        }))
                        await asyncio.Event().wait()
                    return result
                website_worker.select_immediate = positive
            elif fault == "C":
                patch_barrier(turns, "claim_turn", config)
            elif fault == "D":
                patch_barrier(managed_execution, "mark_dispatch", config)
            elif fault == "E":
                patch_barrier(ManagedAIClient, "execute", config)
            from worker import create_worker_app
            app = create_worker_app(Settings(_env_file=None))
    uvicorn.run(app, host="127.0.0.1", port=config["port"], access_log=False,
                log_config=None, timeout_graceful_shutdown=2)


if __name__ == "__main__":
    run()

"""Actual loopback AudioSocket -> Voice -> HTTP Conversation -> HTTP managed fake."""

import asyncio
import os
import socket
import struct
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import uvicorn
from fastapi import FastAPI, Request
from pydantic import SecretStr
from sqlalchemy import select

from db.database import Database
from main import create_app
from models import VoiceCall, VoicePlaybackEvent
from schemas.managed import ManagedTurnRequest, ManagedTurnResult
from services.managed_client import ManagedAIClient
from services.voice_worker import VoiceWorker
from tests.test_voice import AGENT, INTEGRATION, OWNER
from tests.test_voice import vh as vh_fixture

vh = vh_fixture


def listener(allowlist):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    sock.setblocking(False)
    allowlist.add(sock.getsockname())
    return sock


async def start_server(app, sock):
    server = uvicorn.Server(uvicorn.Config(app, log_config=None, access_log=False))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    async with asyncio.timeout(5):
        while not server.started:
            if task.done():
                task.result()
            await asyncio.sleep(0.01)
    return server, task


def packet(kind, value):
    return struct.pack("!BH", kind, len(value)) + value


async def feed(writer, amplitude, milliseconds):
    audio = struct.pack("<h", amplitude) * (milliseconds * 8)
    for offset in range(0, len(audio), 1600):
        writer.write(packet(0x10, audio[offset:offset + 1600]))
        await writer.drain()


@pytest.mark.parametrize("scenario", ["complete", "barge_in", "failed_turn"])
async def test_audio_through_http_conversation_and_managed_boundary(
    vh, network_allowlist, scenario,
):
    barge_in = scenario == "barge_in"
    source = os.environ.get("VOICE_CANDIDATE_ROOT")
    if not source or not (Path(source) / "voice_gateway/conversation_http.py").is_file():
        pytest.fail("VOICE_CANDIDATE_ROOT must select the reviewed Voice candidate")
    sys.path.insert(0, source)
    from voice_gateway.application import LocalFakeSTT, VoiceGatewayRuntime
    from voice_gateway.conversation_http import ConversationHTTPPort, TrustedVoiceBinding
    from voice_gateway.media.interfaces import BaseTTS
    from voice_gateway.settings import Settings as VoiceSettings

    managed_requests = []
    fake_ai = FastAPI()
    ai_token = vh["settings"].managed_ai_bindings[0].service_token.get_secret_value()

    @fake_ai.post("/internal/v1/managed-turns/execute")
    async def execute(request: Request):
        assert request.headers["X-Service-Token"] == ai_token
        value = ManagedTurnRequest.model_validate(await request.json())
        assert value.channel.value == "VOICE" and value.agent_id == AGENT
        assert value.contract_version == 2
        assert value.stream_partials is True and value.max_partial_chars == 2000
        managed_requests.append(value)
        failed = scenario == "failed_turn" and len(managed_requests) == 1
        return ManagedTurnResult(
            contract_version=value.contract_version, conversation_id=value.conversation_id,
            turn_id=value.turn_id, agent_id=value.agent_id, input_revision=value.input_revision,
            authority_revision=value.authority_revision,
            input_first_sequence=value.input_first_sequence,
            input_last_sequence=value.input_last_sequence,
            context_last_sequence=value.context_last_sequence,
            status="UNKNOWN" if failed else "DRAFT_READY",
            draft_text=None if failed else "Локальный ответ",
            error_code="TOOL_EXECUTION_UNKNOWN" if failed else None, operation_refs=[],
        )

    ai_socket, conversation_socket = listener(network_allowlist), listener(network_allowlist)
    settings = vh["settings"].model_copy(update={
        "managed_ai_base_url": f"http://127.0.0.1:{ai_socket.getsockname()[1]}"})
    database = Database(settings.database_url.get_secret_value())
    worker = VoiceWorker(database, settings, ManagedAIClient(settings))
    servers, tasks = [], []
    runtime = port = writer = None
    worker_task = None
    try:
        for app, sock in ((fake_ai, ai_socket), (create_app(settings), conversation_socket)):
            server, task = await start_server(app, sock)
            servers.append(server)
            tasks.append(task)
        worker_task = asyncio.create_task(worker.run())

        async def trusted_binding(call_id):
            return TrustedVoiceBinding(OWNER, INTEGRATION, AGENT)

        class ToneTTS(BaseTTS):
            async def synthesize(self, text):
                assert text == "Локальный ответ" or (
                    scenario == "failed_turn" and "Я на линии" in text
                )
                return struct.pack("<h", 1000) * (16000 if barge_in else 800)

            async def close(self):
                pass

        token = settings.auth_principals[0].token
        port = ConversationHTTPPort(
            f"http://127.0.0.1:{conversation_socket.getsockname()[1]}",
            SecretStr(token.get_secret_value()), trusted_binding,
        )
        runtime = VoiceGatewayRuntime(VoiceSettings(mode="local_fake", audiosocket_port=0),
                                      conversation_port=port, stt_factory=LocalFakeSTT,
                                      tts_factory=ToneTTS)
        await runtime.start()
        network_allowlist.add(("127.0.0.1", runtime.bound_port))
        reader, writer = await asyncio.open_connection("127.0.0.1", runtime.bound_port)
        call_id = uuid4()
        writer.write(packet(1, call_id.bytes))
        await writer.drain()
        await feed(writer, 1500, 300)
        await feed(writer, 0, 1000)
        async with asyncio.timeout(8):
            kind, length = struct.unpack("!BH", await reader.readexactly(3))
            assert kind == 0x10 and length > 0
            assert await reader.readexactly(length)
        if barge_in:
            await asyncio.sleep(0.15)
            await feed(writer, 4000, 250)
            await feed(writer, 0, 1000)
        expected = "INTERRUPTED" if barge_in else "COMPLETED"
        async with asyncio.timeout(8):
            while True:
                async with database.session_factory() as session:
                    fact = await session.scalar(select(VoicePlaybackEvent).where(
                        VoicePlaybackEvent.call_id == call_id,
                        VoicePlaybackEvent.state == expected))
                if fact is not None:
                    break
                await asyncio.sleep(0.02)
        assert managed_requests and fact.accepted_bytes > 0
        if barge_in:
            assert fact.accepted_bytes < fact.generated_bytes
        if scenario == "failed_turn":
            # The actual HTTP adapter and AudioSocket remain usable after the
            # failed turn's authorized fallback, without re-executing that turn.
            async with database.session_factory() as session:
                active = await session.get(VoiceCall, call_id)
                assert active is not None and active.ended_at is None
            await feed(writer, 1500, 300)
            await feed(writer, 0, 1000)
            async with asyncio.timeout(8):
                while len(managed_requests) < 2:
                    await asyncio.sleep(0.02)
            assert managed_requests[0].turn_id != managed_requests[1].turn_id
        writer.close()
        await writer.wait_closed()
        async with asyncio.timeout(5):
            while True:
                async with database.session_factory() as session:
                    call = await session.get(VoiceCall, call_id)
                if call is not None and call.ended_at is not None:
                    break
                await asyncio.sleep(0.02)
        assert not port.sessions
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        if runtime is not None:
            await runtime.stop()
        if port is not None:
            await port.close()
        worker.request_stop()
        if worker_task is not None:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
        for server in servers:
            server.should_exit = True
        await asyncio.gather(*tasks, return_exceptions=True)
        ai_socket.close()
        conversation_socket.close()
        await database.dispose()

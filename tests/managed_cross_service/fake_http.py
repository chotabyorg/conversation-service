"""Bounded, scripted local model/KB servers. Captures are immutable snapshots."""

import copy
import json
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def completion(text="Fixture draft", calls=None):
    message = {"role": "assistant", "content": text, "tool_calls": calls}
    return {
        "id": "fixture-completion",
        "object": "chat.completion",
        "created": 1,
        "model": "fixture-model",
        "choices": [
            {"index": 0, "finish_reason": "tool_calls" if calls else "stop", "message": message}
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def stream_completion(*text_deltas, completion_id="fixture-stream-completion"):
    """OpenAI-compatible SSE chunks for the real SDK streaming path."""
    chunks = [
        {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "fixture-model",
            "choices": [{"index": 0, "delta": {"content": part}, "finish_reason": None}],
        }
        for part in text_deltas
    ]
    chunks.append({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "fixture-model",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    })
    return {"_sse_chunks": chunks}


def call(name, arguments="{}", identifier="fixture-tool"):
    return {
        "id": identifier,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


def descriptor(name, description="Synthetic tool"):
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": {}},
        },
    }


class ScriptedServer:
    def __init__(self, listener, kind):
        self.kind = kind
        self.scripts = deque()
        self.calls = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.errors = []
        controller = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.settimeout(3)

            def log_message(self, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 2097152:
                    controller.errors.append("request-bound")
                    self.send_error(400)
                    return
                value = json.loads(self.rfile.read(length))
                with controller.lock:
                    controller.calls.append(copy.deepcopy(value))
                    script = controller.scripts.popleft() if controller.scripts else None
                if script is None:
                    controller.errors.append("missing-script")
                    script = {"status": 500, "body": {"error": {"message": "No fixture script"}}}
                if script.get("barrier"):
                    controller.entered.set()
                    if not controller.release.wait(25):
                        controller.errors.append("barrier-deadline")
                chunks = script.get("body", {}).get("_sse_chunks")
                if chunks is None:
                    body = json.dumps(script.get("body", {}), ensure_ascii=False).encode()
                    content_type = "application/json"
                else:
                    body = b"".join(
                        b"data: " + json.dumps(chunk, ensure_ascii=False).encode() + b"\n\n"
                        for chunk in chunks
                    ) + b"data: [DONE]\n\n"
                    content_type = "text/event-stream"
                try:
                    self.send_response(script.get("status", 200))
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # Expected when real managed client cancels a held response.

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler, bind_and_activate=False)
        self.server.socket.close()
        self.server.socket = listener
        self.server.server_address = listener.getsockname()
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def queue(self, *bodies, barrier=False, status=200):
        self.scripts.extend({"body": body, "barrier": barrier, "status": status} for body in bodies)

    def queue_stream(self, *text_deltas, barrier=False, status=200):
        self.queue(stream_completion(*text_deltas), barrier=barrier, status=status)

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.thread.join(timeout=3)
        self.server.server_close()
        assert not self.thread.is_alive(), "Owned fake HTTP listener did not stop"

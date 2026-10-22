"""Prepare ephemeral TLS and exact clean Gateway inputs for the opt-in HTTP suite."""
import argparse
import json
import os
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-root", type=Path, required=True)
    parser.add_argument("--gateway-python", type=Path, required=True)
    parser.add_argument("--expected-gateway-revision", required=True)
    parser.add_argument("--network-guard", type=Path, required=True)
    parser.add_argument("--openssl", default="openssl")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root, output = args.gateway_root.resolve(), args.output.resolve()
    revision = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"],
                                      text=True).strip()
    assert revision == args.expected_gateway_revision
    assert not subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"],
                                       text=True).strip()
    assert not (root / ".env").exists()
    output.mkdir(parents=True, exist_ok=False)
    certificate, key = output / "ca.pem", output / "private-key.pem"
    subprocess.run([args.openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=CHOTA disposable local test", "-addext", "subjectAltName=IP:127.0.0.1",
        "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
        "-keyout", str(key), "-out", str(certificate)], check=True, capture_output=True)
    key.chmod(0o600)
    config = dict(gateway_source=str(root), gateway_revision=revision,
        # Preserve a POSIX venv launcher symlink: resolving it selects the base
        # interpreter and silently loses the separately installed dependencies.
        gateway_python=os.path.abspath(args.gateway_python),
        network_guard=str(args.network_guard.resolve()),
        ca_file=str(certificate), key_file=str(key))
    (output / "input.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    print(json.dumps(dict(status="EPHEMERAL_LOCAL_TLS_READY", gateway_revision=revision)))


if __name__ == "__main__":
    main()

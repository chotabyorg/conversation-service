"""Historical managed request fingerprints remain deterministic after upgrades."""

import json
from uuid import UUID

import pytest

from schemas.managed import ManagedTurnRequest, prepare_request
from schemas.contracts import Channel


# These are frozen historical vectors generated with the pre-stream and
# post-stream canonical schemas. Keep them literal so production code cannot
# accidentally update both the expected result and the implementation.
V1_PRE_STREAM = "c7017aa0127aa90dda88ddefda0f181df31660f2dc5eb9051376e7670b97de60"
V1_EXT_FALSE_2000 = "766519d58a086eab847c35989ee882545519c5781b93604f97236a8da970e673"
V2_PRE_STREAM = "41c55a570cec4aae5363698cc910540f9ef30c2d332a26f7d929f8de895aa765"
V2_EXT_FALSE_2000 = "c6f702928a2df1cb68b637bd737331cf25edcb93a729ca3d86e7ff4219648dc9"
V2_EXT_TRUE_2000 = "f72f51ec5537ab41fc71e42d3f13e6bcb817af5ec231d8f4d312a9b1b271c436"
VOICE_V2_PRE_STREAM = "56ed8a9282543e8fec3660996433952123b3fd81c355d289614b5e32c5e1b01b"
VOICE_V2_EXT_FALSE_2000 = "e85966086c9e64377f88314e53852863b72ccffb2073336f6e66531dbeb16f8b"


def historical_request(version=1, *, channel="WEBSITE_WIDGET", stream=False,
                       max_chars=2000):
    if version == 1:
        conversation, turn, integration, agent, first_id, second_id = range(1, 7)
        owner, input_revision, authority_revision = "synthetic-owner", 7, 2
        messages = [
            (first_id, 1, "CUSTOMER", "INBOUND", "synthetic customer message"),
            (second_id, 2, "AGENT", "OUTBOUND", "synthetic prior response"),
        ]
        first, last, bound = 1, 1, 2
    else:
        conversation, turn, integration, agent, first_id, second_id = range(11, 17)
        owner, input_revision, authority_revision = "synthetic-owner-v2", 9, 1
        messages = [
            (first_id, 10, "AGENT", "OUTBOUND", "synthetic prior response"),
            (second_id, 11, "CUSTOMER", "INBOUND", "synthetic customer message"),
        ]
        first, last, bound = 11, 11, 11
    value = dict(
        contract_version=version,
        conversation_id=str(UUID(int=conversation)),
        turn_id=str(UUID(int=turn)),
        owner_id=owner,
        integration_id=str(UUID(int=integration)),
        agent_id=str(UUID(int=agent)),
        channel=channel,
        input_revision=input_revision,
        authority_revision=authority_revision,
        input_first_sequence=first,
        input_last_sequence=last,
        context_last_sequence=bound,
        messages=[dict(id=str(UUID(int=mid)), sequence=seq, actor=actor,
                       direction=direction, content_type="TEXT", text=text)
                  for mid, seq, actor, direction, text in messages],
        selected_input_message_ids=[str(UUID(int=second_id if version == 2 else first_id))],
        stream_partials=stream,
        max_partial_chars=max_chars,
    )
    return ManagedTurnRequest.model_validate(value)


@pytest.mark.parametrize(
    ("managed_request", "expected"),
    [
        (historical_request(1), V1_PRE_STREAM),
        (historical_request(1), V1_EXT_FALSE_2000),
        (historical_request(2), V2_PRE_STREAM),
        (historical_request(2), V2_EXT_FALSE_2000),
        # The reconstructed Voice v2 policy is true today. Historical pre-stream
        # and extension-false rows must still select their exact old body and
        # return a request object whose runtime behavior is false.
        (historical_request(2, channel="VOICE", stream=True), VOICE_V2_PRE_STREAM),
        (historical_request(2, channel="VOICE", stream=True), VOICE_V2_EXT_FALSE_2000),
        (historical_request(2, channel="VOICE", stream=True), V2_EXT_TRUE_2000),
    ],
)
def test_retained_projection_chooses_only_the_exact_historical_wire_profile(managed_request, expected):
    from schemas.managed import prepare_request_for_fingerprint

    prepared = prepare_request_for_fingerprint(managed_request, expected)
    body = json.loads(prepared.body)
    assert prepared.fingerprint == expected
    if expected in {V1_PRE_STREAM, V2_PRE_STREAM, VOICE_V2_PRE_STREAM}:
        assert "stream_partials" not in body and "max_partial_chars" not in body
        assert prepared.request.stream_partials is False
        assert prepared.request.max_partial_chars == 2000
    else:
        assert body["stream_partials"] is prepared.request.stream_partials
        assert body["max_partial_chars"] == 2000
        assert prepared.request.max_partial_chars == 2000


def test_projection_digest_must_match_one_profile_and_custom_stream_limit_stays_significant():
    from schemas.managed import prepare_request_for_fingerprint

    request = historical_request(2)
    with pytest.raises(ValueError):
        prepare_request_for_fingerprint(request, "0" * 64)

    custom = historical_request(2, channel="VOICE", stream=True, max_chars=501)
    exact = prepare_request(custom)
    assert json.loads(exact.body)["max_partial_chars"] == 501
    with pytest.raises(ValueError):
        prepare_request_for_fingerprint(custom, V2_PRE_STREAM)


def test_voice_stream_profile_is_only_valid_for_voice_v2():
    with pytest.raises(ValueError):
        historical_request(2, channel="TELEGRAM", stream=True)
    voice = historical_request(2, channel=Channel.VOICE.value, stream=True)
    assert voice.stream_partials is True


def test_voice_historical_profile_survives_restart_and_rejects_changed_core():
    from schemas.managed import prepare_request_for_fingerprint

    original = historical_request(2, channel=Channel.VOICE.value, stream=True)
    for expected in (VOICE_V2_PRE_STREAM, VOICE_V2_EXT_FALSE_2000):
        selected = prepare_request_for_fingerprint(original, expected)
        reloaded = ManagedTurnRequest.model_validate_json(original.model_dump_json())
        replayed = prepare_request_for_fingerprint(reloaded, expected)
        assert replayed.fingerprint == selected.fingerprint == expected
        assert replayed.body == selected.body
        assert replayed.request.stream_partials is False
        body = json.loads(replayed.body)
        if expected == VOICE_V2_EXT_FALSE_2000:
            assert body["stream_partials"] is False
        else:
            assert "stream_partials" not in body

        changed = reloaded.model_dump(mode="json")
        changed["messages"][-1]["text"] += " changed"
        with pytest.raises(ValueError):
            prepare_request_for_fingerprint(ManagedTurnRequest.model_validate(changed), expected)

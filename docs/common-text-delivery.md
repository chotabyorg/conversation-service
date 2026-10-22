# One ordered text delivery journal

Telegram's existing intent/source validation, authority and input freshness,
catch-up checks, ordered predecessor barrier, one-shot writer acquisition and
original completion evidence now live in `services/text_delivery.py`. Delivery
reconciliation remains the same immutable ledger and fingerprint in
`services/delivery_ledger.py`. Telegram keeps its account checks, request DTOs,
UUID namespaces, fingerprints and replay precedence in the channel adapter.

The shared functions run under the caller's scope → root → intent locks. They do
not authorize a network write from discovery alone, use lease expiry as writer
termination, or convert UNKNOWN into resend permission. Original supplier/process
identity is required for completion; positive task join ends that writer while
preserving its original outcome. Trusted retirement remains a separate positive
proof on the same DeliveryAttempt.

Website native tests use the real credential/selection/AI-v3/Handoff path, owner
claim and HUMAN-send commands, then the same common delivery functions. They
verify single acquisition, unique canonical AGENT output, HUMAN FIFO, UNKNOWN
barriers, positive join, Return to AI and wrong-process rejection. They seed no
parallel Website operation ledger or Handoff manager.

No public Website transport or enrollment is enabled by this extraction. The
credential adapter, serialized response bounds, browser frame identity/history,
original recovery and actual gateway task-join evidence still need their own
channel and HTTP checks before rollout.

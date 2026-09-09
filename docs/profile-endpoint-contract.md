# Telegram MCP endpoint and profile contract

Production runs one Telegram account per MCP endpoint:

| Endpoint/service | Port on `sandbox-ik` | Stable profile identity |
|---|---:|---|
| `telegram-mcp-ikrasinsky` | 8766 | `ikrasinsky` (`ik` is an alias) |
| `telegram-mcp-lisa` | 8767 | `lisa` |

Tool calls should omit `profile` or send `profile="current"`. `current` means
the stable identity assigned to the endpoint that received the call. The old
value `default` remains accepted only as a backward-compatible alias for
`current`; it never means “IK regardless of endpoint”.

`default-lisa` is deliberately not a profile name. It combines a contextual
alias (`default`) with a stable identity (`lisa`) and would create two names for
the same account. Operators and MCP clients must instead select the explicitly
named `telegram-mcp-lisa` endpoint; inside that endpoint, `current` resolves to
`lisa`.

Profile isolation is unconditional: the process constructs exactly one
Telegram client from its endpoint credential and has no cross-profile opt-out.
An explicit identity that does not match the selected endpoint fails closed
with the name of the endpoint to use. This prevents a second process from
opening another endpoint's StringSession and triggering Telegram
`AuthKeyDuplicatedError`. Every successful send/reply also returns `Sent as`,
so the effective Telegram identity is visible in the tool readback.

Examples:

```text
telegram-mcp-lisa.send_message(..., profile="current")      -> sends as lisa
telegram-mcp-lisa.send_message(..., profile="default")      -> sends as lisa (legacy)
telegram-mcp-lisa.send_message(..., profile="ikrasinsky")   -> error; use telegram-mcp-ikrasinsky
telegram-mcp-ikrasinsky.send_message(..., profile="current") -> sends as ikrasinsky
```

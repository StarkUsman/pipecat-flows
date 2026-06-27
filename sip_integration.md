# SIP trunk + FreeSWITCH integration for Pipecat agents

Lets real phone calls reach your Pipecat agents. Inbound PSTN call → **Telnyx SIP trunk**
→ **FreeSWITCH** (Docker, this VM) → **Pipecat SIP gateway** (WebSocket) → the *same*
STT→LLM→TTS + FlowManager pipeline the browser/WebRTC client uses. **No conversation
logic changes** — this is purely a new transport edge.

```
PSTN caller
  → Telnyx SIP trunk (inbound INVITE → 84.46.251.98:5080/udp)
    → FreeSWITCH (Docker, host net, mod_audio_stream)
        dialplan: DID → agent_id → audio_stream start ws://127.0.0.1:8088/ws/<agent_id> mono 8k
          → Pipecat SIP gateway (sip_gateway.py, FastAPI/uvicorn :8088)
              ModAudioStreamSerializer (raw L16 in ↔ base64 "streamAudio" JSON out)
              → session.run_voice_session(): STT → LLM → TTS + FlowManager(agent's flow.py)
          ← TTS audio → base64 JSON → mod_audio_stream → played to caller
```

## What was added (Pipecat side — done & verified)

| File | Purpose |
|------|---------|
| `pipecat-flows/transports/mod_audio_stream_serializer.py` | Serializer for FreeSWITCH `mod_audio_stream` (raw L16 ↔ `streamAudio` JSON). |
| `pipecat-flows/session.py` | `run_voice_session()` + `StatsCollector`, extracted from `bot.py` so the WebRTC bot and the SIP gateway share one pipeline/stats path. |
| `pipecat-flows/sip_gateway.py` | FastAPI/uvicorn gateway. `GET /health`, `WS /ws/{agent_id}` → builds a `FastAPIWebsocketTransport` and runs the agent's flow. Serves all agents from one process. |
| `pipecat-flows/bot.py` | Now delegates to `session.run_voice_session` (WebRTC behaviour unchanged). |
| `pipecat-flows/services.py` | (unchanged signatures) services are built from each agent's `config.json` via `env_overrides` in `session._build_services`, race-safe across concurrent calls. |
| `pipecat-flows/scripts/ws_test_client.py` | Simulates `mod_audio_stream` to test the gateway with no telephony. |

Routing model: **DID → specific agent_id**, defined in the FreeSWITCH dialplan. The gateway
is generic and serves whatever `agent_id` is in the WS path. Find agent ids in
`pipecat-flows/agents/registry.json` or `GET http://127.0.0.1:8088/health`.

---

## Part 1 — Run the Pipecat SIP gateway

```bash
cd pipecat-flows
uv sync                       # installs fastapi + uvicorn + websocket extra (already in pyproject)
uv run uvicorn sip_gateway:app --host 127.0.0.1 --port 8088
# health check:
curl http://127.0.0.1:8088/health
```

Bind to `127.0.0.1` (not `0.0.0.0`): FreeSWITCH runs on the same host and connects over the
loopback, so the WS hop needs no TLS and is never exposed publicly.

**Verify it works with NO telephony** (Phase D-1):

```bash
cd pipecat-flows
uv run python scripts/ws_test_client.py ws://127.0.0.1:8088/ws/<agent_id>
# -> "~N.NNs of bot audio received -> end-to-end pipeline WORKS"
```

*(Already run during implementation against agent `c703612b` — the bot greeted and ~3.6 s of
Cartesia TTS came back as `streamAudio`, and a session row was written to `agent_stats`.)*

Run it under a process manager for production, e.g. systemd or:
`uv run uvicorn sip_gateway:app --host 127.0.0.1 --port 8088 --workers 1`
(one worker; each call is async — multiple concurrent calls are fine in one worker).

---

## Part 2 — Telnyx SIP trunk (free trial), step by step

1. Sign up at **https://telnyx.com** (Mission Control Portal). Verify email + phone — the
   trial includes credit for testing.
2. **Numbers → Buy a Number** → pick a local DID. Note the number.
3. **Voice → SIP Connections → Create** → authentication type **IP**:
   - Add authorized IP **84.46.251.98**, port **5080**, transport **UDP**.
   - (5080 = FreeSWITCH's *external* SIP profile, which handles unauthenticated trunk calls.)
4. **Numbers → My Numbers** → assign the DID to that SIP Connection.
5. Inbound-only agents need no outbound profile/registration. (For outbound later: add a
   Telnyx outbound voice profile + a FreeSWITCH gateway with credentials.)
6. **Copy Telnyx's inbound signaling/media IP ranges** (Portal docs) — used for the firewall below.
7. Map the DID → desired `agent_id` in `freeswitch/conf/dialplan/public/01_pipecat_inbound.xml`.

---

## Part 3 — FreeSWITCH (Docker)

FreeSWITCH 1.10 + `mod_audio_stream`. Building the module needs FreeSWITCH dev headers from
SignalWire's repo, which needs a **free** SignalWire token.

1. Get the token: sign up at **https://signalwire.com** → Dashboard → **Personal Access Token**.
2. Build + run (host networking; the VM's public IP is on eth0 with no NAT, so FreeSWITCH
   `auto` IP detection resolves to 84.46.251.98):

```bash
cd freeswitch
export SIGNALWIRE_TOKEN=pat_xxxxxxxx
docker compose build        # builds mod_audio_stream, bakes our dialplan
docker compose up -d
docker exec -it fs fs_cli    # console; check:  sofia status  /  show application audio_stream
```

3. Confirm the module + profiles:
```bash
docker exec fs fs_cli -x "module_exists mod_audio_stream"   # -> true
docker exec fs fs_cli -x "sofia status"                      # external profile RUNNING on 5080
```

If `auto` ever picks the wrong IP, set the external profile explicitly: in
`/etc/freeswitch/vars.xml` set `external_sip_ip` / `external_rtp_ip` to `84.46.251.98`,
then `fs_cli -x "sofia profile external restart"`.

### Firewall (host)

Open only what's needed, restricted to Telnyx's IP ranges from Part 2 step 6:

```bash
# SIP (external profile) + RTP media, from Telnyx only:
sudo ufw allow from <TELNYX_IP_RANGE> to any port 5080 proto udp
sudo ufw allow from <TELNYX_IP_RANGE> to any port 16384:32768 proto udp
# Do NOT expose 8088 (gateway) or 5060 (internal) publicly.
```

---

## Part 4 — Verification (incremental)

1. **Gateway only (no telephony):** `scripts/ws_test_client.py` → bot audio returns. ✅ (done)
2. **Softphone → FreeSWITCH → agent (no trunk):** register Zoiper/Linphone to this FreeSWITCH
   (internal profile :5060, user `1000`, password `$${default_password}` from `vars.xml` —
   `docker exec fs fs_cli -x "eval \${default_password}"`). Dial **9000** → converse with the
   agent. Proves the FreeSWITCH↔gateway audio path + turn-taking.
3. **Real PSTN call:** call your Telnyx DID from a mobile → FreeSWITCH routes by DID → the
   mapped agent answers. Verify two-way audio and that a row appears in `agent_stats`.
4. **Multi-agent routing:** add a second DID→agent block, call each, confirm each reaches its
   own flow/voice.
5. Tune: `SIP_VAD_STOP_SECS`, and try `SIP_PIPELINE_SAMPLE_RATE=16000` (default) vs 8000.

### Gateway env knobs (sip_gateway.py)
| Var | Default | Meaning |
|-----|---------|---------|
| `SIP_GATEWAY_HOST` / `SIP_GATEWAY_PORT` | `0.0.0.0` / `8088` | bind address (use 127.0.0.1 in prod) |
| `SIP_PIPELINE_SAMPLE_RATE` | `16000` | internal pipeline rate (STT prefers 16k) |
| `SIP_STREAM_SAMPLE_RATE` | `8000` | rate negotiated with FreeSWITCH; must match the dialplan `8k` |
| `SIP_VAD_STOP_SECS` | `0.2` | silence that ends a user turn |

---

## Known limitations / notes

- **Barge-in:** `mod_audio_stream` documents no WebSocket "clear playback" message, so when a
  caller interrupts, the bot stops generating new audio but any already-buffered audio plays
  out. Low `SIP_VAD_STOP_SECS` + small TTS chunks minimise the tail. If your module build
  exposes a flush/clear command, add it to `ModAudioStreamSerializer.serialize` under
  `InterruptionFrame`.
- **`audio_stream` app name:** registered by `mod_audio_stream`. Verify with
  `fs_cli -x "show application audio_stream"`; some builds expose it as the `uuid_audio_stream`
  API only (then drive it from an ESL script instead of the dialplan application).
- **No-token alternative for the module:** amigniter ships a prebuilt Debian-12 `.deb`
  (https://github.com/amigniter/mod_audio_stream/releases) you can `dpkg -i` onto a community
  FreeSWITCH image instead of the SignalWire build stage in `freeswitch/Dockerfile`.
- **Optional later:** have `manager.py` start/stop the gateway and surface each agent's
  `ws://…/ws/<id>` URL + DID mapping in the admin UI.

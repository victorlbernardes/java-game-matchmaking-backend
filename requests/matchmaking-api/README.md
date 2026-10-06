# matchmaking-api (port 8080)

HTTP/WebSocket gateway. Every request except `/health` needs a Bearer token.

## Start

auth-service must be running first (matchmaking-api fetches its JWKS on
startup), then:

```bash
mvn -pl services/matchmaking-api exec:java -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication
```

## Endpoints

| File | Endpoint | Purpose |
|------|----------|---------|
| `GET-health.http` | `GET /health` | Liveness probe (no auth) |
| `POST-v1-matchmaking-tickets.http` | `POST /v1/matchmaking/tickets` | Create ticket; idempotency; 400/401 cases |
| `GET-v1-matchmaking-tickets-by-id.http` | `GET /v1/matchmaking/tickets/{id}` | Fetch ticket; 403/404 cases |
| `DELETE-v1-matchmaking-tickets-by-id.http` | `DELETE /v1/matchmaking/tickets/{id}` | Cancel; 403/404/409 cases |
| `WS-v1-ws.http` | `WS /v1/ws` | Push channel; IntelliJ-only (see below) |

## WebSocket (`GET /v1/ws`)

`WS-v1-ws.http` covers the push channel with IntelliJ IDEA's HTTP Client
(`WEBSOCKET` pseudo-method) — run the request line, then run each `###` section
to send a frame. The Response panel shows server frames. VS Code's REST Client
does **not** support WebSocket; there, use Python or `websocat`:

```bash
python3 -c "
import asyncio, websockets, json
async def main():
    async with websockets.connect('ws://localhost:8080/v1/ws?token=<JWT>') as ws:
        await ws.send(json.dumps({'type':'PING','payload':{}}))
        print(await ws.recv())   # {\"type\":\"PONG\",...}
asyncio.run(main())
"
```

Client→server frame types: `PING`, `MATCH_CONFIRM`, `MATCH_DECLINE`
(payload `{"matchId": "..."}`). Server→client: `PONG`,
`MATCH_PENDING_CONFIRMATION`, `MATCH_CONFIRMED`, `MATCH_CANCELLED`, `ERROR`.
The matchmaking-engine must be running for pairing/confirmation to work.

Suggested order: `GET-health.http` → login (auth-service) →
`POST-v1-matchmaking-tickets.http` → `GET-v1-matchmaking-tickets-by-id.http`.

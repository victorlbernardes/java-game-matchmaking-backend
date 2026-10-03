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

Suggested order: `GET-health.http` → login (auth-service) →
`POST-v1-matchmaking-tickets.http` → `GET-v1-matchmaking-tickets-by-id.http`.

# Request Collection

Single Postman/Insomnia collection for the entire matchmaking backend.

## Import

1. **File → Import** → select `matchmaking.postman_collection.json`
2. **File → Import** → select `matchmaking.postman_environment.json`
3. Select the `matchmaking` environment (top-right dropdown)

## Collection structure

```
matchmaking/
├── auth-service/          # POST /v1/auth/login, GET /.well-known/jwks.json
└── matchmaking-api/
    ├── health/            # GET /health
    ├── tickets/           # POST/GET/DELETE /v1/matchmaking/tickets (+ error cases)
    ├── history/           # GET /v1/players/{id}/matches (+ error cases)
    ├── websocket/         # WS /v1/ws (PING, MATCH_CONFIRM, MATCH_DECLINE)
    └── e2e-w5/            # Full W5 E2E flow (login ×2 → WS → pair → confirm → history)
```

## Start services (order matters)

```bash
mvn -pl services/auth-service exec:java -Dexec.mainClass=com.victor.matchmaking.auth.AuthServiceApplication
mvn -pl services/matchmaking-api exec:java -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication
mvn -pl services/matchmaking-engine exec:java -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication
```

## Typical flow

1. `auth-service` → `POST /v1/auth/login` (captures `token`)
2. `matchmaking-api` → `health` → `tickets` → `history` → `websocket`
3. For E2E: run the `e2e-w5` folder top to bottom

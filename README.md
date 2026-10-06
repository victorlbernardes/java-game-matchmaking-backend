# game-matchmaking-backend

Backend for matchmaking multiplayer games — modular Maven monorepo, Java 21, Eclipse Vert.x.

## Structure

```
├── common/           # common-domain, common-proto, common-redis, common-events, common-db
├── services/         # matchmaking-api, matchmaking-engine, auth-service, skill-service, match-orchestrator
├── requests/         # .http collections per service to test every endpoint manually
├── docs/adr/         # Architecture Decision Records
└── docker-compose.yml
```

## Build and test

```bash
mvn -B verify
```

Every endpoint must be covered by unit/integration tests **and** by a runnable
HTTP request in `requests/` — see `requests/README.md`.

## Local infrastructure

```bash
docker compose up -d
```

## Documentation

The vault at `/Users/victor/Projects/AI/OpenCode/Marchmaking/obsidian` is the source of truth
for scope, domain, API contract, and roadmap. `AGENTS.md` describes the conventions
(English code, enums for states, pure domain, no cross-service dependencies).

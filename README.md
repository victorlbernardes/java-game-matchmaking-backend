# game-matchmaking-backend

Backend de matchmaking para jogos multiplayer — monorepo Maven (modular), Java 21, Eclipse Vert.x.

## Estrutura

```
├── common/           # common-domain, common-proto, common-redis, common-events
├── services/         # matchmaking-api, matchmaking-engine, auth-service, skill-service, match-orchestrator
├── docs/adr/         # Architecture Decision Records
└── docker-compose.yml
```

## Build e teste

```bash
mvn -B verify
```

## Infraestrutura local

```bash
docker compose up -d
```

## Documentação

O vault em `/Users/victor/Projects/AI/OpenCode/Marchmaking/obsidian` é a fonte de verdade
para escopo, domínio, contrato da API e roadmap. O `AGENTS.md` descreve as convenções
(código em inglês, enums para estados, domínio puro, sem dependências cruzadas entre serviços).

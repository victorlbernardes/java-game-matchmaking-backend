# ADR 0035: Regra de cálculo de skill (Elo com calibração)

## Context

O matchmaking precisa de um número de skill (0–100) que evolua com base nos resultados
das partidas. Até a W23 o skill era estático (`DeterministicSkillProvider` deriva um
número fixo do `playerId`). A W24 introduz o ciclo completo: o game server reporta
o resultado via `POST /finished`, o orchestrator publica `MATCH_FINISHED`, o
skill-service consome, recalcula e publica `SKILL_UPDATED`.

A decisão é qual regra usar: Elo, Glicko-2, TrueSkill, ou uma janela deslizante
das últimas N partidas normalizada para 0–100.

## Options

### 1. Elo na escala 0–100 com calibração (escolhido)
- K = 16 nas primeiras 10 partidas, K = 8 depois
- 100 pontos de rating = 400 de Elo interno
- Oponente = média do time adversário (uma partida 5v5 vira um número por time)
- Clamp 0–100

### 2. Glicko-2
- Mantém desvio de rating (incerteza) e volatilidade
- Mais preciso para jogadores com poucos jogos ou longos hiatos
- Complexidade significativamente maior (sistema de equações não-lineares por período)

### 3. TrueSkill (Microsoft)
- Bayesian skill rating para times
- Lida naturalmente com times de tamanhos diferentes
- Propriedade industrial, implementação não-trivial

### 4. Janela das últimas N partidas (rolling average)
- Simples: média dos resultados recentes normalizada
- Não distingue oponente forte de fraco
- Não converge para "verdadeiro" skill, apenas reflete forma recente

## Decision

**Elo com calibração (Opção 1).**

Rationale:
- O Elo é o padrão da indústria para jogos 1v1 e adapta-se bem a times via média
- A calibração (K alto no início) resolve o problema do "jogador novo" sem precisar
  de desvio/volatilidade do Glicko-2
- O 5v5 como média do time é uma simplificação aceitável: o ADR registra que o
  Elo por time não é matematicamente puro (não é cinco duelos independentes), mas
  funciona na prática e evita a complexidade do TrueSkill
- Clamp 0–100 garante que o número sempre sirva para o matchmaker sem validação extra
- Determinístico, testável por propriedades (faixa, monotonicidade, soma zero)

## Como funciona

### Entrada (`MATCH_FINISHED`)
```protobuf
message MatchFinished {
  Winner winner = 5;           // TEAM_1, TEAM_2, DRAW
  repeated MatchPlayerResult players = 6; // player_id, team, skill_at_match
}
```

### Cálculo
Para cada jogador:
1. `opponentRating` = média de `skill_at_match` do time adversário
2. `expected` = 1 / (1 + 10^((opponentRating - playerRating) / 100))
3. `score` = 1.0 (vitória), 0.5 (empate), 0.0 (derrota)
4. `K` = 16 se `matchesPlayed < 10`, senão 8
5. `newRating` = clamp(playerRating + K * (score - expected), 0, 100)
6. `matchesPlayed++`, `provisional = matchesPlayed < 10`

### Saída (`SKILL_UPDATED`)
```protobuf
message SkillUpdated {
  repeated PlayerSkillUpdate updates = 3; // player_id, skill_before, skill_after, matches_played, provisional
}
```

### Persistência
Tabela `player_skills` (PostgreSQL):
- `player_id` PK
- `rating` NUMERIC(5,2) CHECK 0–100
- `matches_played` INT
- `provisional` BOOL
- `updated_at` TIMESTAMPTZ

Cache Redis `skill:{playerId}` (TTL 1 h) escrito **após** commit.

### Idempotência
Inbox `processed_events` com `consumer_group = skill-service`. Replay de `MATCH_FINISHED`
não recalcula (rowCount = 0 no upsert).

### Concorrência
Duas partidas do mesmo jogador terminando juntas: `SELECT ... FOR UPDATE` das linhas
do elenco em ordem de `player_id` (sem deadlock), depois upsert.

## Como testar

### Testes de propriedade (`SkillRatingTest`)
1. **Faixa**: 0 perde → 0, 100 ganha → 100 (clamp)
2. **Vitória sobe**: rating após vitória > rating antes
3. **Derrota desce**: rating após derrota < rating antes
4. **Empate move na direção do favorito**: azarão sobe, favorito desce
5. **Monotonicidade**: mesmo resultado, rating inicial menor → ganho maior
6. **Soma zero**: dois jogadores estabelecidos, longe das bordas, ganho de um = perda do outro
7. **Determinismo**: mesmas entradas → mesma saída

### Testes de calibração (`SkillCalibrationTest`)
- Jogador novo = provisional=true, K=16
- 10ª partida → provisional=false, K=8
- Default 50 para jogador nunca visto

### Teste de idempotência (`SkillUpdateIdempotencyTest`)
- Mesmo `MATCH_FINISHED` aplicado duas vezes → um `processed_events`, um `SKILL_UPDATED`, rating inalterado na segunda

### Teste de concorrência (`SkillConcurrencyTest`)
- Dois eventos do mesmo jogador em paralelo → soma dos deltas = regra serial

### Cenário E2E (`requests/skill/verify_skill.sh`)
1. Partida 1v1 formada, confirmada, iniciada
2. Resultado reportado → `MATCH_FINISHED` no tópico
3. `SKILL_UPDATED` no tópico
4. `player_skills` com 50→54 / 50→46
5. `GET /v1/skills/{id}` retorna novo valor + `provisional=true`
6. Replay do evento → nada muda
7. Segundo ticket do mesmo jogador carrega skill novo (cache)
8. JWT de outro jogador → 403

## Trade-offs

| Aspecto | Elo escolhido | Glicko-2 | TrueSkill | Rolling window |
|---------|---------------|----------|-----------|----------------|
| Precisão | Boa | Muito boa | Excelente (times) | Baixa |
| Complexidade | Baixa | Alta | Muito alta | Mínima |
| Calibração nativa | Sim (K variável) | Sim (RD alto) | Sim | Não |
| Hiato (inatividade) | Não tratado | RD cresce | Não tratado | Janela expira |
| 5v5 via média | Aproximação | Aproximação | Nativo | Nativo |

O Elo perde a incerteza do Glicko-2 (um jogador que não joga há meses mantém o mesmo
rating). Isso é aceitável para o MVP — o decaimento por inatividade fica para
`MelhoriasFutura.md` item 20.

O 5v5 como média do time (em vez de cinco duelos) é uma simplificação: matematicamente
o Elo assume jogos 1v1, mas na prática a média funciona e evita a complexidade do
TrueSkill. O ADR registra que o erro introduzido é pequeno para matchmaking.

## Consequences

### Positivas
- Skill dinâmico implementado com código simples e testável
- `MATCH_FINISHED` fecha o ciclo da partida (orchestrator → skill-service)
- `SKILL_UPDATED` dá telemetria sem acoplar consumidores
- Cache Redis + fallback determinístico = matchmaking nunca para por falta de skill service

### Negativas / Riscos
- Cache fora da transação: stale por um evento (aceitável, o próximo evento corrige)
- Sem decaimento por inatividade (documentado em `MelhoriasFutura.md`)
- 5v5 como média pode subestimar disparidades internas do time (mitigado pelo clamp)
- `release` continua sendo o único caminho que devolve a vaga do servidor (o resultado
  chega antes, mas a vaga só volta no `release`)

## Referências

- ADR 0018: Transactional Outbox (o skill-service usa o outbox genérico de `common-outbox`)
- ADR 0016: One topic per event type (`MATCH_FINISHED`, `SKILL_UPDATED`)
- ADR 0038: Consumer idempotency (inbox `processed_events`)
- `MelhoriasFutura.md` item 18: punição de no-show (dado existe em `no_show_at`, regra pendente)
- `MelhoriasFutura.md` item 20: decaimento por inatividade
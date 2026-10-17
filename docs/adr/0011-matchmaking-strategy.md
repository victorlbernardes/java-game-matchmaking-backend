# ADR-0011 — Matchmaking strategy driven by progressive skill widening (W10)

- **Status:** Accepted
- **Date:** 2026-10-16
- **Sprint:** W10
- **Post:** pending

## Context

The matchmaking rules in `03 - Domínio e Contrato da API.md` require that two
players can only be paired when their skill levels are close enough. At the same
time, players keep re-queueing while they wait, and the current engine pairs the
head of every queue without looking at how long a player has already been waiting.

Introducing a per-player skill number therefore needs two coordinated pieces:

- a way to obtain the skill of any player, owned by the skill-service; and
- a rule that decides when two queued players are compatible, without making
  the rule depend on live skill-service calls on the hot path.

## Problem

Without a strategy:

- the queue cannot express which players are close enough to each other;
- the same player may starve the queue behind a stronger player, because the
  engine never widens the accepted range;
- every candidate must be validated without blocking the event loop (rule 4 of
  the vault: Java 21 + Vert.x is reactive).

## Options Considered

| Option | Pros | Cons | Cost |
| --- | --- | --- | --- |
| Static skill windows fixed at startup | Smallest code | No adaptation to the queue; one wrong constant freezes the rule | Low |
| Live skill lookup per candidate | Always exact | Blocks the event loop; couples the engine to the skill-service at runtime | High |
| Progressive widening around the queued player's skill (chosen) | Reactive, bounded, adapts to wait time; skill stays owned by the skill-service | The range grows over time; needs an explicit half-range per step | Low-Medium |

## Decision

Introduce a `SkillProvider` port in `common-domain` (rule 1: zero framework
dependencies). The API service owns a deterministic stub implementation until W24
replaces it with the real Elo/Glicko-2 call. `CreateTicketUseCase` resolves the
skill of the requesting player when the ticket is created and stores it on the
ticket.

The compatibility rule is `PairingPolicy.canPair`:

- the candidates must contain exactly `GameMode.playersPerMatch()` tickets, all
  `SEARCHING`, with distinct players, in the same game mode and region;
- every pair must lie inside the widening half-range of the *queued* player's
  skill, where the half-range grows with the player's wait time;
- the half-range is chosen by `WideningPolicy.halfRange(waitTimeSeconds)`: waits
  under 30s use a half-range of 2, waits under 60s use 5, waits under 120s use 10,
  and waits of 120s or more use the maximum half-range of 20.

Measured numbers (all numbers below were taken with `mvn -B clean test` on the
W10 redo branch):

- `WideningPolicyTest`: 5 tests, all green — the thresholds (±2/±5/±10/±20) and the
  monotonicity/boundedness properties behave as designed.
- `PairingPolicyTest`: 6 tests, all green — the W9 structural rules plus the W10
  widening rules (accepts inside the initial half-range, rejects outside, widens
  with wait time).
- `MatchmakingApiTest` (HTTP level): 5 tests, all green. The deterministic stub
  assigns `skillForPlayer` via a stable hash (`Math.abs(hashCode) % 101`), and
  the `searchRange` assertions use the expected half-width of 2 (min 58 / max 62
  around the deterministic skill of 60).
- Engine pairing tests: `PairingFiveVsFiveTest` (1), `PairingRaceTest` (1),
  `PairingStressTest` (1), `IdempotencyRaceTest` (1), `IdempotencyStressTest` (1),
  `PendingMatchTest` (2) — all green, **0 invariant violations**.
- Latency of the whole HTTP create-ticket round trip stays under the target
  measured in W8 (≈ 12 ms locally, dominated by JVM startup of the test client).

Trade-offs:

- The widening half-range is a pure function of the wait time (`WideningPolicy`)
  with a named maximum, so the rule stays deterministic and testable; growing the
  range forever would eventually weaken the rule, which is why the maximum is
  bounded.
- Skill values are frozen when the ticket is created, so a later skill-service
  update never changes an ongoing search (intentional: search is anchored).

Wiring (found by the W11 E2E validation):

- The engine must pass the **queue head's `queuedAt`** to `PairingPolicy.canPair`.
  It previously passed `Instant.now()`, so `Duration.between(now, now)` was always
  zero and the half-range stayed at 2 — the widening never activated in
  production. Fixed in `MatchmakingEngineApplication.tryPairFromQueue`
  (`buildRoster(candidates, gameMode, candidates.get(0).queuedAt())`).
- E2E proof against the running services: a skill gap of 3 is **not** paired at
  t≈0 (half-range 2) and **is** paired after the head waits 30–60s (half-range 5).

Consequences:

- The engine never calls the skill-service; it only reads the skill stored on
  each ticket.
- Adding W24 requires no change to `PairingPolicy`: only the `SkillProvider`
  implementation is replaced.

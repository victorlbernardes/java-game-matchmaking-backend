# ADR-0013 — Fill and party units (W11)

- **Status:** Accepted
- **Date:** 2026-10-16
- **Sprint:** W11
- **Post:** pending

## Context

W11 adds the concept of a party: a group of players that queues together as one
unit. Three features need it:

- `POST /v1/parties` creates a party owned by its leader; members join and the
  leader (or the acting leader) may remove members or dissolve the party;
- a cancelled ticket of any member cancels every member ticket, because the
  party unit is atomic — the remaining members keep the party but re-queue later;
- a party that is only partially queued must be completed by solo players
  before pairing, and it must never be split across two matches.

Without this, a party either blocks the queue forever or its members scatter
into unrelated matches, which is exactly the failure described by the sprint
("uma party não pode ser dividida no time").

## Problem

- The engine pairs the head of a queue with exactly `playersPerMatch()` tickets.
  If only some members of a party queued, those tickets either pair without the
  missing players or compete with solo candidates.
- Nothing links the tickets of a party to each other, so the engine cannot know
  that two queued tickets belong to the same party.
- `Party` in `common-domain` carries `memberTicketIds`, but nothing populates it,
  so the party record has no view of which tickets represent its members.
- `POST /v1/matchmaking/tickets` already parses `partyId` and `members` in the
  request body, but `CreateTicketUseCase` ignores them.

## Options Considered

| Option | Pros | Cons | Cost |
| --- | --- | --- | --- |
| Fill the party at enqueue time | One queue position per party | The first member's request creates the tickets of the whole party, blocking solo players who arrive earlier | High |
| Fill the party at pairing time (chosen) | Solo players fill the gaps; parties always prioritized; the queue keeps accepting solo players | Pairing must group tickets by party before pairing | Medium |
| Split the party across matches | Simplest engine change | Violates the sprint goal; the party could be divided | High (product) |
| Drop parties entirely | Smallest code | Violates the MVP scope (parties are part of W11) | High (product) |

## Decision

- `MatchmakingEngineApplication` groups the tickets that share a `partyId` before
  pairing, and always prioritizes a (partial) party over solo pairing, so a party
  is never split across two matches.
- `FillPolicy.fill(party, solos)` completes a partial party: it picks the solo
  candidates whose skill deviates least from the party's aggregated skill and
  applies the anti-boost rule (`Party.MAX_PARTY_DISPERSION`).
- `FillPolicy` is a pure function (no dependencies) so it is unit tested without
  Redis; `MatchmakingEngineApplication.buildRoster` adopts it in the engine loop
  (parties first, then solo pairing).
- Party lifecycle use cases live in `matchmaking-api`: create, add member, get
  party, remove member (leader only), dissolve (leader only).
- A member ticket carries its `partyId`. Cancelling (or removing) a member
  cancels every member ticket of the party, because the party unit is atomic;
  the remaining members keep the party and can re-queue later.
- `POST /v1/matchmaking/tickets` with a `partyId` enqueues the whole party in one
  atomic command: `CreateTicketUseCase` creates one ticket per member and links
  the new ticket ids into `Party.memberTicketIds`.
- `TicketRepository.createPartyTickets` saves every ticket of the party in one
  go and returns their ids; the caller links them to the party.

Trade-offs:

- The engine must group tickets by `partyId` before pairing, which adds one
  grouping step per pairing sweep.
- The anti-boost rule skips candidates that would push the party's skill
  dispersion past `MAX_PARTY_DISPERSION` (20); when no candidate fits, the party
  stays partial and the queue proceeds with the solo candidates.
- Linking tickets to the party needs one extra `PartyStore.save` per enqueue;
  this is a single short Redis call.

Measured numbers (all measured with `mvn -B clean test` on the W11 redo branch):

- `FillPolicyTest`: 5 tests, all green — least-deviation fill, anti-boost skip,
  solo candidates that already belong to a party are never used, not enough
  candidates → `null`.
- `PartyRosterTest` (engine, no Redis): 4 tests, all green — the roster resolver
  fills a partial 5v5 party with solo candidates, keeps the party queued when it
  cannot be filled, pairs solos alone, and never pairs an incomplete party.
- `RemoveMemberUseCaseTest`: 2 tests, `DissolvePartyUseCaseTest`: 1 test,
  `CancelTicketUseCaseTest`: 2 tests, all green — leaving, dissolving or
  cancelling a member cancels **every** member ticket of the party atomically.
- `CreateTicketUseCaseTest`: 2 tests, all green — every ticket of a party enqueue
  carries the `partyId` (so the engine can group the party) and an unknown party
  fails with `PartyNotFoundException` instead of a `NullPointerException`.
- `MatchmakingApiTest` (HTTP level): 6 tests, all green, including the party
  enqueue reaching the party created by `POST /v1/parties` (one shared
  `PartyStore` for every use case).
- The full monorepo build compiles and every test class runs green:
  **65 tests, 0 failures, 0 errors** (`mvn -B clean test`, BUILD SUCCESS).
- E2E against the running services (auth + api + engine + Redis + Postgres):
  **37/37 checks green** — party of 3 + 7 solos → one 10-player match with the
  three members together; 2 parties of 3 + 4 solos → no match (party stays
  queued); cancel/remove/dissolve a member → 3/3 member tickets `CANCELLED`.
  Runner: `requests/e2e/run_e2e.py`.

Fixes found by the E2E validation (all in this sprint's scope):

- The real `main` wiring injected **three different** `InMemoryPartyStore`
  instances, so a party created by one route was invisible to the ticket routes
  (HTTP 500 on party enqueue) and the atomic cancel never found the party. `main`
  now uses a single `RedisPartyStore`, and the dev/test overloads share one
  in-memory store.
- `CreateTicketUseCase.buildTicket` hard-coded `partyId = null`, so party tickets
  never carried the party, and the engine could never keep the party together.

Consequences:

- `MatchmakingEngineApplication` must keep the `partyId` on every ticket it
  considers; tickets of a party are always paired together.
- The leader rule is enforced inside `RemoveMemberUseCase` and
  `DissolvePartyUseCase` (and inside `CancelTicketUseCase` for member tickets),
  so presentation code never decides authorization.
- `PartyUseCases`, `CreateTicketUseCase` and `CancelTicketUseCase` must share the
  same `PartyStore` instance; every wiring path keeps that invariant.

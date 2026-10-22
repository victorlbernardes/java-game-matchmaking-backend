# ADR-0033 — A connection token that is one match, one player, two minutes (W23)

## Status
Accepted (2026-10-21)

## Context

W22 gave the confirmed match a game server. The player still had no way to reach it: the contract in
`03 - Domínio e Contrato da API.md` promised an endpoint that answers "where do I play, and with what
proof", and nothing implemented it. Without it the game server would have to ask the matchmaking for
permission on every accept — a synchronous dependency on the matchmaker's availability at the exact
moment a match is starting — or trust a player id, which is not a credential.

The token also has to be *small in what it grants*. A player's session token is the player's whole
identity: with it, a game server could call anything the player can. What a game server needs is
"this player, in this match, on this team, right now", and nothing else.

## Options

### Who signs

**A. The API signs with its own key.** It is the process that answers the endpoint, and it would add a
second signer whose key the game servers must also trust — and the JWKS endpoint is the auth service's
(`/.well-known/jwks.json`), so the game server would have to trust two issuers for two kinds of token.

**B. The auth service signs, the API asks (chosen).** One issuer, one JWKS, one `kid` rotation story.
The API is a client of `POST /v1/auth/connection-tokens`, authenticated with the **player's own**
session token — so the auth service is not asked to trust the API, only to narrow a token it already
issued.

**C. A shared secret between the API and the game servers.** A second credential system to rotate and
to leak, and the game servers already verify JWTs against the auth service's JWKS.

### What proves the caller

**D. A service token from the API.** The API could mint for any player it names — one compromised API
is every player's connection token.

**E. The player's own session token (chosen).** `sub` comes from the token, never from the body. The
API passes the player's bearer token through; a caller cannot ask for a token for somebody else, and
the auth service's rule is "the subject of the token you hold".

### Where the address comes from

**F. The engine's `PendingMatch`.** The engine knows the roster and the match, not the server: the
allocation is the orchestrator's (W22), and a match that has not been allocated has no address at all.

**G. The allocation's read model, read by the API (chosen).** `GET /v1/matches/:matchId/connection`
reads `match_allocations` + `game_servers` (read-only, the same way the engine reads the pool) and
answers the state it finds. The API does not call the orchestrator: the table is the contract W22
already wrote, and a synchronous call would put the orchestrator in the API's request path for a
question that is a row.

## Decision

`POST /v1/auth/connection-tokens` (auth service, the player's session token, body `{matchId, team}`)
answers `{connectionToken, expiresAt, expiresInSeconds}`, an RS256 JWT signed with the same key the
JWKS publishes, whose claims are `sub` (the player), `matchId`, `team`, `scope = game-connect`,
`iat`, `exp` — **`CONNECTION_TOKEN_TTL_SECONDS`, default 120**. The scope claim is what makes the two
kinds of token non-interchangeable in both directions: a game server that only checks the signature
would accept a session token, and the matchmaking would accept a connection token as a session if it
did not check the scope.

`GET /v1/matches/:matchId/connection` (API, the player's session token) answers, for a member of the
roster:

| allocation state | answer |
|---|---|
| `ALLOCATED`, `STARTED` | `200 {matchId, status, team, serverAddress, connectionToken, expiresAt, expiresInSeconds}` |
| `PENDING_ALLOCATION` | `409 not_allocated` (confirmed, no server yet — wait, do not give up) |
| `REJECTED` | `409 match_rejected` + `reason` (`no_capacity`, `start_timeout`, `server_failed`, `all_no_show`) |
| `RELEASED` | `409 match_finished` |
| not a member, or unknown match | `404 match_not_found` (the two are indistinguishable on purpose) |

The auth service being unavailable is `503 token_issuer_unavailable`, counted with the dependency tag
(`TAG_DEPENDENCY_AUTH`) — the same shed-the-dependency shape W17/W18 gave Postgres and Redis.

The game server validates **offline**: signature against the JWKS it already has, `scope`, `exp`,
`matchId` and `team` against the match it is running. It never calls the matchmaking on the accept
path.

## How it works (files)

- `services/auth-service/.../application/ConnectionTokenIssuer.java` — the port (`issue(playerToken,
  matchId, team)`).
- `services/auth-service/.../infrastructure/JwtConnectionTokenIssuer.java` — the claims, the TTL, the
  same `RsaKeyPairProvider` the session tokens use (`fullJwkOf`, so the JWKS serves the signing key).
- `services/auth-service/.../presentation/AuthRoutes.java` — `POST /v1/auth/connection-tokens`, `401
  missing_token`, `400 match_id_required`.
- `services/matchmaking-api/.../application/MatchConnections.java` — the repository + issuer port pair,
  and `disabled()` (no issuer configured → the route is not registered, which keeps every older
  deployment and scenario untouched).
- `services/matchmaking-api/.../infrastructure/ConnectionTokenClient.java` — the HTTP adapter
  (`vertx-web-client`), the player's bearer token forwarded.
- `common/common-db/.../MatchConnectionRepository.java` — the read model: roster membership, the
  allocation's state and reason, the server's address (`LEFT JOIN game_servers`, so a missing pool row
  is `not_allocated`, not a 500).
- `services/matchmaking-api/.../presentation/HttpRoutes.java` — `connectionInfo`, the five answers and
  `matchmaking.connection.requests{result}` (`served`, `not_found`, `not_allocated`, `match_rejected`,
  `match_finished`, `issuer_unavailable`).

## Measured numbers (local stack, 2026-10-21)

`bash requests/lifecycle/verify_lifecycle.sh` (75 checks, 0 failures) and the API's
`MatchConnectionTest` (9 tests).

**The token is scoped and short.** `200` with `serverAddress 10.0.0.7:7777`, `team 1`, and a token
whose decoded claims were `scope=game-connect`, `matchId=match-<uuid>`, `team=1`, `sub=w23-7`,
`exp` **117 s** away (the 3 s are the request's own time) with `expiresInSeconds = 120`.

**A stranger learns nothing.** A logged-in player outside the roster got `404` — the same answer an
unknown match id gets. The counter moved: `matchmaking_connection_requests_total{result="served"}` > 0.

**A match with no server says so.** `PENDING_ALLOCATION` → `409 not_allocated`, twice in the run (once
in the deadline phase, once in the retry phase), and after the rejection the same endpoint answered
`409 match_rejected` with `reason=no_capacity`; after the release, `409 match_finished`.

**A started match still hands out the token.** `STARTED` → `200` with `status: STARTED` (the state the
client will see for most of the match).

**The issuer is the auth service, not the API.** `POST /v1/auth/connection-tokens` without a token →
`401 missing_token`; with the player's token → `200`, `expiresInSeconds = 120`, verified against the
published JWKS in `ConnectionTokenTest` (6 tests): the signature verifies against the JWKS and the
claims carry the scope and the 120 s `exp`; **a session token is not a connection token** (the scope
check); the `sub` comes from the caller's token, never from the body; a request without a session
token is refused; a forged one is refused; and an incomplete body is `400`.

## How to test

```bash
# the properties (no stack: Postgres for the read model, an in-process auth for the claims)
mvn -o -B -pl services/auth-service test -Dtest=ConnectionTokenTest
mvn -o -B -pl services/matchmaking-api test -Dtest=MatchConnectionTest

# by hand, against the scenario's stack
bash requests/lifecycle/verify_lifecycle.sh
```

By hand: `curl -s localhost:8099/v1/matches/$MATCH/connection -H "authorization: Bearer $TOKEN"`, then
paste `connectionToken` into `jwt.io` (or `cut -d. -f2 | base64 -d`) — the claims are readable and the
`exp` is two minutes out. The `lifecycle-w23` folder in the collection is the same path request by
request (requests 1-2 and 8-9).

The guard was checked by **removing** it: `MatchConnectionTest`'s "a match with no server is not given
an address" fails if the read model falls back to a default address, and the `scope` assertion in
`ConnectionTokenTest` fails if the claim is dropped (the signature still verifies — which is exactly
why the claim is asserted and not assumed).

## Trade-offs

- **The token is a bearer credential for two minutes.** Anyone who reads it can join the match as that
  player for the rest of the window. The window is the mitigation (short, single match, single team),
  and the alternative — a token the game server must exchange for a session — puts the matchmaking back
  in the accept path.
- **The `team` in the body is the caller's claim.** The API passes the roster's team, and the auth
  service signs what it is told: it does not read `match_roster`. A direct caller could ask for the
  other team. The game server is the one that refuses a wrong team (it has the roster it was
  allocated), so the trust boundary is where the match actually is — but it is a claim, not a fact.
- **The API reads the allocation table directly.** No orchestrator call, no cache: a row read per
  request. It is the same read model the engine uses, and it means the API's answer can be one
  watchdog tick behind the truth (`ALLOCATED` just rejected → `409` arrives a second later). The
  client's answer is "try again", which is what it would do anyway.
- **`GET …/connection` is a read the client can hammer.** It is authenticated and rate-limited by the
  API's per-player bucket (W17), and it issues a token per call, so the auth service sees the same
  rate. A cache would be the next step, and it would cache a credential — the wrong thing to cache
  before it is needed.
- **The route disappears when the issuer is not configured.** `MatchConnections.disabled()` keeps the
  older deployments (E2E, load, backpressure) working without an auth service, at the cost of a
  capability that is silently absent instead of answering `503`. The health endpoint does not say it;
  the sprint's scenario is what proves it is wired.
- **No revocation.** A token lives 120 s and there is no deny-list; a match rejected inside the window
  still has valid tokens in the wild. The game server's own start/connect reports and the watchdog are
  what bound the damage, and the window is short by design.

## Consequences

- A confirmed player learns where to play and gets a proof that grants exactly that match, for two
  minutes, without the matchmaking in the accept path.
- The game servers verify offline against one issuer and one JWKS — the same key rotation story as the
  session tokens.
- The endpoint answers the whole lifecycle (`not_allocated`, `match_rejected` with the reason,
  `match_finished`), so a client has one call to poll instead of inferring state from a ticket.
- W24 can add `MATCH_FINISHED` without touching this contract: the release already answers
  `match_finished` through the same read model.

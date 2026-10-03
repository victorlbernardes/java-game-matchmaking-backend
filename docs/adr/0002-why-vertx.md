# ADR-0002 — Why Eclipse Vert.x

- **Status:** Accepted
- **Date:** 2026-10-01
- **Sprint:** W2

## Context

The gateway must serve many concurrent HTTP connections without dedicating a
thread per request. We need a reactive, event-loop based server for the W2 spike:
a ticket API that should sustain thousands of connections on a small thread pool.

## Problem

A thread-per-request server (classic servlet model) would need one thread per
in-flight connection, exhausting memory and context-switch budget long before the
matchmaking gateway reaches tens of thousands of concurrent players.

## Options Considered

| Option | Pros | Cons | Cost |
| --- | --- | --- | --- |
| Spring MVC (thread per request) | Familiar to the team | One thread per connection | High memory per connection |
| Spring WebFlux | Reactive, similar model to Vert.x | Two programming models; still Netty under the hood | Steeper learning curve |
| Vert.x | Event-loop, minimal abstractions, Netty under the hood | Imperative callbacks/futures; smaller ecosystem | Environmental |

## Decision

Use Eclipse Vert.x as the reactive runtime for all HTTP/WebSocket services.

## How it works

Vert.x runs a `Vert.x` instance with `2 × CPU` event loop threads. Each non-blocking
request handler executes on the event loop; I/O completion is pushed back as
callbacks/futures instead of blocking a worker. A single `HttpServer` + `Router`
serves all routes, and every route handler must never block the event loop
(no `Thread.sleep`, no blocking JDBC, no synchronous Redis calls inside handlers).

## How to test

Run `mvn -B verify`; integration tests boot a real `Vert.x` on an ephemeral port and
hit `GET /health`, `POST /v1/auth/login`, `GET /.well-known/jwks.json`,
`POST /v1/matchmaking/tickets` and `GET /v1/matchmaking/tickets/{id}`. The load
number below proves the event loop handles 50 concurrent connections.

## Trade-offs

- Blocking code inside a handler silently stalls **all** connections on that thread.
- Stack traces are less linear than synchronous code (callback chains).
- Smaller job market than Spring.

## Consequences

- [x] Expected gain: measurable concurrency — see Evidence
- [ ] Introduced risk: accidental blocking on the event loop
- [ ] Review needed when "handlers block on I/O" becomes a recurring review finding

## Evidence

Load test on the W2 build (`ab -n 2000 -c 50`, macOS dev machine, JDK, Vert.x 5.2.0):

- Throughput: ~684 requests/s mean
- p50 latency: 62 ms, p95: 168 ms, p99: 242 ms, max 314 ms
- All responses under 50 concurrent connections, no thread pool growth (event loop, `2×CPU` threads)

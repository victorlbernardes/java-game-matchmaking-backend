# HTTP Request Collections

Every endpoint must be exercisable both by unit/integration tests **and** by a
runnable HTTP request in this folder, grouped per service. When you add or
change an endpoint, update the matching `.http` file in the same change.

## Layout

```
requests/
├── auth-service/       # POST /v1/auth/login, GET /.well-known/jwks.json
└── matchmaking-api/    # GET /health, POST/GET /v1/matchmaking/tickets
```

Each `.http` file starts with a comment block explaining **when** to use the
endpoint, **how** to run it, and the expected success/error responses. Each
service folder has its own `README.md` with start commands and the suggested
execution order.

## How to run

These files use the IntelliJ IDEA / VS Code REST Client `.http` format.

1. Start auth-service (see `auth-service/README.md`).
2. Run `auth-service/POST-v1-auth-login.http` — it captures the JWT into the
   `token` client variable.
3. Start matchmaking-api (see `matchmaking-api/README.md`).
4. Run the matchmaking-api requests in the order listed in its README.
   `POST-v1-matchmaking-tickets.http` captures `ticketId` for follow-ups.

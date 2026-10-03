# auth-service (port 8081)

Dev stub: issues RS256 session JWTs and exposes the public JWKS.

## Start

```bash
mvn -pl services/auth-service exec:java -Dexec.mainClass=com.victor.matchmaking.auth.AuthServiceApplication
```

## Endpoints

| File | Endpoint | Purpose |
|------|----------|---------|
| `POST-v1-auth-login.http` | `POST /v1/auth/login` | Issue a session token (and error case) |
| `GET-well-known-jwks.http` | `GET /.well-known/jwks.json` | Public key(s) for offline validation |

Run `POST-v1-auth-login.http` first — it captures the JWT into the `token`
variable used by the matchmaking-api requests.

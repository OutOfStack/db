# Security policy

## Deployment boundary

DB has no authentication, TLS, verified replication peer identity, or per-command authorization. Supported deployment
is limited to loopback or an explicitly trusted, isolated private network. Public or untrusted remote deployment is
unsupported. Anyone who can reach the client port can read and modify data; anyone who can reach a master replication
port can request its WAL and snapshots. A standby trusts its configured master.

Native and container defaults bind `127.0.0.1:3223`. Both client and replication listen addresses require
`network.allow_remote: true` (or `DB_ALLOW_REMOTE=true`) to bind outside literal loopback IPs. Wildcard addresses,
private addresses, and hostnames, including `localhost`, require the opt-in. Use `127.0.0.1` or `[::1]` for local access.
The opt-in is an operator acknowledgement, not authentication or a source-IP restriction. A standby's optional
replication listen address is checked at startup too, before it can be used on promotion.

Restrict both ports with host firewalls and network isolation. Do not expose them to the internet or untrusted tenants.
In Docker, publishing only on host loopback (`-p 127.0.0.1:3223:3223`) restricts host publication; other containers on the
same network can still reach a listener bound to all container interfaces. The [Docker quickstart](README.md#with-docker)
shows the explicit container opt-in. Without it, the default listener is reachable only within the container's network
namespace, even when a port is published.

## Replication and promotion

Replication is a preview and failover is manual. `PROMOTE` over the client port is disabled by default, including for
loopback clients. Enabling `replication.allow_remote_promote` is an unsafe operator action: every reachable client can
then promote that standby. Isolate the old master before promotion and restrict access to the selected node. Neither
this flag nor `network.allow_remote` supplies administrator authorization or automatic fencing.

The replication master caps concurrent connections, including handshakes, at `replication.max_connections` (default
100). Excess connections are closed. `replication.handshake_timeout` (default 10s) bounds the entire incoming handshake,
so trickling bytes cannot extend it. It also bounds standby dialing and handshake writes.

`replication.idle_timeout` (default 1m) bounds each socket read/write during streaming, including snapshots. Progress
renews these deadlines so large snapshots can transfer longer than the timeout. A blocked master write drops that
standby; a standby reconnects after a stalled or silent master. Startup rejects standby timeouts of 1s or less, matching
the maximum master heartbeat interval. Leave margin for scheduling and network delays; the 1m default is recommended.
These are transport liveness checks: the protocol has no acknowledgement of standby application,
and successful writes into TCP buffers do not prove a peer has applied data. Connection bounds are not rate limits or
protection against an attacker on a trusted network.

## Logs and stored data

At info level, command logs contain the validated command name, outcome, duration, and table/key byte lengths. Rejected
commands use a fixed `invalid` label. Full arguments, parse errors, and execution-error details are debug-only. Debug
logging can expose keys, values, and secrets, and is unsafe for production. Restrict access to existing debug logs and
delete or rotate them according to your data-retention policy.

WAL, snapshot, and tiered data files are not encrypted. Their checksums detect accidental damage and do not authenticate
data. Restrict access to data directories, backups, and logs using operating-system permissions.

## Reporting vulnerabilities

Use the repository's [private vulnerability reporting page](https://github.com/OutOfStack/db/security/advisories/new)
when available. If private reporting is unavailable, open an issue requesting a private contact channel without
including exploit details or sensitive data. Include affected versions, reproduction steps, impact, and any proposed
mitigation in the private report. Do not send production data or credentials.

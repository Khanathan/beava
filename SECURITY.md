# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | yes       |

Only the latest release on `main` receives security fixes during the pre-1.0 phase.

## Reporting a Vulnerability

Please email **security@beava.dev** with details of the issue. Do NOT open a public
GitHub issue for security vulnerabilities.

You can also file a private security advisory at
https://github.com/petrpan26/beava/security/advisories/new

Include in your report:
- A description of the vulnerability
- Steps to reproduce
- The version (commit hash) affected
- Any potential impact

We aim to acknowledge reports within 72 hours and ship a fix within 14 days for
confirmed issues. We will credit reporters by name on request.

## Scope

Beava is designed to run behind a trusted boundary. By default:

- The TCP protocol has no authentication (bind to localhost or a private VPC in production).
- The HTTP management API uses an optional admin token (`BEAVA_ADMIN_TOKEN`).
- There is no encryption at rest or in transit (terminate TLS at a reverse proxy if needed).

Deployments exposing Beava directly to untrusted networks are out of scope for security
reports. Deployments following the documented configuration (localhost / private network /
reverse-proxy TLS termination) are in scope.

## Contact

Security: security@beava.dev (monitored by Hoang Phan, sole maintainer)
General: phan.minhhoang2606@gmail.com

# Security Policy

## Reporting a Vulnerability

If you discover a security issue in Beava, please email the maintainers directly rather than opening a public issue.

**Contact:** open a private security advisory at https://github.com/petrpan26/beava/security/advisories/new

Include:
- A description of the vulnerability
- Steps to reproduce
- The version (commit hash) affected
- Any potential impact

We will respond within 7 days and work with you on a coordinated disclosure timeline.

## Scope

Beava is designed to run behind a trusted boundary. By default:

- The TCP protocol has no authentication (bind to localhost or a private VPC in production)
- The HTTP management API has no authentication (bind to localhost in production)
- There is no encryption at rest or in transit (terminate TLS at a reverse proxy if needed)

Deployments exposing Beava directly to untrusted networks are out of scope for security reports. Deployments following the documented configuration (localhost / private network) are in scope.

## Supported Versions

During the pre-1.0 phase, only the latest release on `main` receives security fixes.

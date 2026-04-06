# Security policy

## Supported versions

We currently support only the latest tagged pre-1.0 release line of `bobrapet`.

| Version line | Status | Notes |
| --- | --- | --- |
| Latest tagged `0.x` release | Supported | Security fixes land here first while the project is pre-1.0. |
| Older tagged releases | Unsupported | Upgrade to the newest tagged release before requesting a security fix. |
| Unreleased commits on `main` or feature branches | Unsupported | Security fixes are released through tagged versions, not guaranteed on arbitrary commits. |

We aim to support Kubernetes N-2 relative to upstream stable releases, but this repository does not yet publish a separate compatibility matrix. Treat the latest release notes and the currently tested CI workflows as the source of truth for supported environments.

We do not currently publish a PGP key for vulnerability intake. Use the GitHub Security Advisory flow below so reports stay private.

## Reporting a vulnerability

The BubuStack Team and community take all security vulnerabilities seriously. We appreciate your efforts to responsibly disclose your findings, and will make every effort to acknowledge your contributions.

To report a security vulnerability, please use the GitHub Security Advisory feature in this repository:

- https://github.com/bubustack/bobrapet/security/advisories/new

**Please do not report security vulnerabilities through public GitHub issues.**

When reporting a vulnerability, please provide the following information:

- **A clear description** of the vulnerability and its potential impact.
- **Steps to reproduce** the vulnerability, including any example code, scripts, or configurations.
- **The version(s) of the operator** affected.
- **Your contact information** for us to follow up with you.

## Disclosure process

1.  **Report**: You report the vulnerability through the GitHub Security Advisory feature.
2.  **Confirmation**: We will acknowledge your report within 48 hours.
3.  **Investigation**: We will investigate the vulnerability and determine its scope and impact. We may contact you for additional information during this phase.
4.  **Fix**: We will develop a patch for the vulnerability.
5.  **Disclosure**: We will create a security advisory, issue a CVE (if applicable), and release a new version with the patch. We will credit you for your discovery unless you prefer to remain anonymous.

We aim to resolve high severity vulnerabilities within 30 days, medium within
60 days, and low within 90 days, subject to complexity and scope. We'll keep
you informed of progress.


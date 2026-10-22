# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Performance Monitor, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please email **erik@erikdarling.com** with:

- A description of the vulnerability
- Steps to reproduce the issue
- The potential impact
- Any suggested fixes (optional)

You should receive a response within 72 hours. We will work with you to understand the issue and coordinate a fix before any public disclosure.

## Scope

This policy applies to:

- Lite, the desktop app, and its embedded MCP server
- Darling: the service, the viewer, the MCP server, and the web dashboard
- The shared libraries that Lite and Darling use
- The deprecated Full Edition, which is still supported for existing users
  - The SQL collection and reporting stored procedures
  - The SQL installation scripts in `install/`
  - The Full Dashboard and the CLI installer in `deprecated/`

## Security Best Practices

When using Performance Monitor:

- Use Windows Authentication where possible
- Use dedicated service accounts with minimal required permissions
- Enable encryption for SQL Server connections
- Keep your SQL Server instances patched and up to date
- Review the [permissions](docs/permissions.md) and [authentication](docs/authentication.md) documentation for recommended security configurations

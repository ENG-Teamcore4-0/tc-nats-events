# Security Policy

## Supported Versions

We currently support the following versions with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security vulnerability in TC NATS Events, please report it responsibly.

### How to Report

**Do NOT create a public GitHub issue for security vulnerabilities.**

Instead, please:

1. **Email**: Send details to security@teamcore.net
2. **Subject Line**: Include "[SECURITY] TC NATS Events" in the subject
3. **Include**:
   - Description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact
   - Suggested fix (if any)

### What to Expect

- **Acknowledgment**: We'll acknowledge receipt within 48 hours
- **Investigation**: We'll investigate and assess the issue within 7 days
- **Updates**: We'll keep you informed of our progress
- **Resolution**: We aim to release security patches within 30 days
- **Credit**: We'll credit you in the security advisory (unless you prefer to remain anonymous)

### Security Best Practices

When using TC NATS Events:

#### NATS Server Security
- **Authentication**: Always use authentication in production
- **TLS**: Enable TLS encryption for all connections
- **Network**: Restrict network access to NATS servers
- **Updates**: Keep NATS server updated to the latest version

#### Application Security
- **Input Validation**: Validate all event data before processing
- **Error Handling**: Don't expose sensitive information in error messages
- **Logging**: Be careful not to log sensitive data
- **Dependencies**: Keep dependencies updated

#### Configuration Security
- **Secrets**: Never commit secrets to version control
- **Environment Variables**: Use environment variables for sensitive config
- **Access Control**: Implement proper access controls for event streams
- **Monitoring**: Monitor for unusual event patterns

### Example Secure Configuration

```python
from tc_nats_events import NATSConfig

# Secure production configuration
config = NATSConfig(
    servers=["nats://nats-server:4222"],
    user=os.getenv("NATS_USER"),  # From environment
    password=os.getenv("NATS_PASSWORD"),  # From environment
    stream_name="production-events",
    # Enable TLS in production
    # tls_config=...
)
```

### Security Considerations for Events

#### Event Data
- **PII**: Avoid including Personally Identifiable Information in events
- **Credentials**: Never include passwords, tokens, or keys in event data
- **Sanitization**: Sanitize user input before including in events

#### Event Types
- **Validation**: Validate event types to prevent injection
- **Whitelisting**: Consider whitelisting allowed event types

#### Handlers
- **Input Validation**: Always validate event data in handlers
- **Error Handling**: Handle exceptions gracefully
- **Rate Limiting**: Implement rate limiting for event processing

### Vulnerability Types We Monitor

- Authentication bypass
- Authorization issues
- Data injection attacks
- Denial of Service (DoS)
- Information disclosure
- Remote code execution
- NATS-specific vulnerabilities
- Dependency vulnerabilities

### Responsible Disclosure Timeline

1. **Day 0**: Vulnerability reported
2. **Day 1-2**: Acknowledgment sent
3. **Day 3-7**: Initial assessment
4. **Day 8-21**: Investigation and fix development
5. **Day 22-30**: Testing and release preparation
6. **Day 30**: Public disclosure and patch release

### Hall of Fame

We'll recognize security researchers who help improve TC NATS Events security:

<!-- Future security researchers will be listed here -->

Thank you for helping keep TC NATS Events and our users safe!
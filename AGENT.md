## Documentation Style
When creating any documentation (README files, code comments, design docs), write in the style of an Amazon engineer:
- Start with the customer problem and work backwards
- Use clear, concise, and data-driven language
- Include specific examples and concrete details
- Structure documents with clear headings and bullet points
- Focus on operational excellence, security, and scalability considerations
- Always include implementation details and edge cases
- Use the passive voice sparingly; prefer active, direct statements

## Commit Message Style
Use conventional commits format:
- Start with a type: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, etc.
- Follow with a concise summary line (imperative mood)
- Add bullet points for specific changes when multiple areas are affected
- Keep the summary line under 72 characters
- Group related changes logically in the bullet points

Example:
```
feat: add npm registry support and TTL/LRU cache expiration

- Add npm protocol handler with tarball caching and integrity verification
- Implement expiry system with TTL and LRU eviction policies
- Fix golangci-lint errors across codebase
```
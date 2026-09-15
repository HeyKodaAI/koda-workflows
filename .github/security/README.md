# Repository security checks

- Gitleaks scans full Git history, including generated assets, lockfiles and source maps. Scanner failures and zero-commit scans block the check. Reports redact credential values.
- Semgrep Community Edition checks authored source for high-severity findings using 289 security-category ERROR rules from the Semgrep default ruleset, reviewed on 2026-09-15. It uses no paid license or account token. Review and refresh the pinned tool and rule snapshot periodically. Reports contain metadata only. Parser warnings remain explicit coverage limitations; fatal scanner errors and zero-file scans fail. Generated bundles and scanner rule definitions are omitted from static source analysis; Gitleaks still scans them.
- Reviewed false positives use an exact rule, file, line, and whole-file SHA-256 in `code-exceptions.json`. Any file change invalidates the exception. Active credentials must not be excepted.
- Dependabot alerts report known vulnerable dependencies. Fix the package and lockfile together and run the project's required validation.
- Do not commit real `.env`, `.dev.vars`, `.secrets`, credentials, or passwords. Put runtime values in the appropriate provider secret store.
- Review false positives individually. Exact public identifier exceptions need a reason. Never suppress an active credential to obtain a green check; replace it and remove current copies first.
- A passing scanner is evidence for its scope, not a guarantee that all code or deployed systems are safe.

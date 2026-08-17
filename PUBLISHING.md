# Publishing

Registry: **Maven Central**. Canonical version: **0.1.1**.

The release workflow is tag-bound, verifies conformance and version alignment, and uses a protected GitHub environment. Publishing is considered successful only after the official registry resolves the exact version; a GitHub tag or release is not sufficient.

External one-time setup: Verify the ai.customy namespace in Central Portal and configure the maven-central environment with MAVEN_CENTRAL_USERNAME, MAVEN_CENTRAL_TOKEN, MAVEN_GPG_PRIVATE_KEY and MAVEN_GPG_PASSPHRASE.

After the registry-side setup is complete, dispatch `.github/workflows/publish.yml` against the immutable `v0.1.1` tag when the registry supports manual dispatch. pub.dev requires a new matching tag push. Never replace a published version or bypass a failed conformance job.

# Skip-arm probe (throwaway)

Validation artifact for the acceptance criteria of #3116: a pull request whose only change is a file
matching `Darling/**/*.md` must still leave `Darling PostgreSQL tests` skipping `Run Darling PG tests`.

This branch is based on the fix branch, so the gate being exercised is the shared gate at
`.github/darling-paths-filter.yml` rather than the one on `dev`. The pull request that carries this file
is closed and its branch deleted once the job's step list has been read.

# Skip-arm probe (throwaway)

Validation artifact for the acceptance criteria of #3116: a pull request whose only change matches
`Darling/**/*.md` must still leave `Darling PostgreSQL tests` skipping `Run Darling PG tests`.

Branched off the fix so the gate being exercised is the shared gate at
`.github/darling-paths-filter.yml`. Deleted once the job's step list has been read.

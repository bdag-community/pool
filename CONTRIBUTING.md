# Contributing to the BDAG Community mining pool

Fork this repository, start your work from `develop`, and open a pull request to `develop`. Describe what changed, why it matters, and how you verified it. With Go 1.26.9, run `go build -mod=readonly ./...`. For `go test -mod=readonly -p 1 ./...`, set `POOL_SUBMISSION_JOURNAL_TEST_DATABASE_URL` to a disposable local PostgreSQL database with a name beginning `pool_submission_journal_test_`. Use an account that can create databases in that test instance. Tests recreate schemas and need exclusive access to that instance.

Francois (`@franco-is`) and Jeremy (`@strattice`) maintain the protected branches. Either can approve an outside contribution. When one maintainer authors a pull request, the other reviews it. Maintainers promote reviewed `develop` changes through a separate pull request to `main`.

Preserve licence notices and confirm that you can contribute any code or artwork under its applicable terms. Use [SECURITY.md](SECURITY.md) for private security reports.

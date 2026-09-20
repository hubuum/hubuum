# Backup task failure

## Meaning

At least one backup task failed in the last hour. The alert clears when that observation leaves the window, not necessarily when a backup succeeds.

## Diagnose

Inspect the authorized backup task result, configured size limits, database capacity and output storage. Establish the age of the last independently verified recoverable backup.

## Recover

Correct the cause, create a new backup and verify it through an isolated restore. This alert cannot prove that scheduled backups ran, that an artifact is intact, or that recovery works.

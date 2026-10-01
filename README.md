# PBA Newsletter Archive

Archive of all campaign content sent by Philly Bike Action.

## MailChimp Archive

Historical campaigns from when PBA used MailChimp are included in this repository.

## MailJet Sync

To sync new campaigns from MailJet, create a `.env` file:

```bash
cp .env.example .env
# Edit .env with your credentials
```

Then run:

```bash
./sync_mailjet.py
```

Or pass credentials directly:

```bash
./sync_mailjet.py --api-key YOUR_KEY --api-secret YOUR_SECRET
```

### Options

- `--dry-run`: Show what would be downloaded without making changes
- `--force`: Re-download all campaigns, even if already archived

The script is idempotent and can be run repeatedly to fetch new campaigns.

### Ignoring campaigns

Add MailJet campaign draft IDs to `ignored_campaign_ids.txt`, one per line.
Blank lines and `#` comments are supported:

```text
15112258  # Test Organizer Blast
```

Use the ID printed during sync or the numeric prefix of an archived filename
(for example, `15112258_test-organizer-blast.html`). The committed list applies
to local runs and the scheduled GitHub Actions sync, including `--force` and
`--dry-run`. Ignored campaigns are skipped before fetching their content or assets.
Invalid entries stop the sync with a line-numbered error.

Adding an ID prevents future downloads; it does not remove existing archive files
or index entries. If a campaign was already merged into the archive, also remove
its HTML file and its row from `archive_index.csv`, then regenerate the index.

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

import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import sync_mailjet as sync


class IgnoreCampaignTests(unittest.TestCase):
    def test_ignore_file_comments_duplicates_and_invalid_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ignored_campaign_ids.txt'
            with patch.object(sync, 'IGNORED_CAMPAIGN_IDS_FILE', path):
                path.write_text('# Tests\n\n15112258 # Test blast\n15112258\n42\n')
                self.assertEqual(sync.load_ignored_campaign_ids(), {15112258, 42})
                for value in ('abc', '0', '-1', '12,34'):
                    path.write_text(f'# Tests\n{value}\n')
                    with self.assertRaisesRegex(ValueError, ':2: expected a positive campaign ID'):
                        sync.load_ignored_campaign_ids()

    def test_sync_skips_ignored_content_even_with_force(self):
        for options in ([], ['--force'], ['--dry-run'], ['--force', '--dry-run']):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / 'archive'
                archive.mkdir()
                (archive / '42_existing.html').write_text('existing')
                campaigns = [
                    {'ID': campaign_id, 'Subject': f'Campaign {campaign_id}',
                     'DeliveredAt': '2026-10-01T12:00:00Z'}
                    for campaign_id in (15112258, 42, 43)
                ]
                with (
                    patch.object(sync, 'SCRIPT_DIR', root),
                    patch.object(sync, 'ARCHIVE_DIR', archive),
                    patch.object(sync, 'ASSETS_DIR', archive / 'assets'),
                    patch.object(sync, 'load_ignored_campaign_ids', return_value={15112258}),
                    patch.object(sync, 'MailJetClient') as client_class,
                    patch('sys.argv', ['sync_mailjet.py', '--api-key', 'fake', '--api-secret', 'fake', *options]),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    client = client_class.return_value
                    client.get_sent_campaigns.return_value = campaigns
                    client.get_campaign_content.return_value = {'Html-part': '<p>Newsletter</p>'}
                    sync.main()

                fetched_ids = [call.args[0] for call in client.get_campaign_content.call_args_list]
                self.assertEqual(fetched_ids, [42, 43] if '--force' in options else [43])
                if '--dry-run' in options:
                    self.assertEqual(list(archive.iterdir()), [archive / '42_existing.html'])
                    self.assertFalse((root / 'archive_index.csv').exists())
                else:
                    self.assertNotIn('15112258', (root / 'archive_index.csv').read_text())
                    self.assertNotIn('15112258', (archive / 'index.html').read_text())

    def test_all_ignored_leaves_archive_untouched(self):
        with (
            patch.object(sync, 'load_ignored_campaign_ids', return_value={15112258}),
            patch.object(sync, 'get_existing_campaign_ids', return_value=set()),
            patch.object(sync, 'MailJetClient') as client_class,
            patch.object(sync, 'generate_index_html') as generate_index,
            patch('sys.argv', ['sync_mailjet.py', '--api-key', 'fake', '--api-secret', 'fake']),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            client = client_class.return_value
            client.get_sent_campaigns.return_value = [{'ID': 15112258}]
            sync.main()
            client.get_campaign_content.assert_not_called()
            generate_index.assert_not_called()


if __name__ == '__main__':
    unittest.main()

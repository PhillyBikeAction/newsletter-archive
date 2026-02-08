#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests>=2.28.0",
#     "python-dotenv>=1.0.0",
#     "beautifulsoup4>=4.12.0",
# ]
# ///
"""
MailJet Campaign Archiver

Fetches all sent campaigns from MailJet and archives them as HTML files.
Runs idempotently - only downloads campaigns that haven't been archived yet.

Usage:
    export MJ_APIKEY_PUBLIC="your-api-key"
    export MJ_APIKEY_PRIVATE="your-secret-key"
    python sync_mailjet.py

Or pass keys directly:
    python sync_mailjet.py --api-key YOUR_KEY --api-secret YOUR_SECRET
"""

import argparse
import csv
import hashlib
import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests

# Load .env file if it exists
load_dotenv()


BASE_URL = "https://api.mailjet.com/v3/REST/"
SCRIPT_DIR = Path(__file__).parent.resolve()
ARCHIVE_DIR = SCRIPT_DIR / "archive"
ASSETS_DIR = ARCHIVE_DIR / "assets"

# Domains that contain assets we should mirror (images, etc.)
ASSET_DOMAINS = {
    'mjt.lu',           # MailJet CDN
    'mailjet.com',      # MailJet static assets
}


def should_mirror_url(url: str) -> bool:
    """Check if a URL should be mirrored as a local asset."""
    try:
        parsed = urlparse(url)
        if not parsed.scheme.startswith('http'):
            return False
        # Check if domain matches any asset domain
        for domain in ASSET_DOMAINS:
            if domain in parsed.netloc:
                return True
        return False
    except Exception:
        return False


def get_asset_filename(url: str) -> str:
    """Generate a unique filename for an asset URL."""
    # Create a hash of the URL for uniqueness
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]

    # Try to extract a meaningful extension
    parsed = urlparse(url)
    path = parsed.path

    # Handle MailJet CDN URLs like /img2/skqro/.../content
    if '/content' in path or not '.' in path.split('/')[-1]:
        # No clear extension, default to common image types
        ext = '.png'
    else:
        ext = Path(path).suffix.lower()
        if not ext or len(ext) > 5:
            ext = '.png'

    return f"{url_hash}{ext}"


def download_asset(url: str, session: requests.Session = None) -> bytes | None:
    """Download an asset from a URL."""
    try:
        sess = session or requests.Session()
        response = sess.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"    Warning: Failed to download {url}: {e}")
        return None


def clean_html(html: str) -> str:
    """
    Remove unnecessary elements from archived HTML:
    - "View online version" links with [[PERMALINK]]
    - Unsubscribe links with [[UNSUB_LINK_EN]]
    - Replace [[EMAIL_TO]] with generic text
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Remove links with placeholder hrefs
    placeholder_patterns = ['[[PERMALINK]]', '[[UNSUB_LINK_EN]]']
    for pattern in placeholder_patterns:
        for a in soup.find_all('a', href=pattern):
            # Try to remove the parent paragraph/div if it only contains this link
            parent = a.parent
            if parent and parent.name in ('p', 'div', 'td', 'span'):
                # Check if parent has only whitespace and this link
                other_content = parent.get_text(strip=True).replace(a.get_text(strip=True), '').strip()
                if not other_content:
                    # Remove the whole parent element
                    parent.decompose()
                    continue
            # Otherwise just remove the link
            a.decompose()

    # Replace [[EMAIL_TO]] with empty string or remove elements containing only it
    for text in soup.find_all(string=re.compile(r'\[\[EMAIL_TO\]\]')):
        new_text = text.replace('[[EMAIL_TO]]', '')
        if new_text.strip():
            text.replace_with(new_text)
        else:
            # If the element is now empty, try to remove parent
            parent = text.parent
            if parent and not parent.get_text(strip=True):
                parent.decompose()

    # Remove link tags with href="null" (broken font/stylesheet references)
    for link in soup.find_all('link', href='null'):
        link.decompose()

    # Remove @import url(null); from style tags
    for style in soup.find_all('style'):
        if style.string:
            cleaned = re.sub(r'@import\s+url\(null\);?', '', style.string)
            if cleaned != style.string:
                style.string = cleaned

    return str(soup)


def mirror_assets(html: str, dry_run: bool = False) -> tuple[str, int]:
    """
    Find all assets in HTML, download them, and rewrite URLs to local paths.
    Returns (modified_html, asset_count).
    """
    soup = BeautifulSoup(html, 'html.parser')
    assets_downloaded = 0
    session = requests.Session()

    # Ensure archive and assets directories exist
    if not dry_run:
        ASSETS_DIR.mkdir(parents=True, exist_ok=True)

    # Find all elements with src attribute (images, etc.)
    for tag in soup.find_all(src=True):
        url = tag['src']
        if should_mirror_url(url):
            filename = get_asset_filename(url)
            local_path = ASSETS_DIR / filename

            if not dry_run:
                # Download if not already cached
                if not local_path.exists():
                    content = download_asset(url, session)
                    if content:
                        local_path.write_bytes(content)
                        assets_downloaded += 1

                # Update HTML reference to local path
                if local_path.exists():
                    tag['src'] = f"assets/{filename}"
            else:
                assets_downloaded += 1

    # Find all elements with background-image in style
    for tag in soup.find_all(style=True):
        style = tag['style']
        urls = re.findall(r'url\(["\']?(https?://[^"\')\s]+)["\']?\)', style)
        for url in urls:
            if should_mirror_url(url):
                filename = get_asset_filename(url)
                local_path = ASSETS_DIR / filename

                if not dry_run:
                    if not local_path.exists():
                        content = download_asset(url, session)
                        if content:
                            local_path.write_bytes(content)
                            assets_downloaded += 1

                    if local_path.exists():
                        tag['style'] = style.replace(url, f"assets/{filename}")
                        style = tag['style']  # Update for next iteration
                else:
                    assets_downloaded += 1

    return str(soup), assets_downloaded


def slugify(text: str) -> str:
    """Convert text to a URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    text = text.strip('-')
    return text


def get_existing_campaign_ids() -> set:
    """Get set of campaign IDs already archived in the directory."""
    existing_ids = set()
    for html_file in ARCHIVE_DIR.glob("*.html"):
        # Extract ID from filename pattern: {id}_{slug}.html
        match = re.match(r'^(\d+)_', html_file.name)
        if match:
            existing_ids.add(int(match.group(1)))
    return existing_ids


def update_archive_index(filename: str, subject: str, date_sent: str):
    """Add or update entry in archive_index.csv."""
    from datetime import datetime

    csv_path = SCRIPT_DIR / 'archive_index.csv'

    # Convert ISO format date to CSV format if needed
    # MailJet: "2024-01-15T09:00:00Z" -> CSV: "Jan 15, 2024 09:00 am"
    try:
        if 'T' in date_sent:
            # Parse and convert to local/naive datetime
            dt = datetime.fromisoformat(date_sent.replace('Z', '+00:00'))
            # Remove timezone info to make it naive
            dt = dt.replace(tzinfo=None)
            # Format as CSV expects (e.g., "Jan 15, 2024 09:00 am")
            # Note: %p gives uppercase, so convert AM/PM to lowercase
            formatted = dt.strftime('%b %d, %Y %I:%M %p')
            date_sent = formatted.replace(' AM', ' am').replace(' PM', ' pm')
    except Exception as e:
        print(f"  Warning: Could not parse date '{date_sent}': {e}")

    # Read existing entries
    existing_entries = {}
    if csv_path.exists():
        with open(csv_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_entries[row['filename']] = row

    # Add or update this entry
    existing_entries[filename] = {
        'date_sent': date_sent,
        'subject': subject,
        'filename': filename
    }

    # Sort by date
    def parse_date(entry):
        try:
            # Parse campaigns.csv format: "Jun 01, 2023 09:46 pm"
            # All datetimes should be naive (no timezone) for comparison
            return datetime.strptime(entry['date_sent'], '%b %d, %Y %I:%M %p')
        except Exception as e:
            # If parsing fails, return min datetime
            return datetime.min

    sorted_entries = sorted(existing_entries.values(), key=parse_date)

    # Write back to CSV
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['date_sent', 'subject', 'filename'])
        writer.writeheader()
        writer.writerows(sorted_entries)

    print(f"  Updated archive_index.csv")


def generate_index_html():
    """Generate index.html from archive_index.csv."""
    from html import escape

    csv_path = SCRIPT_DIR / 'archive_index.csv'
    if not csv_path.exists():
        print("Warning: archive_index.csv not found, skipping index.html generation")
        return

    # Read all newsletters from CSV
    newsletters = []
    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        newsletters = list(reader)

    # Generate HTML
    html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Philly Bike Action Newsletter Archive</title>
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }

        .container {
            max-width: 900px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-radius: 8px;
        }

        h1 {
            color: #007C89;
            margin-bottom: 10px;
            font-size: 2.5em;
        }

        .subtitle {
            color: #666;
            margin-bottom: 40px;
            font-size: 1.1em;
        }

        .newsletter {
            border-left: 4px solid #007C89;
            padding: 20px;
            margin-bottom: 30px;
            background: #fafafa;
            border-radius: 4px;
        }

        .newsletter-header {
            margin-bottom: 15px;
        }

        .newsletter-subject {
            font-size: 1.3em;
            font-weight: 600;
            color: #222;
            margin-bottom: 6px;
        }

        .newsletter-subject a {
            color: #007C89;
            text-decoration: none;
        }

        .newsletter-subject a:hover {
            text-decoration: underline;
        }

        .newsletter-date {
            color: #666;
            font-size: 0.9em;
            font-weight: 500;
        }

        .newsletter-preview {
            width: 100%;
            height: 300px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
        }

        .count {
            text-align: center;
            color: #999;
            margin-top: 40px;
            font-size: 0.9em;
        }

        @media (max-width: 600px) {
            .container {
                padding: 20px;
            }

            h1 {
                font-size: 2em;
            }

            .newsletter-preview {
                height: 250px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Philly Bike Action Newsletter Archive</h1>
        <p class="subtitle">Complete archive of email campaigns from June 2023 to present</p>

'''

    # Add each newsletter
    for newsletter in newsletters:
        filename = escape(newsletter['filename'])
        subject = escape(newsletter['subject'])
        date = escape(newsletter['date_sent'])

        html += f'''        <div class="newsletter">
            <div class="newsletter-header">
                <div class="newsletter-subject">
                    <a href="{filename}" target="_blank">{subject}</a>
                </div>
                <div class="newsletter-date">{date}</div>
            </div>
            <iframe src="{filename}" class="newsletter-preview" sandbox="allow-same-origin"></iframe>
        </div>

'''

    html += f'''        <div class="count">
            {len(newsletters)} newsletters archived
        </div>
    </div>
</body>
</html>
'''

    # Write to archive directory
    index_path = ARCHIVE_DIR / 'index.html'
    index_path.write_text(html, encoding='utf-8')
    print(f"Generated {index_path} with {len(newsletters)} newsletters")


class MailJetClient:
    def __init__(self, api_key: str, api_secret: str):
        self.auth = (api_key, api_secret)
        self.session = requests.Session()
        self.session.auth = self.auth

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make a GET request to the MailJet API."""
        url = urljoin(BASE_URL, endpoint)
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _get_all_pages(self, endpoint: str, params: dict = None) -> list:
        """Fetch all pages of results from a paginated endpoint."""
        if params is None:
            params = {}

        all_data = []
        offset = 0
        limit = 1000  # Max allowed by MailJet

        while True:
            params['Limit'] = limit
            params['Offset'] = offset

            result = self._get(endpoint, params)
            data = result.get('Data', [])

            if not data:
                break

            all_data.extend(data)

            # Check if there are more pages
            if len(data) < limit:
                break

            offset += limit

        return all_data

    def get_sent_campaigns(self) -> list:
        """Fetch all sent campaigns (campaigndrafts with status 2 or 3)."""
        # Get all campaign drafts that have been sent
        # Status 2 = sent, Status 3 = A/X testing
        campaigns = self._get_all_pages('campaigndraft', {
            'Status': 2,  # Sent
        })

        # Also fetch status 3 (A/X testing - also sent)
        campaigns_ax = self._get_all_pages('campaigndraft', {
            'Status': 3,
        })

        return campaigns + campaigns_ax

    def get_campaign_content(self, campaign_id: int) -> dict:
        """Fetch the HTML content of a specific campaign draft."""
        try:
            result = self._get(f'campaigndraft/{campaign_id}/detailcontent')
            data = result.get('Data', [])
            if data:
                return data[0]
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        return None


def archive_campaign(client: MailJetClient, campaign: dict, dry_run: bool = False) -> bool:
    """Archive a single campaign to an HTML file."""
    campaign_id = campaign.get('ID')
    title = campaign.get('Title', '') or campaign.get('Subject', '') or f'campaign-{campaign_id}'
    subject = campaign.get('Subject', '')
    sent_at = campaign.get('DeliveredAt') or campaign.get('SendStartAt', '')

    # Get the HTML content
    content = client.get_campaign_content(campaign_id)
    if not content:
        print(f"  Warning: No content found for campaign {campaign_id}")
        return False

    html_part = content.get('Html-part', '')
    text_part = content.get('Text-part', '')

    if not html_part and not text_part:
        print(f"  Warning: Empty content for campaign {campaign_id}")
        return False

    # Generate filename
    slug = slugify(title)
    if not slug:
        slug = f'campaign-{campaign_id}'
    filename = f"{campaign_id}_{slug}.html"
    filepath = ARCHIVE_DIR / filename

    # Create HTML file with content
    # If there's HTML content, use it; otherwise wrap text in basic HTML
    if html_part:
        final_html = html_part
    else:
        # Wrap plain text in a basic HTML structure
        escaped_text = text_part.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        final_html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{subject}</title>
</head>
<body>
<pre style="white-space: pre-wrap; max-width: 40em; font-family: sans-serif;">
{escaped_text}
</pre>
</body>
</html>'''

    # Clean up unnecessary elements (view online, unsubscribe, etc.)
    final_html = clean_html(final_html)

    # Mirror external assets and rewrite URLs
    final_html, asset_count = mirror_assets(final_html, dry_run)

    if dry_run:
        print(f"  Would create: {filename} ({asset_count} assets)")
        return True

    filepath.write_text(final_html, encoding='utf-8')
    print(f"  Created: {filename} ({asset_count} assets mirrored)")

    # Update archive index CSV
    update_archive_index(filename, subject, sent_at)

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Archive MailJet campaigns to HTML files'
    )
    parser.add_argument(
        '--api-key',
        default=os.environ.get('MJ_APIKEY_PUBLIC'),
        help='MailJet API public key (or set MJ_APIKEY_PUBLIC env var)'
    )
    parser.add_argument(
        '--api-secret',
        default=os.environ.get('MJ_APIKEY_PRIVATE'),
        help='MailJet API private key (or set MJ_APIKEY_PRIVATE env var)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-download all campaigns, even if already archived'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Show debug info about what the API returns'
    )

    args = parser.parse_args()

    if not args.api_key or not args.api_secret:
        print("Error: MailJet API credentials required.")
        print("Set MJ_APIKEY_PUBLIC and MJ_APIKEY_PRIVATE environment variables,")
        print("or use --api-key and --api-secret arguments.")
        sys.exit(1)

    client = MailJetClient(args.api_key, args.api_secret)

    if args.debug:
        print("=== DEBUG: Checking API endpoints ===\n")

        # Check account info
        try:
            result = client._get('myprofile')
            data = result.get('Data', [])
            if data:
                profile = data[0]
                print(f"Connected to account: {profile.get('Email')}")
                print(f"  Company: {profile.get('CompanyName')}")
                print()
        except Exception as e:
            print(f"Profile: Error - {e}\n")

        # Check contact lists
        try:
            result = client._get('contactslist', {'Limit': 5})
            data = result.get('Data', [])
            print(f"Contact lists: {len(data)} found")
            for cl in data[:3]:
                print(f"  - ID={cl.get('ID')}, Name={cl.get('Name')}, SubscriberCount={cl.get('SubscriberCount')}")
            print()
        except Exception as e:
            print(f"Contact lists: Error - {e}\n")

        # Check API key permissions
        try:
            result = client._get('apikey')
            data = result.get('Data', [])
            if data:
                key = data[0]
                print(f"API Key: {key.get('Name')}")
                print(f"  IsActive: {key.get('IsActive')}, IsMaster: {key.get('IsMaster')}")
                print()
        except Exception as e:
            print(f"API key: Error - {e}\n")

        # Check messages sent
        try:
            result = client._get('message', {'Limit': 5})
            data = result.get('Data', [])
            count = result.get('Total', len(data))
            print(f"Messages: {count} total")
            for m in data[:3]:
                print(f"  - ID={m.get('ID')}, Status={m.get('Status')}, Subject={m.get('Subject')}")
            print()
        except Exception as e:
            print(f"Messages: Error - {e}\n")

        # Check sender addresses
        try:
            result = client._get('sender', {'Limit': 5})
            data = result.get('Data', [])
            print(f"Senders: {len(data)} found")
            for s in data[:3]:
                print(f"  - {s.get('Email')} (Status: {s.get('Status')})")
            print()
        except Exception as e:
            print(f"Senders: Error - {e}\n")

        # Check newsletters with various statuses
        for status in [0, 1, 2, 3, -1, -2]:
            status_names = {-2: 'deleted', -1: 'archived', 0: 'draft', 1: 'programmed', 2: 'sent', 3: 'A/X testing'}
            try:
                result = client._get('newsletter', {'Status': status, 'Limit': 5})
                data = result.get('Data', [])
                print(f"Newsletter status={status} ({status_names.get(status, '?')}): {len(data)} found")
                if data:
                    for n in data[:2]:
                        print(f"  - ID={n.get('ID')}, Title={n.get('Title')}, Status={n.get('Status')}")
            except Exception as e:
                print(f"Newsletter status={status}: Error - {e}")

        print()

        # Check campaigns endpoint
        try:
            result = client._get('campaign', {'Limit': 5})
            data = result.get('Data', [])
            print(f"Campaigns: {len(data)} found")
            if data:
                for c in data[:3]:
                    print(f"  - ID={c.get('ID')}, Subject={c.get('Subject')}, IsStarred={c.get('IsStarred')}")
        except Exception as e:
            print(f"Campaigns: Error - {e}")

        print()

        # Check campaigndraft endpoint
        try:
            result = client._get('campaigndraft', {'Limit': 5})
            data = result.get('Data', [])
            print(f"Campaign drafts: {len(data)} found")
            if data:
                for d in data[:3]:
                    print(f"  - ID={d.get('ID')}, Title={d.get('Title')}, Status={d.get('Status')}")
        except Exception as e:
            print(f"Campaign drafts: Error - {e}")

        print("\n=== END DEBUG ===\n")

    # Get existing archived campaign IDs
    existing_ids = get_existing_campaign_ids() if not args.force else set()
    print(f"Found {len(existing_ids)} existing archived campaigns")

    # Fetch all sent campaigns
    print("Fetching sent campaigns from MailJet...")
    campaigns = client.get_sent_campaigns()
    print(f"Found {len(campaigns)} sent campaigns")

    # Filter to only new campaigns
    new_campaigns = [c for c in campaigns if c.get('ID') not in existing_ids]
    print(f"New campaigns to archive: {len(new_campaigns)}")

    if not new_campaigns:
        print("No new campaigns to archive.")
        return

    # Sort by send date (oldest first)
    new_campaigns.sort(key=lambda c: c.get('DeliveredAt') or c.get('SendStartAt') or '')

    # Archive each campaign
    archived = 0
    failed = 0

    for campaign in new_campaigns:
        campaign_id = campaign.get('ID')
        title = campaign.get('Title', '') or campaign.get('Subject', '') or f'Campaign {campaign_id}'
        sent_at = campaign.get('DeliveredAt') or campaign.get('SendStartAt', 'unknown date')

        print(f"\nProcessing: {title}")
        print(f"  ID: {campaign_id}, Sent: {sent_at}")

        try:
            if archive_campaign(client, campaign, args.dry_run):
                archived += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  Error: {e}")
            failed += 1

    print(f"\nDone! Archived: {archived}, Failed: {failed}")

    # Regenerate index.html if any campaigns were processed
    if archived > 0 or not args.dry_run:
        print()
        generate_index_html()


if __name__ == '__main__':
    main()

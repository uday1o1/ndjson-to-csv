import csv
import os
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'convertJsonToCSV.py'
SAMPLE = REPO / 'sample_data' / 'sample.ndjson'

OUT_PLAIN = REPO / 'sample_data' / 'out_plain.csv'
OUT_FLAT = REPO / 'sample_data' / 'out_flat.csv'
OUT_EXPLODED = REPO / 'sample_data' / 'out_exploded.csv'

class TestConvertJsonToCSV(unittest.TestCase):

    def setUp(self):
        for p in (OUT_PLAIN, OUT_FLAT, OUT_EXPLODED):
            if p.exists():
                p.unlink()

    def test_plain(self):
        subprocess.check_call([
            'python3', str(SCRIPT),
            '-i', str(SAMPLE),
            '-o', str(OUT_PLAIN)
        ])
        self.assertTrue(OUT_PLAIN.exists(), 'plain CSV not created')

        with OUT_PLAIN.open(newline='', encoding='utf-8') as f:
            r = list(csv.reader(f))
        self.assertGreaterEqual(len(r), 2, 'plain CSV missing data rows')
        self.assertEqual(r[0], ['song', 'tags', 'year'], 'plain header mismatch')

        # cells should be stringified dict/list in plain mode
        self.assertTrue(r[1][0].startswith("{'artist': "), 'plain mode song cell not a dict string')
        self.assertTrue(r[1][1].startswith("['"), 'plain mode tags cell not a list string')

    def test_flatten(self):
        subprocess.check_call([
            'python3', str(SCRIPT),
            '-i', str(SAMPLE),
            '-o', str(OUT_FLAT),
            '--flatten'
        ])
        self.assertTrue(OUT_FLAT.exists(), 'flattened CSV not created')

        with OUT_FLAT.open(newline='', encoding='utf-8') as f:
            r = list(csv.reader(f))
        self.assertGreaterEqual(len(r), 2, 'flattened CSV missing data rows')

        header = r[0]
        self.assertIn('song.artist', header, 'missing song.artist in header')
        self.assertIn('song.track', header, 'missing song.track in header')
        self.assertIn('tags', header, 'missing tags in header')

        # tags should be a JSON string in flattened mode (not exploded)
        tags_idx = header.index('tags')
        self.assertTrue(r[1][tags_idx].startswith('['), 'tags not JSON string in flattened mode')

    def test_flatten_and_explode(self):
        subprocess.check_call([
            'python3', str(SCRIPT),
            '-i', str(SAMPLE),
            '-o', str(OUT_EXPLODED),
            '--flatten',
            '--explode-column', 'tags'
        ])
        self.assertTrue(OUT_EXPLODED.exists(), 'exploded CSV not created')

        with OUT_EXPLODED.open(newline='', encoding='utf-8') as f:
            r = list(csv.reader(f))
        self.assertGreaterEqual(len(r), 5, 'exploded CSV should have header + 4 rows')
        header = r[0]
        self.assertEqual(header, ['song.artist', 'song.track', 'tags', 'year'], 'exploded header mismatch')

        # expected rows after explosion (order should match sample)
        expected_rows = {
            ('Parachute', 'Halfway', 'pop', '2011'),
            ('Parachute', 'Halfway', 'rock', '2011'),
            ('Coldplay', 'Yellow', 'alt', '2000'),
            ('Coldplay', 'Yellow', 'britpop', '2000'),
        }
        actual_rows = set(tuple(row) for row in r[1:])
        self.assertTrue(expected_rows.issubset(actual_rows), 'exploded rows do not match expected')

if __name__ == '__main__':
    unittest.main()
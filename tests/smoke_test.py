import csv
import os
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'convertJsonToCSV.py'
SAMPLE = REPO / 'sample_data' / 'sample.ndjson'
SAMPLE_MULTI = REPO / 'sample_data' / 'sample_multi.ndjson'

OUT_PLAIN = REPO / 'sample_data' / 'out_plain.csv'
OUT_FLAT = REPO / 'sample_data' / 'out_flat.csv'
OUT_EXPLODED = REPO / 'sample_data' / 'out_exploded.csv'
OUT_EXPLODED_ALL = REPO / 'sample_data' / 'out_exploded_all.csv'

class TestConvertJsonToCSV(unittest.TestCase):

    def setUp(self):
        for p in (OUT_PLAIN, OUT_FLAT, OUT_EXPLODED, OUT_EXPLODED_ALL):
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
        self.assertEqual(r[0], ['song', 'tags', 'year'], 'plain header mismatch')
        self.assertGreaterEqual(len(r), 3, 'plain CSV missing rows')

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
        header = r[0]
        self.assertIn('song.artist', header)
        self.assertIn('song.track', header)
        self.assertIn('tags', header)

    def test_explode_single_column(self):
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
        header = r[0]
        self.assertEqual(header, ['song.artist', 'song.track', 'tags', 'year'])
        rows = set(tuple(row) for row in r[1:])
        expected = {
            ('Parachute','Halfway','pop','2011'),
            ('Parachute','Halfway','rock','2011'),
            ('Coldplay','Yellow','alt','2000'),
            ('Coldplay','Yellow','britpop','2000'),
        }
        self.assertTrue(expected.issubset(rows))

    def test_explode_all_columns_cartesian(self):
        subprocess.check_call([
            'python3', str(SCRIPT),
            '-i', str(SAMPLE_MULTI),
            '-o', str(OUT_EXPLODED_ALL),
            '--flatten',
            '--explode-all'
        ])
        self.assertTrue(OUT_EXPLODED_ALL.exists(), 'explode-all CSV not created')
        with OUT_EXPLODED_ALL.open(newline='', encoding='utf-8') as f:
            r = list(csv.reader(f))
        header = r[0]
        self.assertIn('tags', header)
        self.assertIn('moods', header)
        rows = set(tuple(row) for row in r[1:])
        expected_subset = {
            ('Parachute','Halfway','pop','happy','2011'),
            ('Parachute','Halfway','rock','sad','2011'),
            ('Coldplay','Yellow','alt','nostalgic','2000'),
            ('Coldplay','Yellow','britpop','calm','2000'),
        }
        proj_idx = [header.index('song.artist'), header.index('song.track'), header.index('tags'), header.index('moods'), header.index('year')]
        projected = set(tuple(row[i] for i in proj_idx) for row in r[1:])
        self.assertTrue(expected_subset.issubset(projected))

if __name__ == '__main__':
    unittest.main()

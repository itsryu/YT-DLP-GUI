import unittest
import logging
from unittest.mock import patch

from processamento import NormalizedMediaEntity, SpotifyToYTMAdapter, YtDlpService

class TestNormalizedMediaEntity(unittest.TestCase):
    def test_display_duration_formats_correctly(self):
        entity_zero = NormalizedMediaEntity(original_id="1", title="Test", artist="A", album="A", duration=0)
        self.assertEqual(entity_zero.display_duration, "N/A")

        entity_sec = NormalizedMediaEntity(original_id="2", title="Test", artist="A", album="A", duration=45.5)
        self.assertEqual(entity_sec.display_duration, "00:45")

        entity_min = NormalizedMediaEntity(original_id="3", title="Test", artist="A", album="A", duration=125)
        self.assertEqual(entity_min.display_duration, "02:05")

        entity_hr = NormalizedMediaEntity(original_id="4", title="Test", artist="A", album="A", duration=3665)
        self.assertEqual(entity_hr.display_duration, "01:01:05")

    def test_is_search_query_detects_search_prefixes(self):
        entity_search1 = NormalizedMediaEntity(original_id="ytmsearch:test", title="Test", artist="A", album="A")
        self.assertTrue(entity_search1.is_search_query)

        entity_normal = NormalizedMediaEntity(original_id="v=dQw4w9WgXcQ", title="Test", artist="A", album="A")
        self.assertFalse(entity_normal.is_search_query)

    def test_ytm_search_query_strips_special_characters(self):
        entity = NormalizedMediaEntity(original_id="1", title="Song Title!", artist="Artist Name-", album="A")
        self.assertEqual(entity.ytm_search_query, "ytmsearch1:Song Title Artist Name-")

class TestSpotifyToYTMAdapter(unittest.TestCase):
    def setUp(self):
        logging.getLogger('SpotifyToYTMAdapter').setLevel(logging.ERROR)
        with patch('os.environ.get', return_value=None):
            self.adapter = SpotifyToYTMAdapter()

    def test_is_spotify_url(self):
        self.assertTrue(self.adapter.is_spotify_url("https://open.spotify.com/track/4PTG3Z6ehGkBFwjybzWkR8?si=f15133eac8f74ad1"))
        self.assertFalse(self.adapter.is_spotify_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ"))

    def test_extract_id_resolves_valid_hashes(self):
        track_id = self.adapter._extract_id("https://open.spotify.com/track/4PTG3Z6ehGkBFwjybzWkR8?si=f15133eac8f74ad1", "track")
        self.assertEqual(track_id, "4PTG3Z6ehGkBFwjybzWkR8")

        with self.assertRaises(ValueError):
            self.adapter._extract_id("https://open.spotify.com/invalid/123", "track")

class TestYtDlpService(unittest.TestCase):
    def test_validate_url_regex(self):
        self.assertTrue(YtDlpService.validate_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ"))
        self.assertTrue(YtDlpService.validate_url("https://youtu.be/dQw4w9WgXcQ"))
        self.assertTrue(YtDlpService.validate_url("https://soundcloud.com/artist/track"))
        self.assertTrue(YtDlpService.validate_url("https://music.youtube.com/watch?v=123"))
        
        self.assertFalse(YtDlpService.validate_url("https://www.google.com"))
        self.assertFalse(YtDlpService.validate_url("not a url"))


if __name__ == '__main__':
    unittest.main()
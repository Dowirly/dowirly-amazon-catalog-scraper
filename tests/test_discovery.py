import unittest

from dowirly_amazon_scraper.discovery import extract_search_candidates, merge_candidates


class DiscoveryTests(unittest.TestCase):
    def test_extract_and_dedupe(self):
        wrapper = {
            "results": [{
                "content": {
                    "query": "headphones",
                    "results": {
                        "organic": [
                            {"asin": "B0ABC12345", "title": "A"},
                            {"asin": "B0ABC12345", "title": "A"},
                            {"asin": "BAD", "title": "Bad"},
                        ]
                    }
                }
            }]
        }
        records = extract_search_candidates(wrapper, query_to_category={"headphones": "Audio"}, include_paid=False)
        self.assertEqual(len(records), 2)
        merged = merge_candidates(records)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["logical_categories"], ["Audio"])


if __name__ == "__main__":
    unittest.main()

import unittest

from dowirly_amazon_scraper.normalization import normalize_product


class NormalizeTests(unittest.TestCase):
    def test_normalizes_embedding_ready_product(self):
        parsed = {
            "asin": "B0ABC12345",
            "title": "Example Headphones",
            "brand": "Acme",
            "description": "Wireless headphones",
            "bullet_points": "Noise cancelling\n30 hour battery",
            "price": 299.0,
            "currency": "SAR",
            "images": ["https://example.test/image.jpg"],
            "category": [{"ladder": [{"name": "Electronics", "url": "/e"}, {"name": "Headphones", "url": "/h"}]}],
            "parse_status_code": 12000,
            "product_overview": [{"title": "Connectivity", "description": "Bluetooth"}],
        }
        result = normalize_product(parsed, {"status_code": 200, "job_id": "1"}, None, require_price=True, require_image=True, require_category=True)
        self.assertEqual(result.rejection_reasons, [])
        self.assertEqual(result.product["category"]["path"], ["Electronics", "Headphones"])
        self.assertIn("Noise cancelling", result.product["embedding_text"])
        self.assertNotIn("299.0", result.product["embedding_text"])

    def test_rejects_incomplete_product(self):
        parsed = {"asin": "B0ABC12345", "title": "No image", "price": 10, "category": [], "parse_status_code": 12000}
        result = normalize_product(parsed, {"status_code": 200}, None, require_price=True, require_image=True, require_category=True)
        self.assertIsNone(result.product)
        self.assertIn("missing_images", result.rejection_reasons)
        self.assertIn("missing_category", result.rejection_reasons)


if __name__ == "__main__":
    unittest.main()

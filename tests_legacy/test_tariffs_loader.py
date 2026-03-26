import unittest

from tariffs.loader import format_traffic, format_duration


class TariffsLoaderTests(unittest.TestCase):
    def test_format_traffic(self):
        self.assertEqual(format_traffic(100), "100 ГБ")
        self.assertEqual(format_traffic(1024), "1 ТБ")
        self.assertEqual(format_traffic("bad"), "bad")

    def test_format_duration(self):
        self.assertEqual(format_duration(30), "30 дней")


if __name__ == "__main__":
    unittest.main()

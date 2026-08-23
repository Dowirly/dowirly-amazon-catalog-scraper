from pathlib import Path

from dowirly_amazon_scraper.config import load_search_plan


def test_queries_are_round_robin_across_categories(tmp_path: Path) -> None:
    path = tmp_path / "queries.yaml"
    path.write_text(
        """
max_pages_per_query: 2
sorts: [featured]
categories:
  - name: A
    queries: [a1, a2, a3]
  - name: B
    queries: [b1, b2]
  - name: C
    queries: [c1, c2, c3]
""".strip(),
        encoding="utf-8",
    )

    plan = load_search_plan(path)
    assert [(q.logical_category, q.query) for q in plan.queries] == [
        ("A", "a1"),
        ("B", "b1"),
        ("C", "c1"),
        ("A", "a2"),
        ("B", "b2"),
        ("C", "c2"),
        ("A", "a3"),
        ("C", "c3"),
    ]

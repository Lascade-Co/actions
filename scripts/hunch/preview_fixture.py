"""Approved preview inputs, retained as a deterministic regression fixture."""
from datetime import date


REPORT_DATE = date(2026, 11, 19)
GROUP_COUNTS = {
    "2026-10-31": (11, 0, 0, 0, 0, 0, 10, 0, 0),
    "2026-11-01": (38, 1, 2, 2, 0, 0, 37, 4, 2),
    "2026-11-02": (38, 1, 2, 2, 0, 0, 38, 4, 2),
    "2026-11-03": (19, 2, 3, 3, 0, 2, 19, 3, 2),
    "2026-11-04": (15, 0, 0, 1, 0, 0, 14, 1, 1),
    "2026-11-05": (32, 1, 2, 2, 0, 0, 30, 3, 1),
    "2026-11-06": (29, 0, 0, 1, 0, 0, 27, 1, 1),
    "2026-11-07": (16, 1, 1, 1, 0, 0, 16, 1, 0),
    "2026-11-08": (17, 4, 5, 6, 1, 4, 16, 6, 6),
    "2026-11-09": (27, 2, 3, 5, 0, 2, 24, 5, 4),
    "2026-11-10": (13, 5, 5, 6, 0, 0, 13, 6, 5),
    "2026-11-11": (92, 10, 14, 16, 1, 6, 89, 16, 12),
    "2026-11-12": (116, 13, 15, 16, 0, 7, 113, 16, 13),
    "2026-11-13": (138, 15, 21, 21, 0, 5, 132, 21, 19),
    "2026-11-14": (110, 6, 10, 10, 0, 3, 104, 10, 10),
    "2026-11-15": (111, 9, 11, 11, 0, 1, 106, 11, 11),
    "2026-11-16": (99, 4, 4, 4, 0, 1, 94, 4, 4),
    "2026-11-17": (9, 1, 1, 1, 0, 0, 1, 1, 1),
    "2026-11-18": (5, 0, 0, 0, 0, 0, 0, 0, 0),
}
DAY_COUNTS = {
    "2026-11-12": (226, 130, 777, 707, 653, 75),
    "2026-11-13": (286, 163, 932, 851, 809, 92),
    "2026-11-14": (244, 141, 650, 574, 542, 74),
    "2026-11-15": (231, 132, 751, 682, 653, 75),
    "2026-11-16": (267, 133, 514, 462, 426, 72),
    "2026-11-17": (35, 18, 53, 53, 51, 17),
    "2026-11-18": (33, 15, 66, 61, 59, 12),
}
NEW_EVENTS = ["Application Installed", "Application Opened", "Application Backgrounded"]
NEW_VERSIONS = [
    {"day": "2026-11-18", "platform": "Android", "version": "0.1.1"},
    {"day": "2026-11-18", "platform": "iOS", "version": "0.1.2"},
]


def preview_inputs():
    group_fields = ("n", "d1", "d3", "d7any", "d7", "days3", "paid", "returned", "returned_daily")
    day_fields = ("people", "players", "hunts", "finished", "won", "daily_players")
    return {
        "groups": [{"d0": day, **dict(zip(group_fields, counts))} for day, counts in GROUP_COUNTS.items()],
        "days": [{"day": day, **dict(zip(day_fields, counts))} for day, counts in DAY_COUNTS.items()],
        "new_events": NEW_EVENTS.copy(),
        "new_versions": [row.copy() for row in NEW_VERSIONS],
    }

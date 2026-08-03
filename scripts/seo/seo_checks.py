"""Rule registry. Aggregates the three group modules — add new groups here."""

from __future__ import annotations

from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC
from seo_checks_def import BLOG_RULES_DEF, RUN_RULES_DEF
from seo_checks_ghi import BLOG_RULES_GHI, RUN_RULES_GHI

BLOG_RULES = BLOG_RULES_ABC + BLOG_RULES_DEF + BLOG_RULES_GHI
RUN_RULES = RUN_RULES_ABC + RUN_RULES_DEF + RUN_RULES_GHI
ALL_RULES = BLOG_RULES + RUN_RULES
RULES_BY_ID = {rule.id: rule for rule in ALL_RULES}

assert len(RULES_BY_ID) == len(ALL_RULES), "duplicate rule id in the registry"

"""Offline Muscat reconciliation checks. All rows here are synthetic."""
import datetime as dt
import json
from pathlib import Path
import subprocess
import tempfile

import pytest

import ip_crosscheck as X


class TestMuscatCrosscheck:
    @pytest.fixture
    def crosscheck_repo(self, monkeypatch):
        cache = Path(__file__).resolve().parent / ".cache"
        cache.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="crosscheck-tests-", dir=cache) as name:
            repo = Path(name)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / ".gitignore").write_text("*.local.json\n")
            monkeypatch.chdir(repo)
            yield repo

    def test_muscat_query_uses_fixed_whole_spine_anchor_and_utc_days(self):
        # Advancing the checked dates must not advance the first-install lookback.
        q = X.muscat_days_sql([dt.date(2024, 2, 3), dt.date(2024, 2, 4)],
                             dt.date(2024, 1, 1), ["192.0.2.0/24"])
        assert "timestamp >= toDateTime('2023-06-28 20:00:00', 'UTC')" in q
        assert "t0 >= toDateTime('2024-02-02 20:00:00', 'UTC')" in q
        assert "t0 < toDateTime('2024-02-04 20:00:00', 'UTC')" in q
        assert "timestamp < toDateTime('2024-02-05 20:00:00', 'UTC')" in q
        assert "toDate(toTimeZone(t0, 'Asia/Muscat'))" in q
        q_later = X.muscat_days_sql([dt.date(2024, 2, 4)], dt.date(2024, 1, 1), ["192.0.2.0/24"])
        assert "timestamp >= toDateTime('2023-06-28 20:00:00', 'UTC')" in q_later

    def test_query_keeps_first_install_attributes_and_all_eligible_builds(self):
        q = X.muscat_days_sql([dt.date(2024, 2, 3)], dt.date(2024, 1, 1), ["192.0.2.0/24"])
        assert "min(timestamp) AS t0" in q
        for prop in ("$app_version", "$os", "$device_model", "$ip"):
            assert f"argMin(ifNull(toString(properties.{prop}), ''), timestamp)" in q
        assert "os IN ('iOS', 'iPadOS', 'Android')" in q
        assert "match(v, '^[0-9]+([.][0-9]+)+$')" in q
        assert "startsWith(v, '4.0.') OR startsWith(v, '5.')" in q
        assert "NOT startsWith(model, 'sdk_')" in q
        assert "NOT (isIPAddressInRange(if(ip = '', '0.0.0.0', ip), '192.0.2.0/24'))" in q
        assert "country" not in q and "LIMIT 2" in q

    def test_paid_join_counts_people_once_with_exclusive_24_hour_end(self):
        q = X.muscat_days_sql([dt.date(2024, 2, 3)], dt.date(2024, 1, 1), ["192.0.2.0/24"])
        assert "LEFT JOIN paid_events ON paid_events.person_id = inst.person_id" in q
        assert "properties.environment = 'Production'" in q
        assert "toFloat(properties.price_usd) > 0" in q
        assert "paid_events.timestamp >= inst.t0" in q
        assert "paid_events.timestamp < inst.t0 + interval 24 hour" in q
        assert "countIf(has_paid) AS paid" in q
        assert "GROUP BY inst.person_id, inst.t0, inst.platform" in q
        assert "LEFT JOIN daily" in q
        assert "ifNull(daily.eligible, 0) AS eligible" in q
        assert "ifNull(daily.paid, 0) AS paid" in q

    def test_validation_rejects_days_outside_anchor_and_immature_days(self):
        with pytest.raises(ValueError):
            X.muscat_days_sql([dt.date(2023, 12, 25)], dt.date(2024, 1, 1), ["192.0.2.0/24"])
        now = dt.datetime(2024, 2, 4, 20, tzinfo=dt.timezone.utc)
        assert X.validate_muscat_days([dt.date(2024, 2, 3)], dt.date(2024, 1, 1), now=now)
        with pytest.raises(ValueError):
            X.validate_muscat_days([dt.date(2024, 2, 4)], dt.date(2024, 1, 1), now=now)

    def test_output_rejects_public_tracked_and_symlink_paths(self, crosscheck_repo):
        repo = crosscheck_repo
        with pytest.raises(ValueError):
            X.local_output_path(repo / "public.json")
        tracked = repo / "tracked.local.json"
        tracked.write_text("{}")
        subprocess.run(["git", "add", "-f", str(tracked)], check=True)
        with pytest.raises(ValueError):
            X.local_output_path(tracked)
        link = repo / "link.local.json"
        link.symlink_to(repo / "public.json")
        with pytest.raises(ValueError):
            X.local_output_path(link)
        assert X.local_output_path(repo / "result.local.json") == repo / "result.local.json"

    def test_cli_writes_only_local_counts_and_compare_emits_only_verdict(self, crosscheck_repo, monkeypatch, capsys):
        rows = [{"date": "2024-02-03", "platform": "ios", "eligible": 2, "paid": 2},
                {"date": "2024-02-03", "platform": "android", "eligible": 0, "paid": 0}]
        monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "synthetic-key")
        monkeypatch.setattr(X.F, "load_cidrs", lambda: (["192.0.2.0/24"], "test"))
        monkeypatch.setattr(X.F, "make_runner", lambda key: lambda sql: rows)
        out = crosscheck_repo / "result.local.json"
        args = ["--muscat-days", "2024-02-03", "--spine-start", "2024-01-01"]
        assert X.main(args + ["--output", str(out)]) == 0
        assert capsys.readouterr().out == ""
        written = json.loads(out.read_text())
        assert written["spine_start"] == "2024-01-01"
        assert sorted(written["rows"], key=lambda row: row["platform"]) == sorted(rows, key=lambda row: row["platform"])
        assert X.main(args + ["--compare", str(out)]) == 0
        assert capsys.readouterr().out == "MATCH\n"
        written["rows"][0]["paid"] = 1
        out.write_text(json.dumps(written))
        assert X.main(args + ["--compare", str(out)]) == 1
        assert capsys.readouterr().out == "MISMATCH\n"

    def test_cli_rejects_wrong_anchor_and_missing_zero_rows(self, crosscheck_repo, monkeypatch, capsys):
        monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "synthetic-key")
        monkeypatch.setattr(X.F, "load_cidrs", lambda: (["192.0.2.0/24"], "test"))
        monkeypatch.setattr(X.F, "make_runner", lambda key: lambda sql: [])
        compare = crosscheck_repo / "compare.local.json"
        compare.write_text(json.dumps({"spine_start": "2024-01-02", "rows": []}))
        args = ["--muscat-days", "2024-02-03", "--spine-start", "2024-01-01", "--compare", str(compare)]
        assert X.main(args) == 1
        captured = capsys.readouterr()
        assert captured.out == "MISMATCH\n" and captured.err == ""
        compare.write_text(json.dumps({"spine_start": "2024-01-01", "rows": []}))
        assert X.main(args) == 1
        captured = capsys.readouterr()
        assert captured.out == "MISMATCH\n" and captured.err == ""

    def test_cli_errors_never_echo_response_content(self, crosscheck_repo, monkeypatch, capsys):
        monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "synthetic-key")
        monkeypatch.setattr(X.F, "load_cidrs", lambda: (["192.0.2.0/24"], "test"))
        def fail(sql):
            raise ValueError("sensitive response content")
        monkeypatch.setattr(X.F, "make_runner", lambda key: fail)
        args = ["--muscat-days", "2024-02-03", "--spine-start", "2024-01-01",
                "--output", str(crosscheck_repo / "result.local.json")]
        assert X.main(args) != 0
        captured = capsys.readouterr()
        assert "sensitive response content" not in captured.out + captured.err
        assert not (crosscheck_repo / "result.local.json").exists()

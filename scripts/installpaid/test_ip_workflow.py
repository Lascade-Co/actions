"""Execute workflow wiring offline; timestamps, API history and sends are synthetic."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = yaml.load((ROOT / ".github/workflows/daily-install-paid.yml").read_text(), Loader=yaml.BaseLoader)


def expression(source, context):
    """Evaluate the small expression subset used at this workflow's boundaries."""
    source = source.replace("&&", " and ").replace("||", " or ")
    source = re.sub(r"!(?!=)", " not ", source)
    source = re.sub(r"\b(?:github|inputs|needs|steps|secrets)\.[\w.]+",
                    lambda match: f"context.get({match[0]!r}, '')", source)
    return eval(source.strip(), {"__builtins__": {}, "context": context,
                                "startsWith": lambda value, prefix: value.startswith(prefix),
                                "true": True, "false": False})


def rendered(source, context):
    def replace(match):
        value = expression(match[1], context)
        return str(value).lower() if isinstance(value, bool) else str(value)
    return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", replace, source)


# This stand-in handles every outbound boundary and refuses unknown commands.
# The gate and workflow helpers themselves execute unchanged; only sleeping and
# the fetch/render/send entrypoints are intercepted.
STANDIN = r'''
import json, os, pathlib, runpy, shutil, sys, time
args = sys.argv[1:]
kind = pathlib.Path(sys.argv[0]).name
with open(os.environ["TEST_COMMAND_LOG"], "a") as stream:
    stream.write(json.dumps({"tool": kind, "args": args}) + "\n")
if kind == "curl":
    url = next(value for value in args if value.startswith("https://"))
    if url.startswith("https://api.telegram.org/"):
        assert "--data-urlencode" in args
    else:
        relative = url.split("/scripts/", 1)[1]
        source = pathlib.Path(os.environ["TEST_SOURCE_ROOT"]) / "scripts" / relative
        assert source.is_file()
        shutil.copyfile(source, args[args.index("-o") + 1])
elif kind == "gh":
    assert args[0] == "api"
    path = args[1]
    if "--jq" in args:
        print(os.environ["TEST_ATTEMPT_STARTED"] if "/attempts/2" in path else "2029-01-01T00:00:00Z")
    elif "/jobs" in path:
        print(json.dumps({"jobs": [{"name": "email", "conclusion": "failure", "steps": [
            {"name": os.environ["TEST_ACCEPTED_STEP"], "conclusion": "success"}]}]}))
    else:
        runs = [{"id": 9, "run_attempt": 1, "event": "schedule", "head_branch": "main",
                 "head_repository": {"full_name": "Lascade-Co/actions"}}] if os.environ.get("TEST_ACCEPTED_STEP") else []
        print(json.dumps({"workflow_runs": runs}))
elif kind == "python3":
    if pathlib.Path(args[0]).name not in ("ip_main.py", "ip_send.py"):
        sys.argv = args
        sys.path.insert(0, str(pathlib.Path(args[0]).resolve().parent))
        time.sleep = lambda seconds: None
        runpy.run_path(args[0], run_name="__main__")
else:
    raise AssertionError("unexpected external command")
'''


class OfflineWorkflow:
    def __init__(self, tmp_path, *, event="schedule", edition="pt", schedule="41 3 * * *", now="2030-01-12T03:41:00Z", **inputs):
        self.base = tmp_path
        self.checkout = tmp_path / "checkout"
        for relative in ("scripts/installpaid/ip_editions.py", "scripts/installpaid/ip_workflow.py",
                         "scripts/installpaid/send_resend_ip.sh", "scripts/catchup/catchup_gate.py"):
            target = self.checkout / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)
        defaults = dict(edition=edition, report_date="", recovery=False, force_resend=False,
                        dry_run=False, schedule_in_min=0, test_alert=False)
        defaults.update(inputs)
        self.context = {"github.event_name": event, "github.event.schedule": schedule,
                        "github.sha": "synthetic", "github.run_id": "17", "github.run_number": "23",
                        "github.repository": "Lascade-Co/actions", "github.server_url": "https://example.invalid",
                        "github.token": "synthetic", **{f"inputs.{key}": value for key, value in defaults.items()}}
        binary = tmp_path / "bin"
        binary.mkdir()
        for name in ("python3", "curl", "gh"):
            executable = binary / name
            executable.write_text(f"#!{sys.executable}\n" + STANDIN)
            executable.chmod(0o755)
        self.log = tmp_path / "commands.jsonl"
        self.env = dict(os.environ, NOW=now, GITHUB_REPOSITORY="Lascade-Co/actions", GITHUB_RUN_ID="17",
                        GITHUB_RUN_ATTEMPT="2", TEST_COMMAND_LOG=str(self.log), TEST_SOURCE_ROOT=str(ROOT),
                        TEST_ATTEMPT_STARTED=now, TEST_ACCEPTED_STEP="", PATH=str(binary) + os.pathsep + os.environ["PATH"])
        self.env.pop("REPORT_DATE", None)
        self.env.update({key: rendered(value, self.context) for key, value in WORKFLOW["env"].items()})
        self.sequence = 0

    def begin(self, job):
        self.job = job
        self.context = {key: value for key, value in self.context.items() if not key.startswith("steps.")}

    def step(self, selector):
        return next(step for step in WORKFLOW["jobs"][self.job]["steps"]
                    if step.get("id") == selector or step.get("name") == selector)

    def run(self, step):
        self.sequence += 1
        output = self.base / f"output-{self.sequence}"
        env = dict(self.env, GITHUB_OUTPUT=str(output))
        env.update({key: rendered(value, self.context) for key, value in step.get("env", {}).items()})
        cwd = self.checkout if self.job == "email" else self.base / self.job
        cwd = cwd / step.get("working-directory", "")
        cwd.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=cwd, env=env,
                                capture_output=True, text=True, timeout=15)
        assert result.returncode == 0, result.stderr
        values = dict(line.split("=", 1) for line in output.read_text().splitlines()) if output.exists() else {}
        self.context.update({f"steps.{step['id']}.outputs.{key}": value for key, value in values.items()} if "id" in step else {})
        return values

    def publish(self):
        values = {key: rendered(value, self.context) for key, value in WORKFLOW["jobs"][self.job].get("outputs", {}).items()}
        self.context.update({f"needs.{self.job}.outputs.{key}": value for key, value in values.items()})
        return values

    def gates(self, released_at):
        self.begin("gate")
        self.run(self.step("settings"))
        self.run(self.step("gate"))
        first = self.publish()
        self.begin("gate2")
        self.env["NOW"] = released_at
        self.run(self.step("gate"))
        return first, self.publish()

    def commands(self, tool=None, script=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [row["args"] for row in rows if (not tool or row["tool"] == tool)
                and (not script or Path(row["args"][0]).name == script)]


def flag(args, name):
    return args[args.index(name) + 1]


class TestDailyWorkflow:
    @pytest.mark.parametrize("edition,cron,started,released,zone,prefix", [
        ("pt", "41 3 * * *", "2030-01-12T03:41:00Z", "2030-01-12T13:05:00Z", "America/Los_Angeles", "Deliver for "),
        ("pt", "41 6 * * *", "2030-01-12T06:41:00Z", "2030-01-12T13:05:00Z", "America/Los_Angeles", "Deliver for "),
        ("pt", "41 9 * * *", "2030-01-12T09:41:00Z", "2030-01-12T13:05:00Z", "America/Los_Angeles", "Deliver for "),
        ("ist", "7 13 * * *", "2030-01-11T13:07:00Z", "2030-01-11T23:35:00Z", "Asia/Kolkata", "Deliver IST for "),
        ("ist", "7 16 * * *", "2030-01-11T16:07:00Z", "2030-01-11T23:35:00Z", "Asia/Kolkata", "Deliver IST for "),
        ("ist", "7 19 * * *", "2030-01-11T19:07:00Z", "2030-01-11T23:35:00Z", "Asia/Kolkata", "Deliver IST for "),
    ])
    def test_cron_edition_reaches_gates_build_and_send(self, tmp_path, edition, cron, started, released, zone, prefix):
        run = OfflineWorkflow(tmp_path, edition="ist" if edition == "pt" else "pt", schedule=cron, now=started,
                              force_resend=True, schedule_in_min=3)
        assert {item["cron"] for item in WORKFLOW["on"]["schedule"]} >= {cron}
        first, second = run.gates(released)
        assert first["edition"] == second["edition"] == edition
        assert first["report_date"] == second["report_date"] == "2030-01-12"
        for args in run.commands("python3", "catchup_gate.py"):
            assert flag(args, "--tz") == zone
            assert flag(args, "--target") == "05:05"
            assert flag(args, "--sent-step") == prefix
        assert rendered(WORKFLOW["concurrency"]["group"], run.context) == f"daily-install-paid-{edition}-schedule"
        assert rendered(WORKFLOW["jobs"]["email"]["concurrency"]["group"], run.context) == f"daily-install-paid-email-{edition}"
        run.begin("email")
        dates = run.run(run.step("dates"))
        assert dates["edition"] == edition and dates["send_step"] == prefix + "2030-01-12"
        assert run.run(run.step("dedupe"))["sent"] == "False"
        run.run(run.step("Build the email"))
        send = WORKFLOW["jobs"]["email"]["steps"][-1]
        run.run(send)
        for script in ("ip_main.py", "ip_send.py"):
            args, = run.commands("python3", script)
            assert flag(args, "--edition") == edition and flag(args, "--report-date") == "2030-01-12"
        assert flag(args, "--event") == "schedule" and flag(args, "--schedule-in-min") == "0"
        assert "--force-resend" not in args and "--recovery" not in args

    @pytest.mark.parametrize("edition", ["pt", "ist"])
    def test_manual_historical_date_survives_both_gates_and_early_failure(self, tmp_path, edition):
        run = OfflineWorkflow(tmp_path, event="workflow_dispatch", edition=edition,
                              now="2030-01-13T02:00:00Z", report_date="2030-01-02", recovery=True)
        first, second = run.gates("2030-01-13T02:00:00Z")
        assert first["report_date"] == second["report_date"] == "2030-01-02"
        assert rendered(WORKFLOW["concurrency"]["group"], run.context) == f"daily-install-paid-{edition}-workflow_dispatch"
        assert rendered(WORKFLOW["jobs"]["email"]["concurrency"]["group"], run.context) == f"daily-install-paid-email-{edition}"
        run.begin("notify")
        run.run(run.step("Alert on Telegram"))
        message = next(value[5:] for args in run.commands("curl") for value in args if value.startswith("text="))
        assert f"{edition.upper()} edition for 2030-01-02" in message
        assert f"edition={edition}, report_date=2030-01-02, recovery ✓" in message

    def test_failure_without_outputs_uses_current_attempt_date(self, tmp_path):
        run = OfflineWorkflow(tmp_path, now="2030-01-12T03:41:00Z")
        run.begin("notify")
        run.run(run.step("Alert on Telegram"))
        message = next(value[5:] for args in run.commands("curl") for value in args if value.startswith("text="))
        assert "PT edition for 2030-01-12" in message
        assert any(args[1].endswith("/runs/17/attempts/2") and args[-1] == ".run_started_at" for args in run.commands("gh"))

    @pytest.mark.parametrize("edition,prefix,other_prefix", [("pt", "Deliver for ", "Deliver IST for "),
                                                             ("ist", "Deliver IST for ", "Deliver for ")])
    def test_email_dedupe_uses_only_the_selected_editions_delivery(self, tmp_path, edition, prefix, other_prefix):
        run = OfflineWorkflow(tmp_path, event="workflow_dispatch", edition=edition, report_date="2030-01-02", recovery=True)
        run.begin("email")
        run.run(run.step("dates"))
        run.env["TEST_ACCEPTED_STEP"] = other_prefix + "2030-01-02"
        assert run.run(run.step("dedupe"))["sent"] == "False"
        run.env["TEST_ACCEPTED_STEP"] = prefix + "2030-01-02"
        assert run.run(run.step("dedupe"))["sent"] == "True"
        send = WORKFLOW["jobs"]["email"]["steps"][-1]
        assert rendered(send["if"], run.context) == "false"

    @pytest.mark.parametrize("edition,recovery,force,dry,offset,wanted_flag,wanted_step", [
        ("pt", False, False, False, 3, None, "Test send for 2030-01-02"),
        ("pt", True, False, False, 0, "--recovery", "Deliver for 2030-01-02"),
        ("pt", True, True, False, 0, "--force-resend", "Forced send for 2030-01-02"),
        ("ist", True, True, False, 0, "--force-resend", "Forced send for 2030-01-02"),
        ("ist", False, False, True, 0, None, "Test send for 2030-01-02"),
    ])
    def test_manual_modes_reach_sender_and_dry_run_blocks_send(self, tmp_path, edition, recovery, force, dry, offset, wanted_flag, wanted_step):
        run = OfflineWorkflow(tmp_path, event="workflow_dispatch", edition=edition, report_date="2030-01-02",
                              recovery=recovery, force_resend=force, dry_run=dry, schedule_in_min=offset)
        run.begin("email")
        run.run(run.step("dates"))
        # Forced sends must bypass even an existing accepted delivery.
        if force:
            run.env["TEST_ACCEPTED_STEP"] = ("Deliver for " if edition == "pt" else "Deliver IST for ") + "2030-01-02"
        assert run.run(run.step("dedupe"))["sent"] == "False"
        send = WORKFLOW["jobs"]["email"]["steps"][-1]
        assert rendered(send["name"], run.context) == wanted_step
        assert rendered(send["if"], run.context) == ("false" if dry else "true")
        if not dry:
            run.run(send)
            args, = run.commands("python3", "ip_send.py")
            assert flag(args, "--edition") == edition and flag(args, "--event") == "workflow_dispatch"
            assert flag(args, "--schedule-in-min") == str(offset)
            assert {item for item in args if item in ("--recovery", "--force-resend")} == ({wanted_flag} if wanted_flag else set())

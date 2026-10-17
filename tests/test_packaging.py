"""The Dataflow Flex Template packaging is never launched here, so at least check it is consistent with itself."""

import json
import re
from pathlib import Path

import pytest
import yaml

from payments_beam.pipeline import parse_stream_args

ROOT = Path(__file__).resolve().parent.parent
META = json.loads((ROOT / "flex" / "metadata.json").read_text(encoding="utf-8"))
PARAMS = {p["name"]: p for p in META["parameters"]}


def test_every_template_parameter_is_an_argument_the_pipeline_accepts():
    src = (ROOT / "payments_beam" / "pipeline.py").read_text(encoding="utf-8")
    declared = set(re.findall(r'add_argument\("--(\w+)"', src))
    assert set(PARAMS) == declared


def test_required_arguments_are_not_marked_optional_and_optional_ones_have_defaults():
    src = (ROOT / "payments_beam" / "pipeline.py").read_text(encoding="utf-8")
    for name, p in PARAMS.items():
        line = next(line for line in src.splitlines() if f'"--{name}"' in line)
        required = "required=True" in line
        assert required == (not p.get("isOptional", False)), name
        if not required:
            assert "default=" in line, name


def test_the_regexes_accept_real_values_and_reject_nonsense():
    ok = {"input_subscription": "projects/acme/subscriptions/payments", "output_table": "acme:ds.totals",
          "dead_letter_table": "acme:ds.dead", "window_seconds": "60", "allowed_lateness_seconds": "0"}
    bad = {"input_subscription": "payments", "output_table": "acme.ds.totals", "dead_letter_table": "ds.dead",
           "window_seconds": "0", "allowed_lateness_seconds": "-5"}
    for name, value in ok.items():
        assert all(re.search(rx, value) for rx in PARAMS[name]["regexes"]), name
    for name, value in bad.items():
        assert not all(re.search(rx, value) for rx in PARAMS[name]["regexes"]), name


def test_the_arguments_parse_and_unknown_beam_options_pass_through():
    args, rest = parse_stream_args(["--input_subscription", "projects/p/subscriptions/s", "--output_table", "p:d.t",
                                    "--dead_letter_table", "p:d.dead", "--runner=DataflowRunner", "--region=us-central1"])
    assert args.window_seconds == 60 and args.allowed_lateness_seconds == 120
    assert rest == ["--runner=DataflowRunner", "--region=us-central1"]


def test_the_dockerfile_points_at_files_that_exist():
    text = (ROOT / "flex" / "Dockerfile").read_text(encoding="utf-8")
    for name in re.findall(r"\$\{WORKDIR\}/([\w./]+)", text):
        assert (ROOT / name).exists(), name
    for src in re.findall(r"^COPY (.+?) \./?", text, flags=re.M):
        for part in src.split():
            assert (ROOT / part).exists(), part


def test_the_flex_entry_point_runs_the_streaming_pipeline():
    assert "run_stream" in (ROOT / "main.py").read_text(encoding="utf-8")


def test_the_bigquery_schemas_match_the_rows_the_pipeline_emits():
    from payments_beam.pipeline import DEAD_SCHEMA, TOTALS_SCHEMA

    emitted = {"merchant", "window_start", "window_end", "total_cents", "count", "duplicates", "conflicts", "pane"}
    assert {f.split(":")[0] for f in TOTALS_SCHEMA.split(",")} == emitted
    assert {f.split(":")[0] for f in DEAD_SCHEMA.split(",")} == {"raw", "reason", "truncated"}


# --- workflows ---------------------------------------------------------------------------------------------------

WORKFLOWS = ROOT / ".github" / "workflows"
FILES = sorted(WORKFLOWS.glob("*.yml"))


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_workflows_hold_no_key_material_and_default_to_read_only(path):
    text = path.read_text(encoding="utf-8")
    for needle in ("credentials_json", "GOOGLE_CREDENTIALS", "private_key"):
        assert needle not in text
    assert yaml.safe_load(text).get("permissions") == {"contents": "read"}


def test_the_deploy_workflow_is_gated_and_uses_federation_not_keys():
    wf = yaml.safe_load((WORKFLOWS / "deploy.yml").read_text(encoding="utf-8"))
    job = next(iter(wf["jobs"].values()))
    assert "GCP_WIF_PROVIDER" in job["if"] and job["permissions"]["id-token"] == "write"
    auth = next(s for s in job["steps"] if "google-github-actions/auth" in str(s.get("uses", "")))
    assert auth["with"]["workload_identity_provider"].startswith("${{ vars.")

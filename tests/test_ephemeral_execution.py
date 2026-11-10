"""End-to-end tests: a consumer project compiles an upstream versioned ephemeral."""

import json
import os
import re
from pathlib import Path
from typing import Optional

import duckdb
import pytest
import yaml
from dbt.cli.main import dbtRunner
import dbt.exceptions

UPSTREAM_PATH = Path(__file__).parent.parent / "test_projects" / "ephemeral_upstream"
MACROS_PATH = UPSTREAM_PATH.parent / "ephemeral_macros"
DATABASE_PATH = UPSTREAM_PATH.parent / "ephemeral.duckdb"

# Different from the upstream's vars: the consumer must compile the SQL.
CONSUMER_VARS = {"start_date": "'2024-01-01'", "end_date": "'2024-12-31'"}

V1_TABLE = '"ephemeral"."main"."stg_coaching_session"'
EPHEMERAL_CTE_PREFIX = "__dbt__cte__stg_coaching_session"


@pytest.fixture(scope="module")
def upstream_manifest():
    """Build the upstream tables, then write the manifest with `dbt parse`,
    which leaves `compiled_code` empty, like a production manifest."""
    starting_path = os.getcwd()
    os.chdir(UPSTREAM_PATH)
    try:
        runner = dbtRunner()
        runner.invoke(["clean"])
        assert runner.invoke(["deps"]).success
        assert runner.invoke(["build"]).success
        assert runner.invoke(["parse"]).success
    finally:
        os.chdir(starting_path)

    return UPSTREAM_PATH / "target" / "manifest.json"


def _write_yaml(path: Path, content: dict) -> None:
    path.write_text(yaml.safe_dump(content))


def make_consumer(
    path: Path,
    manifest_path: Path,
    model_sql: str,
    include_protected_ephemeral: bool = True,
    with_macros: bool = True,
    project_vars: Optional[dict] = None,
    manifest_options: Optional[dict] = None,
) -> Path:
    (path / "models").mkdir(parents=True)
    _write_yaml(
        path / "dbt_project.yml",
        {
            "name": "ephemeral_consumer",
            "version": "1.0.0",
            "config-version": 2,
            "profile": "ephemeral_consumer",
            # Different from the upstream's vars: the consumer must compile the SQL.
            "vars": CONSUMER_VARS if project_vars is None else project_vars,
            "models": {"ephemeral_consumer": {"+materialized": "table"}},
        },
    )
    _write_yaml(
        path / "profiles.yml",
        {
            "ephemeral_consumer": {
                "target": "dev",
                "outputs": {
                    "dev": {"type": "duckdb", "path": str(DATABASE_PATH), "threads": 1}
                },
            }
        },
    )
    if with_macros:
        _write_yaml(path / "packages.yml", {"packages": [{"local": str(MACROS_PATH)}]})
    _write_yaml(
        path / "dbt_loom.config.yml",
        {
            "include_protected_ephemeral": include_protected_ephemeral,
            "manifests": [
                {
                    "name": "ephemeral_upstream",
                    "type": "file",
                    "config": {"path": str(manifest_path)},
                    **(manifest_options or {}),
                }
            ],
        },
    )
    (path / "models" / "coaching_sessions.sql").write_text(model_sql)
    return path


REF_V3 = "select * from {{ ref('ephemeral_upstream', 'stg_coaching_session', v=3) }}"

V3_UID = "model.ephemeral_upstream.stg_coaching_session.v3"
REPORT_V2_UID = "model.ephemeral_upstream.stg_coaching_report.v2"
COACH_V2_UID = "model.ephemeral_upstream.stg_coach_the_coach.v2"
CONSUMER_UID = "model.ephemeral_consumer.coaching_sessions"


def _ls_json(output) -> dict:
    assert output.exception is None, output.exception
    assert isinstance(output.result, list)
    nodes = [json.loads(line) for line in output.result]
    return {node["unique_id"]: node for node in nodes}


def _assert_ephemeral_lineage(manifest: dict) -> None:
    """v3 sits between its upstream tables and the consumer model."""
    assert manifest["nodes"][V3_UID]["config"]["materialized"] == "ephemeral"
    assert set(manifest["parent_map"][V3_UID]) >= {REPORT_V2_UID, COACH_V2_UID}
    assert CONSUMER_UID in manifest["child_map"][V3_UID]
    assert V3_UID in manifest["parent_map"][CONSUMER_UID]
    assert V3_UID in manifest["child_map"][REPORT_V2_UID]
    assert V3_UID in manifest["child_map"][COACH_V2_UID]


def _invoke(project: Path, monkeypatch, *commands):
    monkeypatch.chdir(project)
    runner = dbtRunner()
    result = None
    for command in commands:
        result = runner.invoke(command.split())
    return result


def test_versioned_ephemeral_is_compiled_as_cte(
    tmp_path, monkeypatch, upstream_manifest
):
    project = make_consumer(tmp_path / "consumer", upstream_manifest, REF_V3)

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert output.exception is None, output.exception

    compiled = (
        project / "target/compiled/ephemeral_consumer/models/coaching_sessions.sql"
    ).read_text()

    # dbt < 1.8 names the CTE after the model name rather than the versioned alias.
    cte = re.match(rf"with ({EPHEMERAL_CTE_PREFIX}(_v3)?) as \(", compiled)
    assert cte is not None, compiled
    assert compiled.rstrip().endswith(f"select * from {cte.group(1)}")
    assert V1_TABLE not in compiled
    # Refs inside the ephemeral resolve to the upstream v2 tables.
    assert 'from "ephemeral"."main"."stg_coaching_report_v2"' in compiled
    assert 'from "ephemeral"."main"."stg_coach_the_coach_v2"' in compiled
    # Consumer vars and the consumer's copy of the macro package are used.
    assert "between '2024-01-01' and '2024-12-31'" in compiled
    assert "1900-01-01" not in compiled
    assert "md5(concat_ws('||', report_id, tenant))" in compiled
    assert (
        project / "target/compiled/ephemeral_upstream/stg_coaching_session_v3.sql"
    ).exists()


def test_injected_ephemeral_is_in_the_lineage(tmp_path, monkeypatch, upstream_manifest):
    """v3 is a real node in the consumer manifest, list output, selectors, and docs."""
    project = make_consumer(tmp_path / "consumer", upstream_manifest, REF_V3)

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert output.exception is None, output.exception

    manifest = json.loads((project / "target/manifest.json").read_text())
    _assert_ephemeral_lineage(manifest)

    listed = _ls_json(_invoke(project, monkeypatch, "ls --output json"))
    assert listed[V3_UID]["config"]["materialized"] == "ephemeral"
    assert listed[V3_UID]["original_file_path"] == ""

    # Ancestors of the consumer model include v3 and the tables it refs, not v1.
    selected = _ls_json(
        _invoke(project, monkeypatch, "ls --output json -s +coaching_sessions")
    )
    assert V3_UID in selected
    assert REPORT_V2_UID in selected
    assert COACH_V2_UID in selected
    assert CONSUMER_UID in selected
    assert "model.ephemeral_upstream.stg_coaching_session.v1" not in selected

    # docs generate compiles the project, then writes the manifest the docs site serves.
    docs = _invoke(project, monkeypatch, "docs generate")
    assert docs.exception is None, docs.exception
    assert (project / "target/index.html").exists()
    documented = json.loads((project / "target/manifest.json").read_text())
    _assert_ephemeral_lineage(documented)


def test_versioned_ephemeral_runs_in_consumer(tmp_path, monkeypatch, upstream_manifest):
    project = make_consumer(tmp_path / "consumer", upstream_manifest, REF_V3)

    output = _invoke(project, monkeypatch, "deps", "build")
    assert output.exception is None, output.exception
    assert output.success

    with duckdb.connect(str(DATABASE_PATH)) as connection:
        rows = connection.execute(
            "select bk_coaching_session from main.coaching_sessions order by 1"
        ).fetchall()

    assert rows == [("10||emea",), ("1||emea",), ("2||emea",)]


def test_consumer_seeds_do_not_break_ephemeral_validation(
    tmp_path, monkeypatch, upstream_manifest
):
    """Seeds depend only on macros (MacroDependsOn has no `nodes`)."""
    project = make_consumer(tmp_path / "consumer", upstream_manifest, REF_V3)
    (project / "seeds").mkdir()
    (project / "seeds" / "tenants.csv").write_text("tenant\nemea\n")

    output = _invoke(project, monkeypatch, "deps", "parse")
    assert output.exception is None, output.exception
    assert output.success

    manifest = json.loads((project / "target/manifest.json").read_text())
    assert "seed.ephemeral_consumer.tenants" in manifest["nodes"]
    _assert_ephemeral_lineage(manifest)


def test_protected_ephemeral_requires_opt_in(tmp_path, monkeypatch, upstream_manifest):
    project = make_consumer(
        tmp_path / "consumer",
        upstream_manifest,
        REF_V3,
        include_protected_ephemeral=False,
    )

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert isinstance(output.exception, dbt.exceptions.DbtReferenceError)


def test_missing_macro_package_raises_clear_error(
    tmp_path, monkeypatch, upstream_manifest
):
    from dbt_loom import LoomEphemeralDependencyError

    project = make_consumer(
        tmp_path / "consumer", upstream_manifest, REF_V3, with_macros=False
    )

    output = _invoke(project, monkeypatch, "compile")
    assert isinstance(output.exception, LoomEphemeralDependencyError)
    message = str(output.exception)
    assert "loom_macros" in message
    assert "macro.loom_macros.hash_key" in message
    assert "model.ephemeral_upstream.stg_coaching_session.v3" in message


def test_unreferenced_ephemeral_does_not_require_macros(
    tmp_path, monkeypatch, upstream_manifest
):
    project = make_consumer(
        tmp_path / "consumer",
        upstream_manifest,
        "select * from {{ ref('ephemeral_upstream', 'stg_coaching_session') }}",
        with_macros=False,
    )

    output = _invoke(project, monkeypatch, "compile")
    assert output.exception is None, output.exception

    compiled = (
        project / "target/compiled/ephemeral_consumer/models/coaching_sessions.sql"
    ).read_text()
    # Unversioned refs still resolve to the latest (v1) table, as before.
    assert compiled.strip() == f"select * from {V1_TABLE}"

    # The unused ephemeral is listed, and it was not compiled on its own.
    listed = _ls_json(_invoke(project, monkeypatch, "ls --output json"))
    ephemeral = listed["model.ephemeral_upstream.stg_coaching_session.v3"]
    assert ephemeral["config"]["materialized"] == "ephemeral"
    manifest = json.loads((project / "target/manifest.json").read_text())
    assert manifest["nodes"][ephemeral["unique_id"]].get("compiled_code") in (None, "")
    assert not (
        project / "target/compiled/ephemeral_upstream/stg_coaching_session_v3.sql"
    ).exists()


REF_TENANTS = "select * from {{ ref('ephemeral_upstream', 'int_known_tenants') }}"


def test_upstream_sources_and_project_macros_are_injected(
    tmp_path, monkeypatch, upstream_manifest
):
    """The ephemeral reads an upstream source and calls a macro that exists
    only in the upstream project (which calls another one)."""
    project = make_consumer(tmp_path / "consumer", upstream_manifest, REF_TENANTS)

    # The second build reuses the partial-parse manifest.
    for _ in range(2):
        output = _invoke(project, monkeypatch, "deps", "build")
        assert output.exception is None, output.exception
        assert output.success

    compiled = (
        project / "target/compiled/ephemeral_consumer/models/coaching_sessions.sql"
    ).read_text()
    assert 'from "ephemeral"."main"."raw_coach_the_coach"' in compiled
    assert "tenant in ('emea', 'apac')" in compiled

    with duckdb.connect(str(DATABASE_PATH)) as connection:
        rows = connection.execute(
            "select tenant from main.coaching_sessions order by 1"
        ).fetchall()
    assert rows == [("apac",), ("emea",)]

    # Injected sources belong to the upstream project and are never selected.
    output = _invoke(project, monkeypatch, "ls --resource-type source")
    assert output.success
    assert output.result == []


def test_missing_var_raises_clear_error(tmp_path, monkeypatch, upstream_manifest):
    project = make_consumer(
        tmp_path / "consumer",
        upstream_manifest,
        REF_V3,
        project_vars={"end_date": "'2024-12-31'"},
    )

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert output.exception is not None
    message = str(output.exception)
    assert "Missing var `start_date`" in message
    assert "end_date" not in message


def test_missing_var_is_satisfied_by_cli_vars(tmp_path, monkeypatch, upstream_manifest):
    project = make_consumer(
        tmp_path / "consumer",
        upstream_manifest,
        REF_V3,
        project_vars={"end_date": "'2024-12-31'"},
    )
    monkeypatch.chdir(project)
    runner = dbtRunner()
    runner.invoke(["deps"])
    output = runner.invoke(["compile", "--vars", "{start_date: \"'2024-01-01'\"}"])
    assert output.exception is None, output.exception


def test_manifest_level_protected_ephemeral_override(
    tmp_path, monkeypatch, upstream_manifest
):
    project = make_consumer(
        tmp_path / "consumer",
        upstream_manifest,
        REF_V3,
        include_protected_ephemeral=False,
        manifest_options={"include_protected_ephemeral": True},
    )

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert output.exception is None, output.exception


REF_PROTECTED_TABLE = (
    "select * from {{ ref('ephemeral_upstream', 'int_report_counts') }}"
)


def test_protected_table_is_blocked_by_default(
    tmp_path, monkeypatch, upstream_manifest
):
    project = make_consumer(
        tmp_path / "consumer", upstream_manifest, REF_PROTECTED_TABLE
    )

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert isinstance(output.exception, dbt.exceptions.DbtReferenceError)


def test_allowed_protected_models(tmp_path, monkeypatch, upstream_manifest):
    project = make_consumer(
        tmp_path / "consumer",
        upstream_manifest,
        REF_PROTECTED_TABLE,
        include_protected_ephemeral=False,
        manifest_options={"allowed_protected_models": ["int_report_counts"]},
    )

    output = _invoke(project, monkeypatch, "deps", "compile")
    assert output.exception is None, output.exception
    compiled = (
        project / "target/compiled/ephemeral_consumer/models/coaching_sessions.sql"
    ).read_text()
    assert '"ephemeral"."main"."int_report_counts"' in compiled

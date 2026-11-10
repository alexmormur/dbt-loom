"""End-to-end tests: a consumer project compiles an upstream versioned ephemeral."""

import os
import re
from pathlib import Path

import duckdb
import pytest
import yaml
from dbt.cli.main import dbtRunner
import dbt.exceptions

UPSTREAM_PATH = Path(__file__).parent.parent / "test_projects" / "ephemeral_upstream"
MACROS_PATH = UPSTREAM_PATH.parent / "ephemeral_macros"
DATABASE_PATH = UPSTREAM_PATH.parent / "ephemeral.duckdb"

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
            "vars": {"start_date": "'2024-01-01'", "end_date": "'2024-12-31'"},
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
                }
            ],
        },
    )
    (path / "models" / "coaching_sessions.sql").write_text(model_sql)
    return path


REF_V3 = "select * from {{ ref('ephemeral_upstream', 'stg_coaching_session', v=3) }}"


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

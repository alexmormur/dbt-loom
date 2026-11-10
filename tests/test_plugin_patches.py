"""Tests for how dbt-loom patches dbt-core."""

import json

import dbt.tracking
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode

import dbt_loom
from dbt_loom import dbtLoom, required_vars
from dbt_loom.config import ManifestReference
from dbt_loom.manifests import ManifestNode


def _make_plugin(tmp_path, monkeypatch, config=None):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"metadata": {}, "nodes": {}}))
    config_path = tmp_path / "dbt_loom.config.yml"
    if config is None:
        config_path.unlink(missing_ok=True)
    else:
        config_path.write_text(json.dumps(config))
    monkeypatch.setenv("DBT_LOOM_CONFIG", str(config_path))
    return dbtLoom("consumer")


def test_reinitializing_does_not_stack_wrappers(tmp_path, monkeypatch):
    config = {"manifests": []}
    _make_plugin(tmp_path, monkeypatch, config)
    original_from_args = dbt_loom._ORIGINAL_ATTRIBUTES[(ModelNode, "from_args")][1]
    original_track = dbt_loom._ORIGINAL_ATTRIBUTES[(dbt.tracking, "track")][1]

    second = _make_plugin(tmp_path, monkeypatch, config)

    # Each wrapper wraps dbt's original function, not the previous wrapper.
    for patched, original in (
        (ModelNode.from_args, original_from_args),
        (dbt.tracking.track, original_track),
    ):
        closure = [cell.cell_contents for cell in patched.__closure__]
        assert original in closure
    # And instance-bound wrappers belong to the newest plugin.
    assert second in [cell.cell_contents for cell in dbt.tracking.track.__closure__]


def test_patches_are_removed_without_config(tmp_path, monkeypatch):
    _make_plugin(tmp_path, monkeypatch, {"manifests": []})
    assert "from_args" in vars(ModelNode)
    assert not isinstance(vars(ModelNode)["from_args"], classmethod)

    _make_plugin(tmp_path, monkeypatch, None)

    assert isinstance(vars(ModelNode)["from_args"], classmethod)
    assert "get_target_write_path" not in vars(ModelNode)
    assert Manifest.is_invalid_protected_ref.__module__.startswith("dbt.")
    # Telemetry blocking is still active without a config.
    assert (dbt.tracking, "track") in dbt_loom._ORIGINAL_ATTRIBUTES


def test_required_vars():
    raw_code = """
        {% set a = var("start_date") %}
        {{ var('end_date') }} {{ var('with_default', 1) }}
        {{ env_var('NOT_A_VAR') }} {{ var(dynamic_name) }}
    """
    assert required_vars(raw_code) == {"start_date", "end_date"}


def _node(unique_id, access, materialized="table", version=None):
    name = unique_id.split(".")[2]
    return ManifestNode(
        unique_id=unique_id,
        name=name,
        package_name="upstream",
        resource_type="model",
        schema="main",
        access=access,
        version=version,
        config={"materialized": materialized},
    )


def test_protected_refs_allowed_by(tmp_path, monkeypatch):
    plugin = _make_plugin(
        tmp_path, monkeypatch, {"manifests": [], "include_protected_ephemeral": True}
    )
    nodes = {
        uid: node
        for uid, node in {
            "model.upstream.orders.v2": _node(
                "model.upstream.orders.v2", "protected", version="2"
            ),
            "model.upstream.orders.v1": _node(
                "model.upstream.orders.v1", "protected", version="1"
            ),
            "model.upstream.customers": _node("model.upstream.customers", "protected"),
            "model.upstream.payments": _node("model.upstream.payments", "public"),
            "model.upstream.secret": _node("model.upstream.secret", "private"),
            "model.upstream.eph": _node("model.upstream.eph", "protected", "ephemeral"),
        }.items()
    }

    def reference(**options):
        return ManifestReference(
            name="upstream",
            type="file",
            config={"path": str(tmp_path / "manifest.json")},
            **options,
        )

    allowed = plugin.protected_refs_allowed_by(
        reference(allowed_protected_models=["orders.v2", "customers", "secret"]),
        nodes,
    )
    # Private and public models are never added; the global ephemeral opt-in applies.
    assert allowed == {
        "model.upstream.orders.v2",
        "model.upstream.customers",
        "model.upstream.eph",
    }

    allowed = plugin.protected_refs_allowed_by(
        reference(include_protected_ephemeral=False), nodes
    )
    assert allowed == set()

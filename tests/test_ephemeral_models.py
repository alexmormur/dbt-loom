"""Unit tests for injecting upstream ephemeral models."""

from dbt_loom import (
    add_ephemeral_dependencies,
    convert_model_nodes_to_model_node_args,
    identify_node_subgraph,
)

PACKAGE = "icoach_source"
SESSION = "STG_H_ICOACH_COACHING_SESSION"
V1_UID = f"model.{PACKAGE}.{SESSION}.v1"
V3_UID = f"model.{PACKAGE}.{SESSION}.v3"
DEP_UID = f"model.{PACKAGE}.STG_ICOACH_COACHING_REPORT.v2"
INNER_EPHEMERAL_UID = f"model.{PACKAGE}.int_coaching_dates"


def _manifest():
    return {
        "metadata": {"project_name": PACKAGE},
        "nodes": {
            V1_UID: {
                "unique_id": V1_UID,
                "name": SESSION,
                "package_name": PACKAGE,
                "schema": "STAGE",
                "database": "DEV_DB",
                "resource_type": "model",
                "access": "public",
                "version": 1,
                "latest_version": 1,
                "relation_name": f"DEV_DB.STAGE.{SESSION}",
                "config": {"materialized": "table"},
                "raw_code": "select 1 as id",
            },
            V3_UID: {
                "unique_id": V3_UID,
                "name": SESSION,
                "package_name": PACKAGE,
                "schema": "STAGE",
                "database": "DEV_DB",
                "alias": f"{SESSION}_V3",
                "resource_type": "model",
                "access": "protected",
                "version": 3,
                "latest_version": 1,
                "relation_name": None,
                "language": "sql",
                "config": {"materialized": "ephemeral", "access": "protected"},
                "raw_code": (
                    "select {{ dbt_macros_core.hash_key(['ID']) }} as HK "
                    "from {{ ref('STG_ICOACH_COACHING_REPORT', v=2) }} "
                    "join {{ ref('int_coaching_dates') }} using (ID) "
                    "where LOAD_DATE >= {{ var('start_date') }}"
                ),
                "refs": [
                    {
                        "name": "STG_ICOACH_COACHING_REPORT",
                        "package": None,
                        "version": 2,
                    },
                    {"name": "int_coaching_dates", "package": None, "version": None},
                ],
                "depends_on": {
                    "macros": ["macro.dbt_macros_core.hash_key"],
                    "nodes": [DEP_UID, INNER_EPHEMERAL_UID],
                },
            },
            INNER_EPHEMERAL_UID: {
                "unique_id": INNER_EPHEMERAL_UID,
                "name": "int_coaching_dates",
                "package_name": PACKAGE,
                "schema": "STAGE",
                "resource_type": "model",
                "access": "private",
                "config": {"materialized": "ephemeral"},
                "raw_code": "select ID from {{ source('raw', 'dates') }}",
                "depends_on": {
                    "macros": [],
                    "nodes": ["source.icoach_source.raw.dates"],
                },
            },
            DEP_UID: {
                "unique_id": DEP_UID,
                "name": "STG_ICOACH_COACHING_REPORT",
                "package_name": PACKAGE,
                "schema": "STAGE",
                "database": "DEV_DB",
                "resource_type": "model",
                "access": "protected",
                "version": 2,
                "latest_version": 2,
                "relation_name": "DEV_DB.STAGE.STG_ICOACH_COACHING_REPORT_V2",
                "config": {"materialized": "incremental"},
                "raw_code": "select 1 as ID",
            },
        },
    }


def test_versioned_ephemeral_is_injected_with_its_sql():
    nodes = convert_model_nodes_to_model_node_args(identify_node_subgraph(_manifest()))

    v3 = nodes[V3_UID]
    assert v3.is_ephemeral
    assert v3.unique_id == V3_UID
    assert v3.version == "3"
    assert v3.relation_name is None
    # The CTE name comes from the ephemeral's own alias, never the v1 table.
    assert v3.identifier == f"{SESSION}_V3"
    assert "dbt_macros_core.hash_key" in v3.raw_code
    assert v3.depends_on_macros == ["macro.dbt_macros_core.hash_key"]
    assert [ref["name"] for ref in v3.refs] == [
        "STG_ICOACH_COACHING_REPORT",
        "int_coaching_dates",
    ]

    inner = nodes[INNER_EPHEMERAL_UID]
    assert inner.is_ephemeral
    assert inner.depends_on_sources == ["source.icoach_source.raw.dates"]


def test_non_ephemeral_nodes_are_unchanged():
    nodes = convert_model_nodes_to_model_node_args(identify_node_subgraph(_manifest()))

    v1 = nodes[V1_UID]
    assert not v1.is_ephemeral
    assert v1.identifier == SESSION
    assert v1.relation_name == f"DEV_DB.STAGE.{SESSION}"
    # SQL is only carried for nodes the consumer compiles.
    assert v1.raw_code is None


def test_ephemeral_without_raw_code_is_skipped():
    manifest = _manifest()
    del manifest["nodes"][V3_UID]["raw_code"]

    assert V3_UID not in identify_node_subgraph(manifest)


def test_public_ephemeral_version_uses_protected_config_access():
    """A versioned ephemeral can carry access public on the node and protected in config."""
    manifest = _manifest()
    manifest["nodes"][V3_UID]["access"] = "public"
    manifest["nodes"][V3_UID]["latest_version"] = 1
    manifest["nodes"][V3_UID]["config"]["access"] = "protected"

    assert identify_node_subgraph(manifest)[V3_UID].access == "protected"


def test_public_ephemeral_without_config_access_is_not_injected_as_public():
    manifest = _manifest()
    manifest["nodes"][V3_UID]["access"] = "public"
    manifest["nodes"][V3_UID]["config"].pop("access")

    assert identify_node_subgraph(manifest)[V3_UID].access == "protected"


def test_ephemeral_identifier_falls_back_to_versioned_name():
    manifest = _manifest()
    del manifest["nodes"][V3_UID]["alias"]

    assert identify_node_subgraph(manifest)[V3_UID].identifier == f"{SESSION}_v3"


def test_ephemeral_dependencies_are_added_even_when_filtered_out():
    selected = identify_node_subgraph(_manifest())
    filtered = {V3_UID: selected[V3_UID]}

    output = add_ephemeral_dependencies(selected, filtered)

    assert set(output) == {V3_UID, DEP_UID, INNER_EPHEMERAL_UID}


def test_non_ephemeral_selection_does_not_pull_dependencies():
    selected = identify_node_subgraph(_manifest())
    filtered = {V1_UID: selected[V1_UID]}

    assert set(add_ephemeral_dependencies(selected, filtered)) == {V1_UID}

from collections import defaultdict
from dataclasses import dataclass, field
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.contracts.graph.nodes import Macro, ModelNode, SourceDefinition
from dbt.exceptions import CompilationError
from dbt.graph.selector import NodeSelector

from dbt.plugins.manager import dbt_hook, dbtPlugin
from dbt.plugins.manifest import PluginNodes
from dbt.config.project import VarProvider

from dbt_loom.shims import is_invalid_private_ref, is_invalid_protected_ref

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore

try:
    from dbt.artifacts.resources import RefArgs
except ImportError:
    from dbt.contracts.graph.nodes import RefArgs  # type: ignore


from dbt_loom.config import ManifestReference, dbtLoomConfig
from dbt_loom.logging import fire_event
from dbt_loom.manifests import ManifestLoader, ManifestNode

import importlib.metadata


_LOOM_ONLY_ARGS = (
    "resource_type",
    "group",
    "config",
    "alias",
    "raw_code",
    "language",
    "refs",
    "depends_on_macros",
    "depends_on_sources",
)


@dataclass
class LoomModelNodeArgs(ModelNodeArgs):
    """A dbt-loom extension of ModelNodeArgs to preserve resource types across lineages."""

    resource_type: NodeType = NodeType.Model
    group: Optional[str] = None
    event_time: Optional[str] = None
    materialized: Optional[str] = None
    raw_code: Optional[str] = None
    language: str = "sql"
    refs: List[Dict] = field(default_factory=list)
    depends_on_macros: List[str] = field(default_factory=list)
    depends_on_sources: List[str] = field(default_factory=list)

    def __init__(self, **kwargs):
        super().__init__(
            **{
                key: value
                for key, value in kwargs.items()
                if key not in _LOOM_ONLY_ARGS
            }
        )
        config = kwargs.get("config") or {}
        self.resource_type = kwargs.get("resource_type", NodeType.Model)
        self.group = kwargs.get("group")
        self.event_time = config.get("event_time", None)
        self.materialized = config.get("materialized")
        self.raw_code = kwargs.get("raw_code")
        self.language = kwargs.get("language") or "sql"
        self.refs = list(kwargs.get("refs") or [])
        self.depends_on_macros = list(kwargs.get("depends_on_macros") or [])
        self.depends_on_sources = list(kwargs.get("depends_on_sources") or [])

    @property
    def unique_id(self) -> str:
        unique_id = f"{self.resource_type}.{self.package_name}.{self.name}"
        if self.version:
            unique_id = f"{unique_id}.v{self.version}"

        return unique_id

    @property
    def is_ephemeral(self) -> bool:
        return self.resource_type == NodeType.Model and self.materialized == "ephemeral"


def access_for_node(node: dict, is_ephemeral: bool, unique_id: str) -> str:
    """Access to store on an injected node.

    dbt rejects an ephemeral model whose access is public. A version often sets
    ``access: protected`` only under ``config`` while the node's top-level
    ``access`` stays public (the project default). Prefer a non-public config
    value, and never inject a public ephemeral.
    """
    access = node.get("access")
    config_access = (node.get("config") or {}).get("access")
    if access is None:
        access = config_access or "protected"
    if is_ephemeral and access == "public":
        if config_access in ("protected", "private"):
            access = config_access
        else:
            fire_event(
                msg=f"dbt-loom: `{unique_id}` is ephemeral with access public, "
                "which dbt rejects. Injecting it as protected."
            )
            access = "protected"
    return access


def identify_node_subgraph(manifest) -> Dict[str, ManifestNode]:
    """
    Identify all nodes that should be selected from the manifest, and return ManifestNodes.
    """

    output = {}

    # We're going to temporarily allow all nodes here.
    for unique_id in manifest["nodes"].keys():
        if unique_id.split(".")[0] in (NodeType.Test.value, NodeType.Macro.value):
            continue

        node = manifest.get("nodes", {}).get(unique_id)

        if not node:
            continue

        is_ephemeral = (
            node.get("resource_type") == NodeType.Model.value
            and (node.get("config") or {}).get("materialized") == "ephemeral"
        )

        if is_ephemeral and not node.get("raw_code"):
            fire_event(
                msg=f"dbt-loom: Skipping ephemeral model `{unique_id}` because the "
                "upstream manifest does not include its raw_code."
            )
            continue

        node["access"] = access_for_node(node, is_ephemeral, unique_id)

        # Versions may be floats or strings. Standardize on strings for compatibility.
        for key in ("version", "latest_version"):
            if node.get(key):
                node[key] = str(node[key])

        if not is_ephemeral:
            # Only ephemerals are compiled in the consumer, so don't carry SQL around.
            node = {
                key: value
                for key, value in node.items()
                if key not in ("raw_code", "refs", "language")
            }

        output[unique_id] = ManifestNode(**(node))

    return output


def add_ephemeral_dependencies(
    selected_nodes: Dict[str, ManifestNode],
    filtered_nodes: Dict[str, ManifestNode],
) -> Dict[str, ManifestNode]:
    """Return ``filtered_nodes`` plus every upstream node that an included
    ephemeral model depends on (transitively), even if package filters
    excluded it. Ephemerals are compiled in the consumer, so their refs must
    resolve there."""

    output = dict(filtered_nodes)
    stack = [uid for uid, node in filtered_nodes.items() if node.is_ephemeral]
    visited: Set[str] = set()

    while stack:
        unique_id = stack.pop()
        if unique_id in visited:
            continue
        visited.add(unique_id)

        node = output.get(unique_id)
        if node is None or node.depends_on is None:
            continue

        for dependency in getattr(node.depends_on, "nodes", ()):
            dependency_node = selected_nodes.get(dependency)
            if dependency_node is None:
                continue
            output.setdefault(dependency, dependency_node)
            if dependency_node.is_ephemeral:
                stack.append(dependency)

    return output


def convert_model_nodes_to_model_node_args(
    selected_nodes: Dict[str, ManifestNode],
) -> Dict[str, LoomModelNodeArgs]:
    """Generate a dictionary of ModelNodeArgs based on a dictionary of ModelNodes"""
    return {
        unique_id: LoomModelNodeArgs(
            schema=node.schema_name,
            identifier=node.identifier,
            depends_on_macros=node.depends_on_macros,
            depends_on_sources=node.depends_on_sources,
            **(node.dump()),
        )
        for unique_id, node in selected_nodes.items()
        if node is not None
    }


def build_ephemeral_model_node(model: ModelNode, args: LoomModelNodeArgs) -> ModelNode:
    """Turn an external ModelNode into an ephemeral node that the consumer
    compiles from the upstream raw_code and inlines as a CTE.

    The node keeps an empty ``path``/``original_file_path`` so dbt still treats
    it as external: it is dropped and re-injected on every (partial) parse.
    """
    model.config.materialized = "ephemeral"
    model.unrendered_config["materialized"] = "ephemeral"
    model.relation_name = None
    model.raw_code = args.raw_code or ""
    model.language = args.language
    model.refs = [
        RefArgs(
            name=ref["name"],
            package=ref.get("package"),
            version=None if ref.get("version") is None else str(ref["version"]),
        )
        for ref in args.refs
    ]
    model.depends_on.macros = list(args.depends_on_macros)
    return model


_MISSING = object()

# (owner, attribute) -> (raw attribute in owner.__dict__ or _MISSING, original callable)
_ORIGINAL_ATTRIBUTES: Dict[Tuple[Any, str], Tuple[Any, Callable]] = {}


def _patch_once(owner: Any, attribute: str, wrapper_factory: Callable) -> None:
    """Wrap the *original* ``owner.attribute``, so that re-initializing the
    plugin in the same process (e.g. repeated ``dbtRunner`` invocations)
    replaces the wrapper instead of stacking it on top of stale instances."""
    key = (owner, attribute)
    if key not in _ORIGINAL_ATTRIBUTES:
        _ORIGINAL_ATTRIBUTES[key] = (
            vars(owner).get(attribute, _MISSING),
            getattr(owner, attribute),
        )
    setattr(owner, attribute, wrapper_factory(_ORIGINAL_ATTRIBUTES[key][1]))


def _restore(owner: Any, attribute: str) -> None:
    """Undo ``_patch_once`` for ``owner.attribute``, if it was patched."""
    original = _ORIGINAL_ATTRIBUTES.pop((owner, attribute), None)
    if original is None:
        return
    raw, _ = original
    if raw is _MISSING:
        delattr(owner, attribute)
    else:
        setattr(owner, attribute, raw)


def _restore_all() -> None:
    for owner, attribute in list(_ORIGINAL_ATTRIBUTES):
        _restore(owner, attribute)


def target_write_path_wrapper(function) -> Callable:
    """Give injected ephemerals a compiled path. They have no file path because
    dbt must keep treating them as external nodes."""

    def outer_function(inner_self, target_path, subdirectory, *args, **kwargs):
        if inner_self.is_external_node and inner_self.is_ephemeral_model:
            return os.path.join(
                target_path,
                subdirectory,
                inner_self.package_name,
                f"{inner_self.identifier}.sql",
            )
        return function(inner_self, target_path, subdirectory, *args, **kwargs)

    return outer_function


def apply_upstream_config(model: ModelNode, args: LoomModelNodeArgs) -> None:
    """Copy the upstream materialization and access onto the config `dbt ls` prints.

    ``ModelNode.from_args`` builds a fresh config that defaults to a view with
    protected access, so the JSON hides what the upstream manifest actually said.
    """
    if args.materialized:
        model.config.materialized = args.materialized
        if isinstance(model.unrendered_config, dict):
            model.unrendered_config["materialized"] = args.materialized
    access = getattr(args, "access", None)
    if access is None or not hasattr(model.config, "access"):
        return
    try:
        from dbt.artifacts.resources.types import AccessType
    except ImportError:
        from dbt.node_types import AccessType  # type: ignore

    model.config.access = AccessType(access)


def resource_uniqueness_wrapper(function) -> Callable:
    """Leave injected ephemerals out of dbt's duplicate-relation check.

    That check builds ``database.schema.alias`` for every relational node,
    and an ephemeral is relational even though it has no table. A version
    whose alias matches v1 then aborts the consumer parse.
    """

    def outer_function(manifest, config):
        hidden = {}
        for unique_id, node in list(manifest.nodes.items()):
            if (
                isinstance(node, ModelNode)
                and node.is_external_node
                and node.is_ephemeral_model
            ):
                hidden[unique_id] = manifest.nodes.pop(unique_id)
        try:
            return function(manifest, config)
        finally:
            manifest.nodes.update(hidden)

    return outer_function


def compile_node_wrapper(function) -> Callable:
    """Do not compile an injected ephemeral as its own node.

    ``dbt compile``, ``run``, ``build`` and ``docs generate`` select every
    graph member. Rendering the ephemeral there fails when nothing in the
    consumer references it, and it is the wrong SQL anyway. Downstream models
    still compile it through ``_compile_code`` and inline it as a CTE.
    """

    def outer_function(inner_self, node, manifest, *args, **kwargs):
        if (
            isinstance(node, ModelNode)
            and node.is_external_node
            and node.is_ephemeral_model
        ):
            return node
        return function(inner_self, node, manifest, *args, **kwargs)

    return outer_function


class LoomEphemeralDependencyError(CompilationError):
    """Raised when an injected ephemeral model needs macros, sources or vars
    that the consumer project does not provide."""


# `var("name")` with no default. Dynamic names and vars with defaults are
# left for dbt to resolve at compile time.
VAR_WITHOUT_DEFAULT_PATTERN = re.compile(r"""\bvar\(\s*(?:"([^"]+)"|'([^']+)')\s*\)""")


def required_vars(raw_code: str) -> Set[str]:
    """Return the names of vars that ``raw_code`` reads without a default."""
    return {
        double or single
        for double, single in VAR_WITHOUT_DEFAULT_PATTERN.findall(raw_code or "")
    }


def merge_loom_nodes(
    existing_nodes: Dict[str, LoomModelNodeArgs],
    new_nodes: Dict[str, LoomModelNodeArgs],
    manifest_name: str,
) -> None:
    """Merge ``new_nodes`` from a manifest into ``existing_nodes`` in place.

    Nodes owned by this manifest's project are authoritative and should
    always be used. Transitive dependency nodes (from other packages) should
    only be added if not already provided by a previous, more authoritative
    manifest.
    """
    for key, value in new_nodes.items():
        is_authoritative = value.package_name == manifest_name
        if is_authoritative or key not in existing_nodes:
            existing_nodes[key] = value


@dataclass
class LoomRunnableConfig:
    """A shim class to allow is_invalid_*_ref functions to correctly handle access for loom-injected models."""

    restrict_access: bool = True
    vars: VarProvider = VarProvider(vars={})


class dbtLoom(dbtPlugin):
    """
    dbtLoom is a dbt plugin that loads manifest files, parses a DAG from the manifest,
    and injects public nodes from imported manifest.
    """

    def __init__(self, project_name: str):
        # Log the version of dbt-loom being initialized
        fire_event(
            msg=f"Initializing dbt-loom={importlib.metadata.version('dbt-loom')}"
        )

        configuration_path = Path(
            os.environ.get("DBT_LOOM_CONFIG", "dbt_loom.config.yml")
        )

        self._manifest_loader = ManifestLoader()
        self.manifests: Dict[str, Dict] = {}

        self.config: Optional[dbtLoomConfig] = self.read_config(configuration_path)
        self.models: Dict[str, LoomModelNodeArgs] = {}
        # Protected upstream models that consumers may reference anyway.
        self.allowed_protected_refs: Set[str] = set()
        # Upstream sources and macros that injected ephemerals need, keyed by unique_id.
        self.upstream_sources: Dict[str, Dict] = {}
        self.upstream_macros: Dict[str, Dict] = {}
        self.injected_source_ids: Set[str] = set()

        # Drop wrappers left behind by a previous invocation in this process.
        _restore_all()

        if self.config is not None:
            self._patch_ref_protection()

        if not self.config or (self.config and not self.config.enable_telemetry):
            self._patch_plugin_telemetry()

        super().__init__(project_name)

    def _patch_ref_protection(self) -> None:
        """Patch out the ref protection functions for proper protections"""
        import dbt.contracts.graph.manifest
        from dbt.parser.manifest import ManifestLoader as DbtManifestLoader

        fire_event(
            msg="dbt-loom: Patching ref protection methods to support dbt-loom dependencies."
        )

        _patch_once(
            dbt.contracts.graph.manifest.Manifest,
            "is_invalid_protected_ref",
            lambda _: self.protected_ref_wrapper(
                self.dependency_wrapper(is_invalid_protected_ref)
            ),
        )
        _patch_once(
            dbt.contracts.graph.manifest.Manifest,
            "is_invalid_private_ref",
            lambda _: self.dependency_wrapper(is_invalid_private_ref),
        )
        _patch_once(
            DbtManifestLoader,
            "check_valid_group_config_node",
            self.group_validation_wrapper,
        )
        _patch_once(ModelNode, "from_args", self.model_node_wrapper)
        _patch_once(DbtManifestLoader, "load", self.manifest_load_wrapper)
        _patch_once(
            DbtManifestLoader, "inject_external_nodes", self.inject_external_wrapper
        )
        _patch_once(ModelNode, "get_target_write_path", target_write_path_wrapper)
        _patch_once(NodeSelector, "_is_graph_member", self.graph_member_wrapper)
        from dbt.compilation import Compiler

        _patch_once(Compiler, "compile_node", compile_node_wrapper)
        import dbt.parser.manifest as manifest_parser

        _patch_once(
            manifest_parser,
            "_check_resource_uniqueness",
            resource_uniqueness_wrapper,
        )

    def _patch_plugin_telemetry(self) -> None:
        """Patch the plugin telemetry function to prevent tracking of dbt plugins."""
        import dbt.tracking

        _patch_once(dbt.tracking, "track", self.tracking_wrapper)

    def tracking_wrapper(self, function) -> Callable:
        """Wrap the telemetry `track` function and return early if we're tracking plugin actions."""

        def outer_function(*args, **kwargs):
            """Check the context of the snowplow tracker message for references to loom. Return if present."""

            if any(
                [
                    self.__class__.__name__ in str(context_item.__dict__)
                    or "dbt-loom" in str(context_item.__dict__)
                    or "dbt_loom" in str(context_item.__dict__)
                    for context_item in kwargs.get("context", [])
                ]
            ):
                return

            return function(*args, **kwargs)

        return outer_function

    def model_node_wrapper(self, function) -> Callable:
        """Wrap the ModelNode.from_args function and inject extra properties from the LoomModelNodeArgs."""

        def outer_function(args: LoomModelNodeArgs) -> ModelNode:
            model = function(args)
            model.group = args.group
            model.config.event_time = args.event_time
            apply_upstream_config(model, args)
            if getattr(args, "is_ephemeral", False):
                model = build_ephemeral_model_node(model, args)
            return model

        return outer_function

    def protected_ref_wrapper(self, function) -> Callable:
        """Allow refs to protected, loom-injected models that the config allows:
        protected ephemerals when opted in, and `allowed_protected_models`."""

        def outer_function(inner_self, node, target_model, dependencies) -> bool:
            if (
                isinstance(target_model, ModelNode)
                and target_model.is_external_node
                and target_model.unique_id in self.allowed_protected_refs
            ):
                return False

            return function(inner_self, node, target_model, dependencies)

        return outer_function

    def graph_member_wrapper(self, function) -> Callable:
        """Keep sources copied in for injected ephemerals out of selection.

        Injected ephemerals stay in the graph: `dbt ls`, graph selectors, and
        the docs DAG need them. They are not compiled on their own; see
        ``compile_node_wrapper``.
        """

        def outer_function(inner_self, unique_id) -> bool:
            if unique_id in self.injected_source_ids:
                return False
            return function(inner_self, unique_id)

        return outer_function

    def inject_external_wrapper(self, function) -> Callable:
        """After dbt injects the plugin nodes, add the upstream sources and
        upstream-project macros that the injected ephemerals need."""

        def outer_function(inner_self, *args, **kwargs) -> bool:
            modified = function(inner_self, *args, **kwargs)
            local_packages = set(getattr(inner_self, "all_projects", {}) or {})
            injected = self.inject_ephemeral_resources(
                inner_self.manifest, local_packages
            )
            return modified or injected

        return outer_function

    def inject_ephemeral_resources(self, manifest, local_packages: Set[str]) -> bool:
        """Add upstream sources and macros to ``manifest``. Anything the consumer
        defines itself wins. Copies left in a partial-parse manifest by an
        earlier run are replaced, since their package is not a local one."""

        def is_replaceable(existing) -> bool:
            return existing is None or existing.package_name not in local_packages

        def is_stale(resource, wanted: Dict[str, Dict]) -> bool:
            return (
                resource.package_name in self.manifests
                and resource.package_name not in local_packages
                and resource.unique_id not in wanted
            )

        modified = False
        for resources, wanted in (
            (manifest.sources, self.upstream_sources),
            (manifest.macros, self.upstream_macros),
        ):
            for unique_id in [
                unique_id
                for unique_id, resource in resources.items()
                if is_stale(resource, wanted)
            ]:
                del resources[unique_id]
                modified = True

        self.injected_source_ids = set()
        for source_id, source in self.upstream_sources.items():
            if not is_replaceable(manifest.sources.get(source_id)):
                continue
            try:
                manifest.sources[source_id] = SourceDefinition.from_dict(source)
            except Exception as error:
                fire_event(
                    msg=f"dbt-loom: Could not inject upstream source `{source_id}`: {error}"
                )
                continue
            self.injected_source_ids.add(source_id)
            modified = True

        for macro_id, macro in self.upstream_macros.items():
            if not is_replaceable(manifest.macros.get(macro_id)):
                continue
            try:
                manifest.macros[macro_id] = Macro.from_dict(macro)
            except Exception as error:
                fire_event(
                    msg=f"dbt-loom: Could not inject upstream macro `{macro_id}`: {error}"
                )
                continue
            modified = True

        if modified:
            # Manifest caches macro lookups by name and package.
            for cache in ("_macros_by_name", "_macros_by_package"):
                if hasattr(manifest, cache):
                    setattr(manifest, cache, None)

        for unique_id in self.ephemeral_models:
            node = manifest.nodes.get(unique_id)
            if node is None or not node.is_external_node:
                continue
            for source_id in self.models[unique_id].depends_on_sources:
                if source_id in manifest.sources:
                    node.depends_on.add_node(source_id)

        return modified

    def manifest_load_wrapper(self, function) -> Callable:
        """Validate injected ephemerals once dbt has finished building the manifest."""

        def outer_function(inner_self, *args, **kwargs):
            manifest = function(inner_self, *args, **kwargs)
            self.validate_ephemeral_dependencies(
                manifest, getattr(inner_self, "root_project", None)
            )
            return manifest

        return outer_function

    @property
    def ephemeral_models(self) -> Set[str]:
        return {
            unique_id for unique_id, model in self.models.items() if model.is_ephemeral
        }

    @staticmethod
    def available_vars(root_project, package_name: str) -> Optional[Set[str]]:
        """Names of the vars a node of ``package_name`` can read in the consumer,
        or None if they cannot be determined."""
        try:
            project_vars = root_project.vars.to_dict()
            names = set(project_vars)
            names.update(project_vars.get(package_name) or {})
            names.update(root_project.cli_vars or {})
        except AttributeError:
            return None
        return names

    def validate_ephemeral_dependencies(self, manifest, root_project=None) -> None:
        """Raise if an injected ephemeral that the consumer actually uses calls
        macros, sources or vars that are not available in the consumer project."""

        injected = {
            unique_id
            for unique_id in self.ephemeral_models
            if unique_id in manifest.nodes
            and manifest.nodes[unique_id].is_external_node
        }
        if not injected:
            return

        # Seeds have a MacroDependsOn, which has no `nodes`.
        stack = [
            dependency
            for node in manifest.nodes.values()
            if node.unique_id not in injected
            for dependency in getattr(node.depends_on, "nodes", ())
            if dependency in injected
        ]
        used: Set[str] = set()
        while stack:
            unique_id = stack.pop()
            if unique_id in used:
                continue
            used.add(unique_id)
            stack.extend(
                dependency
                for dependency in getattr(
                    manifest.nodes[unique_id].depends_on, "nodes", ()
                )
                if dependency in injected
            )

        missing_macros: Dict[str, Set[str]] = defaultdict(set)
        missing_sources: Dict[str, Set[str]] = defaultdict(set)
        missing_vars: Dict[str, Set[str]] = defaultdict(set)
        for unique_id in sorted(used):
            args = self.models[unique_id]
            for macro_id in args.depends_on_macros:
                if macro_id.split(".")[1] == "dbt" or macro_id in manifest.macros:
                    continue
                missing_macros[macro_id.split(".")[1]].add(unique_id)
            for source_id in args.depends_on_sources:
                if source_id not in manifest.sources:
                    missing_sources[source_id].add(unique_id)
            if root_project is not None:
                available = self.available_vars(root_project, args.package_name)
                if available is not None:
                    for var_name in required_vars(args.raw_code or "") - available:
                        missing_vars[var_name].add(unique_id)

        if not missing_macros and not missing_sources and not missing_vars:
            return

        lines = ["dbt-loom: Injected ephemeral models cannot be compiled here."]
        for package, models in sorted(missing_macros.items()):
            macros = sorted(
                macro_id
                for unique_id in models
                for macro_id in self.models[unique_id].depends_on_macros
                if macro_id.split(".")[1] == package and macro_id not in manifest.macros
            )
            lines.append(
                f"  - Missing macros from package `{package}` "
                f"({', '.join(macros)}) required by {', '.join(sorted(models))}. "
                f"Add `{package}` to this project's packages.yml (or dependencies.yml)"
                " and run `dbt deps`."
            )
        for source_id, models in sorted(missing_sources.items()):
            lines.append(
                f"  - Missing source `{source_id}` required by "
                f"{', '.join(sorted(models))}. Define it in this project."
            )
        for var_name, models in sorted(missing_vars.items()):
            lines.append(
                f"  - Missing var `{var_name}` required by "
                f"{', '.join(sorted(models))}. Set it under `vars:` in "
                "dbt_project.yml or pass it with `--vars`."
            )

        raise LoomEphemeralDependencyError("\n".join(lines))

    def group_validation_wrapper(self, function) -> Callable:
        """Wrap the check_valid_group_config_node function to inject upstream group names."""

        def outer_function(
            inner_self, groupable_node, valid_group_names: Set[str]
        ) -> bool:
            new_groups: Set[str] = {
                model.group for model in self.models.values() if model.group is not None
            }

            return function(
                inner_self, groupable_node, valid_group_names.union(new_groups)
            )

        return outer_function

    def dependency_wrapper(self, function) -> Callable:
        def outer_function(inner_self, node, target_model, dependencies) -> bool:
            if self.config is not None:
                for manifest_name in self.manifests.keys():
                    if manifest_name in dependencies:
                        continue

                    dependencies[manifest_name] = LoomRunnableConfig()

            return function(inner_self, node, target_model, dependencies)

        return outer_function

    def get_groups(self) -> Set[str]:
        """Get all groups defined in injected models."""

        return {
            model.group for model in self.models.values() if model.group is not None
        }

    def read_config(self, path: Path) -> Optional[dbtLoomConfig]:
        """Read the dbt-loom configuration file."""
        if not path.exists():
            fire_event(msg=f"dbt-loom: Config file `{path}` does not exist")
            return None

        with open(path) as file:
            config_content = file.read()

        config_content = self.replace_env_variables(config_content)

        return dbtLoomConfig(**yaml.load(config_content, yaml.SafeLoader))

    @staticmethod
    def replace_env_variables(config_str: str) -> str:
        """Replace environment variable placeholders in the configuration string."""
        pattern = r"\$(\w+)|\$\{([^}]+)\}"
        return re.sub(
            pattern,
            lambda match: os.environ.get(
                match.group(1) if match.group(1) is not None else match.group(2), ""
            ),
            config_str,
        )

    @staticmethod
    def filter_models(reference: ManifestReference, node: ManifestNode) -> bool:
        """Evaluate if a node should be included based on the node's package and the manifest reference."""
        if len(reference.included_packages) > 0:
            if node.package_name in reference.included_packages:
                return True
            return False

        if node.package_name not in reference.excluded_packages:
            return True

        return False

    def initialize(self) -> None:
        """Initialize the plugin"""

        if self.models != {} or not self.config:
            return

        for manifest_reference in self.config.manifests:
            fire_event(
                msg=f"dbt-loom: Loading manifest for `{manifest_reference.name}`"
                f" from `{manifest_reference.type.value}`"
            )

            manifest = self._manifest_loader.load(manifest_reference)
            if manifest is None:
                continue

            # Find the official project name from the manifest metadata and use that as the manifests key.
            manifest_name = manifest.get("metadata", {}).get(
                "project_name", manifest_reference.name
            )
            self.manifests[manifest_name] = manifest

            selected_nodes = identify_node_subgraph(manifest)

            # Remove nodes from excluded packages.
            filtered_nodes = add_ephemeral_dependencies(
                selected_nodes,
                {
                    key: value
                    for key, value in selected_nodes.items()
                    if self.filter_models(manifest_reference, value)
                },
            )

            loom_nodes = convert_model_nodes_to_model_node_args(filtered_nodes)

            merge_loom_nodes(self.models, loom_nodes, manifest_name)

            self.allowed_protected_refs.update(
                self.protected_refs_allowed_by(manifest_reference, filtered_nodes)
            )
            self.collect_ephemeral_resources(manifest, manifest_name, filtered_nodes)

    def protected_refs_allowed_by(
        self, reference: ManifestReference, nodes: Dict[str, ManifestNode]
    ) -> Set[str]:
        """Unique ids of protected ``nodes`` that ``reference`` allows consumers to ref."""
        include_ephemeral = (
            reference.include_protected_ephemeral
            if reference.include_protected_ephemeral is not None
            else bool(self.config and self.config.include_protected_ephemeral)
        )
        wanted = set(reference.allowed_protected_models)
        matched: Set[str] = set()
        allowed: Set[str] = set()

        for unique_id, node in nodes.items():
            names = {unique_id, node.name}
            if node.version:
                names.add(f"{node.name}.v{node.version}")
            hits = names & wanted
            matched |= hits
            if node.access != "protected":
                continue
            if hits or (include_ephemeral and node.is_ephemeral):
                allowed.add(unique_id)

        for entry in sorted(wanted - matched):
            fire_event(
                msg=f"dbt-loom: `{entry}` in allowed_protected_models for "
                f"`{reference.name}` does not match any injected model."
            )
        return allowed

    def collect_ephemeral_resources(
        self, manifest: Dict, manifest_name: str, nodes: Dict[str, ManifestNode]
    ) -> None:
        """Remember the upstream sources and upstream-project macros that the
        injected ephemerals need, so they can be added to the consumer."""
        upstream_macros = manifest.get("macros", {})
        upstream_sources = manifest.get("sources", {})

        macro_stack: List[str] = []
        for node in nodes.values():
            if not node.is_ephemeral:
                continue
            for source_id in node.depends_on_sources:
                if source_id in upstream_sources:
                    self.upstream_sources.setdefault(
                        source_id, upstream_sources[source_id]
                    )
            macro_stack.extend(node.depends_on_macros)

        # Third-party macro packages must be installed in the consumer; only
        # macros defined in the upstream project itself are copied over.
        while macro_stack:
            macro_id = macro_stack.pop()
            if macro_id in self.upstream_macros or macro_id not in upstream_macros:
                continue
            macro = upstream_macros[macro_id]
            if macro.get("package_name") != manifest_name:
                continue
            self.upstream_macros[macro_id] = macro
            macro_stack.extend((macro.get("depends_on") or {}).get("macros", []))

    @dbt_hook
    def get_nodes(self) -> PluginNodes:
        """
        Inject PluginNodes to dbt for injection into dbt's DAG.
        """
        fire_event(msg="dbt-loom: Injecting nodes")
        return PluginNodes(models=self.models)  # type: ignore


plugins = [dbtLoom]

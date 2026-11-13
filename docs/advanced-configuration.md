# Advanced Configuration

`dbt-loom` also has a couple advanced configuration options for power users.

## Using environment variables in the `dbt-loom` config

You can easily incorporate your own environment variables into the config file. This allows for dynamic configuration values that can change based on the environment. To specify an environment variable in the `dbt-loom` config file, use one of the following formats:

`${ENV_VAR}` or `$ENV_VAR`

### Example:

```yaml
manifests:
  - name: revenue
    type: gcs
    config:
      project_id: ${GCP_PROJECT}
      bucket_name: ${GCP_BUCKET}
      object_name: ${MANIFEST_PATH}
```

## Exclude nested packages

In some circumstances, like running `dbt-project-evaluator`, you may not want a
given package in an upstream project to be imported into a downstream project.
You can manually exclude downstream projects from injecting assets from packages
by adding the package name to the downstream project's `excluded_packages` list.

```yaml
manifests:
  - name: revenue
    type: file
    config:
      path: ../revenue/target/manifest.json
    excluded_packages:
      # Provide the string name of the package to exclude during injection.
      - dbt_project_evaluator
```

## Include only specific packages

In a mesh with bi-directional references between many projects, maintaining
`excluded_packages` lists can become unwieldy. As an alternative, you can use
`included_packages` to explicitly allowlist which packages from a referenced
manifest should be injected:

```yaml
manifests:
  - name: project_b
    type: file
    config:
      path: ../project_b/target/manifest.json
    included_packages:
      - project_b # Only import nodes owned by this project
```

> **Note:** `included_packages` and `excluded_packages` are mutually exclusive.
> You may only use one or the other, not both.

## Gzipped files

`dbt-loom` natively supports decompressing gzipped manifest files. This is useful to reduce object storage size and to minimize loading times when reading manifests from object storage. Compressed file detection is triggered when the file path for the manifest is suffixed
with `.gz`.

```yaml
manifests:
  - name: revenue
    type: s3
    config:
      bucket_name: example_bucket_name
      object_name: manifest.json.gz
```

## Upstream ephemeral models

Ephemeral models have no relation in the warehouse, so `dbt-loom` cannot point
at a table for them. Instead, it injects each upstream ephemeral model as a
real ephemeral model built from its `raw_code`. The consumer project compiles
that SQL itself and inlines it as a `__dbt__cte__<alias>` CTE, the same way
dbt handles ephemeral models from installed packages. This also works for
versioned ephemerals, for example `ref('upstream', 'my_model', v=3)` where v3
is ephemeral and v1 is a table.

Because the SQL is compiled in the consumer:

- `var()` calls resolve against the consumer's `vars` (and `--vars`), not the
  values used when the upstream manifest was built. If the ephemeral reads a
  var without a default (`var("start_date")`) that the consumer doesn't set,
  `dbt-loom` raises an error naming the var.
- Macros from third-party packages come from the consumer project. Every
  package whose macros the ephemeral calls (its `depends_on.macros`, for
  example `dbt_macros_core`) must be in the consumer's `packages.yml`. If one
  is missing, `dbt-loom` raises a compilation error that names the package,
  the macros and the ephemeral model.
- Macros defined in the upstream project itself (and the macros they call)
  are copied into the consumer from the upstream manifest.
- Sources the ephemeral reads with `source()` are copied from the upstream
  manifest. A source the consumer defines itself takes precedence. Injected
  sources are not selectable, so `dbt source freshness` ignores them.
- The models the ephemeral refs are injected too, even if `included_packages`
  or `excluded_packages` would filter them out.

The upstream manifest must include `raw_code` for the ephemeral (`dbt parse`
and `dbt compile` both write it). Ephemerals without `raw_code` are skipped.
`dbt ls --output json` shows the upstream `config.materialized` and `config.access`, not a default view with protected access.

Injected ephemerals are part of the consumer's lineage: they are nodes in
`target/manifest.json` (and its `parent_map` / `child_map`), they show up in
`dbt ls` and graph selectors such as `+my_model`, and they appear in the
`dbt docs` DAG, with edges from the upstream models they ref to the consumer
models that use them. `dbt compile`, `run`, `build` and `docs generate` do not
compile an injected ephemeral on its own. Its SQL is rendered only when a
downstream model inlines it as a CTE.

dbt rejects an ephemeral model with `access: public`. If a version is ephemeral and the manifest still has top-level `access: public` (the project default) while `config.access` is `protected` or `private`, dbt-loom uses that config value. If both say public, the ephemeral is injected as `protected` so the consumer parse does not fail.

Access rules still apply. Ephemerals are usually `protected`, which blocks refs
from other projects. To allow refs to protected upstream ephemerals, opt in
with `include_protected_ephemeral`, either for every manifest or per manifest
(the per-manifest value wins). Private ephemerals stay blocked.

```yaml
include_protected_ephemeral: true # Default: false
manifests:
  - name: icoach_source
    type: file
    config:
      path: ../icoach_source/target/manifest.json
  - name: revenue
    type: file
    include_protected_ephemeral: false # Overrides the top-level value
    config:
      path: ../revenue/target/manifest.json
```

## Allow specific protected models

By default, consumers can only reference `public` upstream models. To allow
specific `protected` models without making them public for everyone, list them
in `allowed_protected_models` on the manifest. Entries can be a model name
(all versions), a versioned name (`orders.v2`) or a unique_id. Private models
are never allowed, and entries that match nothing are logged.

```yaml
manifests:
  - name: revenue
    type: file
    config:
      path: ../revenue/target/manifest.json
    allowed_protected_models:
      - stg_orders
      - orders.v2
      - model.revenue.accounts
```

## Enabling Telemetry

By default, the `dbt-loom` plugin blocks outbound telemetry that reports on
the use of this plugin. This is a privacy-preserving measure for `dbt-loom`
users that does not impact the function of dbt-core and does not impede
dbt-core development in any way. If you _want_ this telemetry to be sent, you
can re-enable this behavior by setting the `enable_telemetry` property
in the `dbt_loom.config.yml` file.

```yaml
enable_telemetry: true
manifests: ...
```

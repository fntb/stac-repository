# Usage - "file" Backend Case

## Configuring and Activating the Repository

Let's first configure a new repository using the `"file"` backend in `~/.stac-repository.toml`.

> The `"file"` backend expects a single configuration option `"path"`, which is the path to the repository directory.

Let's create (or update) `~/.stac-repository.toml` :

```toml
[test]
    backend = "file"
    path = "~/test_catalog"
```

To ensure that `~/.stac-repository.toml` is valid we can run :

```bash
stac-repository show-repositories
```

```bash
 • test
```

We have a single repository is properly configured but currently unactivated.

> Since stac-repository can manage multiple repositories, all configured in `~/.stac-repository.toml`, we must
> distinguish the one we are currently working on by activating it with `stac-repository activate <repository>`

```bash
stac-repository activate test
```

And quick check of `stac-repository show-repositories` shows

```bash
 • test (active)
```

## Ingesting and Pruning Data

Suppose we have a STAC catalog we want to use as root for our new repository catalog in `~/data/catalog.json` we can ingest it using the default `stac-processor` `"stac"` with

```bash
stac-repository ingest ~/data/catalog.json
```

Note that any subsequent use of ingest will require the `--parent` option overwise the root catalog will be overwritten by whatever product we are trying to ingest.

> Ingestions are recursive, meaning that when ingesting a catalog all its descendants contained in the same base directory or under the same base url are ingested too. To ingest descendants out of this scope (potentially dangerous) use `--ingest-out-of-scope`.
>
> By default ingestions retrieve assets when accessible (i.e. on the local filesystem or over the web) and store them alongside their owner stac object in the repository. This behaviour can be disabled with the `--no-ingest-assets` option. To also ingest assets out of scope (potentially dangerous) use `--ingest-assets-out-of-scope`.

Suppose we have some product `~/data/products/product-1.json` we want to ingest in this new catalog. If the ingested catalog has `"root"` for id then we use

```bash
stac-repository ingest ~/data/products/product-1.json --parent root
```

Now if we want to remove this product, use

```bash
stac-repository prune product-1
```

# STAC Repository

<p align="center">
  <img src="https://stacspec.org/public/images-original/STAC-01.png" style="vertical-align: middle; max-width: 400px; max-height: 100px; margin: 20px;" />
  <img src="https://git-scm.com/images/logo@2x.png" alt="FastAPI" style="vertical-align: middle; max-width: 100px; max-height: 100px; margin: 20px;" />
</p>

A (git-)versionned [STAC](https://stacspec.org/en) catalog storage and management system.

Project in late stage development phase.

## Introduction and Features

`stac-repository` is a storage system and command-line interface for managing STAC catalogs. It implements advanced features necessary to build and maintain a complex STAC catalog :

- **Automated ingestions (Non-STAC)**

  Automated ingestion of non-STAC products via custom `stac-processors` modules. These are designed for ease of implementation via the `StacProcessor` Protocol. To view installed processors, run `stac-repository show-processors`.

- **Automated Ingestions (STAC)**

  Automated ingestion of STAC items and collections using a built-in `stac-processor`.

- **Backend Support**

  Support for multiple storage backends, including built-in Git+LFS `"git"` and local filesystem `"file"`. To view installed backends, run `stac-repository show-backends`. The architecture is also designed to facilitate the development of additional backends (e.g. FTP, NoSQL databases).

- **Transactional Operations**

  Transactional ingestions, updates, and deletions to ensure catalog integrity and atomicity

- **Immutable History**

  Immutable history of all transactions (note: this feature is not supported by the local filesystem backend).

- **Backup and Rollback**

  Backup and rollback of the catalog at any point in history.

- **Export**

  Export command to extract the catalog to the local filesystem independently of the underlying storage backend.

These capabilities make stac-repository a powerful tool for robust STAC catalog management.

## Installation

`stac-repository` is available directly on [pypi](https://pypi.org/project/stac-repository/).

```bach
pip install stac-repository
```

This installs two commands : `stac-repository`, and `stac-processor` to try out a processor without ingesting.

## Usage

```bash
stac-repository --help
```

```bash
Usage: stac_repository_cli [OPTIONS] COMMAND [ARGS]...

 🌍🛰️     STAC Repository

 The interface to manage STAC catalogs.

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --repository                TEXT  Repository URI - interpreted by the chosen backend. [default: None]                                                                                  │
│ --backend                   TEXT  Backend. [default: file]                                                                                                                             │
│ --install-completion              Install completion for the current shell.                                                                                                            │
│ --show-completion                 Show completion for the current shell, to copy it or customize the installation.                                                                     │
│ --help                            Show this message and exit.                                                                                                                          │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ version           Shows stac-repository version number.                                                                                                                                │
│ show-backends     Shows installed stac-repository backends.                                                                                                                            │
│ show-processors   Shows installed stac-repository processors.                                                                                                                          │
│ init              Initializes the repository.                                                                                                                                          │
│ config            Get or set the repository configuration options - interpreted by the chosen backend.                                                                                 │
│ ingest            Ingests some products from various sources (eventually using an installed processor).                                                                                │
│ prune             Removes some products from the catalog.                                                                                                                              │
│ history           Logs the catalog history.                                                                                                                                            │
│ rollback          Rollbacks the catalog to a previous commit. Support depends on the chosen backend.                                                                                   │
│ export            Exports the catalog. If a commit ref is specified, exports the catalog as it was at that point in time.                                                              │
│ backup            Backups the repository. If a commit ref is specified, backups the repository only up to this point in time.                                                          │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

Let's initialize a new repository (using the default `"file"` backend - implicit `--backend file`)

```bash
stac-repository --repository test_repository init
```

Since we didn't specify `--root-catalog` we are prompted for some basic information :

```
Initialize from an existing root catalog file ? (Leave blank to use the interactive initializer):
id (root): test_catalog
title: A Simple Demo Catalog
description: This is a simple demo catalog.
license (proprietary):
{
    'id': 'test_catalog',
    'description': 'This is a simple demo catalog.',
    'stac_version': '1.0.0',
    'links': [],
    'title': 'A Simple Demo Catalog',
    'type': 'Catalog',
    'license': 'proprietary'
}
Use as root catalog ? [y/n] (n): y
```

The newly created catalog :

```
test_repository/
test_repository/catalog.json
```

Let's ingest some STAC product using the default processor (implicit `--processor stac`)

```bash
stac-repository --repository test_repository ingest ~/test_catalogs/thermavolc/
```

```
 • ~/test_catalogs/thermavolc/ : Discovered products ~/test_catalogs/thermavolc/collection.json
 • ~/test_catalogs/thermavolc/collection.json : Cataloged
```

Our catalog now looks like this :

```
test_repository/
test_repository/orthophotos-Summit-20221116
test_repository/orthophotos-Summit-20221116/collection.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Visible
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Visible/20221116_Summit_Visible_orthophoto.png
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Visible/20221116_Summit_Visible_orthophoto.tif.aux.xml
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Visible/20221116_Summit_Visible_orthophoto.tif
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Visible/orthophoto-Summit-20221116-Visible.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/20221116_Summit_DTM.tif.aux.xml
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/orthophoto-Summit-20221116-DTM.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/20221116_Summit_DTM.png
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/20221116_Summit_DTM.tif
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/20221116_Summit_Thermal_orthophoto.png
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/20221116_Summit_Thermal_orthophoto.tif
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/orthophoto-Summit-20221116-Thermal.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/20221116_Summit_Thermal_orthophoto.tif.aux.xml
test_repository/catalog.json
```

Finally let's delete some product

```bash
stac-repository --repository test_repository prune orthophoto-Summit-20221116-Visible
```

```
 • orthophoto-Summit-20221116-Visible : Uncataloged
```

Finally our demo catalog looks like this :

```
test_repository/
test_repository/orthophotos-Summit-20221116
test_repository/orthophotos-Summit-20221116/collection.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/20221116_Summit_DTM.tif.aux.xml
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/orthophoto-Summit-20221116-DTM.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/20221116_Summit_DTM.png
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-DTM/20221116_Summit_DTM.tif
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/20221116_Summit_Thermal_orthophoto.png
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/20221116_Summit_Thermal_orthophoto.tif
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/orthophoto-Summit-20221116-Thermal.json
test_repository/orthophotos-Summit-20221116/orthophoto-Summit-20221116-Thermal/20221116_Summit_Thermal_orthophoto.tif.aux.xml
test_repository/catalog.json
```

## Motivation - _Why this Project ?_

**Full motivation is detailed in [`motivation.md`](https://github.com/fntb/stac-repository/blob/main/docs/motivation.md).**

Presenting scientific data products as STAC catalogs is primarily motivated by the objective of achieving data [FAIR](https://en.wikipedia.org/wiki/FAIR_data)-ness (Findable, Accessible, Interoperable, Reusable). The mature [STAC ecosystem](https://stacindex.org/ecosystem) makes building such a catalog relatively straightforward for simple cases (e.g. [pystac | Creating a Landsat 8 STAC](https://pystac.readthedocs.io/en/stable/tutorials.html#creating-a-landsat-8-stac)).

However, building and maintaining a complex STAC catalog - one subject to incremental changes over an extended period and encompassing diverse product types (e.g., satellite scenes, InSAR interferograms, InSAR time series) - introduces significant challenges. Effective maintenance necessitates capabilities for data rollback, backup, and exploring historical changes. And routine data ingestion requires automation of product conversion and ingestion, which itself requires transactional operations to ensure data integrity.

It is precisely to address these complex catalog management challenges that `stac-repository` was developed. It provides a robust storage system and CLI that integrates with and abstracts away the complexities of the underlying chosen backend (like Git+LFS). This approach allows `stac-repository` to offer transactional integrity, immutable history, backup/rollback capabilities, by treating the STAC catalog as a versioned data product, without requiring users to directly interact with the underlying backend.

While `stac-repository` greatly simplifies complex STAC catalog management, the underlying architecture introduces limitations. The Git+LFS backend, for instance, provides strong versioning capabilities but introduces a dependency on Git and Git LFS, which may require some foundational understanding for advanced operations or troubleshooting. For extremely large catalogs with millions of items or very high update frequencies, performance characteristics of the current backends will not be enough. While the local filesystem backend simplifies setup, it foregoes the immutable history provided by Git-based backends.

## The Processor Protocol

A processor is a python module implementing the processor protocol described [processor.py](https://github.com/fntb/stac-repository/blob/main/stac_repository/processor.py).

An example can be found in [`stac-processor.py`](https://github.com/fntb/stac-repository/blob/main/stac_repository/stac_processor.py)

## Source & Contributing

```bash
just --list
```

See [the Justfile](https://github.com/fntb/stac-repository/blob/main/justfile).

## History

**stac-repository** is being actively developped at the [OPGC](https://opgc.uca.fr/) an observatory for the sciences of the universe (OSU) belonging to the [CNRS](https://www.cnrs.fr/en) and the [UCA](https://www.uca.fr/) by its main author Pierre Fontbonne [@fntb](https://github.com/fntb).

## License

[OPEN LICENCE 2.0](https://github.com/fntb/stac-repository/blob/main/LICENCE.txt)

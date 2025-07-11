# STAC Repository

A (git-)versionned [STAC](https://stacspec.org/en) catalog and catalog management system.

Project in late stage development phase.

## Project Goal - _What does it do ?_

is a high level storage system for STAC ressources implementing features auch as : transactional ingestions and deletions, immutable history, and backup., plugable provider processors to automate ingestions.

Two backends currently exists : Git(+GitLFS) and Filesystem

`stac-repository` is a higher-level library and cli that uses **Git** (with [Git LFS](https://git-lfs.com/)) to build, host, and maintain a large and complex [**STAC**](https://stacspec.org/en) catalog.

As **data hosts** we interact with **data consumers** - who query our catalog and work with our products - and **data providers** - who create products that we retrieve, process (to STAC objects) and store.

This project offers the following low-level abstractions :

- Read a STAC catalog from a specific commit
- Manage catalog mutations as transactions
- Navigate the commit log by STAC object ids instead of files, and differentiate insertions, updates, and deletions of such objects

On top of this base we built a higher-level abstraction : A simple `Processor` [Protocol](https://typing.readthedocs.io/en/latest/spec/protocol.html) to build custom processors.

A **processor** is a python module, responsible for discovering, processing, and cataloging products from a data provider.

This interface is intended to be simple to implement while abstracting the ingestion / deletion process enough that `stac-repository` can handle higher-level operations, such as ingesting from a data producer, pruning a managed product, or restricting which operations are allowed with specific transaction types (CRUD).

An admin **CLI** and a **containered distribution** [WIP] is provided to manage the STAC Repository.

## CLI Demo

## The Processor Protocol

A processor is a python module implementing the processor protocol described [in this file](stac_repository/managed/processor.py).

An example can be found in [`stac-processor-demo`](./stac_processor_demo/)

## Project Motivation - _Why is it needed ? What problem does it solve ?_

The choice of presenting a scientific product as a STAC catalog can be fairly easily motivated by an objective of data [FAIR](https://en.wikipedia.org/wiki/FAIR_data)-ness (Findable, Accessible, Interoperable, Reusable). The [STAC ecosystem](https://stacindex.org/ecosystem) is developped enough that building such a catalog is fairly straightforward (e.g. [pystac | Creating a Landsat 8 STAC](https://pystac.readthedocs.io/en/stable/tutorials.html#creating-a-landsat-8-stac)).

On the other hand, building and maintaining a complex STAC catalog, one which will be subject to incremental changes over a long period of time and will contain many different types of products (e.g. satellite scenes, InSAR interferograms, InSAR time series, ..) requires some additional considerations. Specifically, for feasible maintenance, we need the ability to rollback and backup the data, or explore the change log.

The data problem is as such :

- Complex data where each datum is comprised of many large files potentially organized in an important (must be preserved) directory structure
- Wildly differing metadata accross data types, not always consistent

Traditional databases are not built to handle such a dataset.
Document-oriented databases could well handle the metadata, and even products composed of a single large files, but complex outputs composed of many large files organized in subdirectories would remain a challenge.

The most natural solution appears to simply build a static STAC catalog directly on the filesystem.

The issue then, is to implement history, rollback, backup over the filesystem, in short, data versionning.
A natural solution to this problem is git but obviously large data isn't git original purpose. Something better suited to large data is required, some sort of _data_ version control system.

Exploration lead us to specialized tooling such as [DVC | Data Version Control](https://dvc.org/), [lakeFS](https://lakefs.io), or [Git LFS](https://git-lfs.com/).

Assuming this design choice, an additional advantage is a new way for data consumers to discover and work with the data.
The whole catalog metadata (the STAC .json files and directory structure) can be fetched and synced (forked and pulled)
directly from the dataset (which is now a simple git repository). Exploration can then be done with tooling such as [`pystac`](https://pystac.readthedocs.io/en/stable/), and (large) data fetching with [`git lfs fetch`](https://github.com/git-lfs/git-lfs/wiki/Tutorial)

## Source & Contributing

Python version : `3.12`

### Venv

```bash
python3.12 -m venv .venv
source .venv/bin/activate
# ...
deactivate
```

### Dependencies

```bash
pip install .[dev,cli]
```

- `[cli]` is required to use the CLI,
- `[dev]` to run the demo or the test suite

### Test Suite

```bash
pytest -vv
```

### Generating the CLI Demo

```bash
python make_docs.py
```

# Motivation - _Why this Project ?_

Presenting scientific data products as STAC catalogs is primarily motivated by the objective of achieving data [FAIR](https://en.wikipedia.org/wiki/FAIR_data)-ness (Findable, Accessible, Interoperable, Reusable). The mature [STAC ecosystem](https://stacindex.org/ecosystem) makes building such a catalog relatively straightforward for simple cases (e.g. [pystac | Creating a Landsat 8 STAC](https://pystac.readthedocs.io/en/stable/tutorials.html#creating-a-landsat-8-stac)).

#### Challenge of Complex Catalogs

However, building and maintaining a complex STAC catalog - one subject to incremental changes over an extended period and encompassing diverse product types (e.g., satellite scenes, InSAR interferograms, InSAR time series) - introduces significant challenges. Effective maintenance necessitates capabilities for data rollback, backup, and exploring historical changes. And routine data ingestion requires automation of product conversion and ingestion, which itself requires transactional operations to ensure data integrity.

Addressing these challenges requires a solution capable of handling specific data characteristics :

- Complex Data Structures

  Complex data where each datum often consist of multiple large files, necessitating the preservation of intricate directory structures.

- Diverse and Inconsistent Metadata

  Metadata varies significantly across different data types and may not always adhere to strict consistency rules.

#### Evaluating Existing Solutions

Traditional relational databases are generally ill-suited for managing such datasets. While document-oriented databases can effectively handle metadata and even single-file products, they still struggle with complex outputs comprising numerous large files organized within intricate subdirectory structures.

Consequently, building a static STAC catalog directly on the filesystem emerges as the most intuitive initial approach. However, this immediately presents the challenge of implementing essential capabilities like history tracking, rollback, and backup directly over the filesystem - in essence, data versioning

A natural solution of this problem points to Git for version control. However, Git's design is not optimized for very large data files. This necessitates a solution specifically engineered for large-scale data. Our exploration led us to specialized data versioning tools, including [DVC | Data Version Control](https://dvc.org/), [lakeFS](https://lakefs.io), or [Git LFS](https://git-lfs.com/).

#### `stac-repository`: A Solution for Complex STAC Catalog Management

It is precisely to address these complex catalog management challenges that `stac-repository` was developed. It provides a robust storage system and CLI that integrates with and abstracts away the complexities of the underlying chosen backend (like Git+LFS). This approach allows `stac-repository` to offer transactional integrity, immutable history, backup/rollback capabilities, by treating the STAC catalog as a versioned data product, without requiring users to directly interact with the underlying backend.

#### Additional Advantages for Data Consumers (Git+LFS backend)

This design choice also offers significant advantages for data consumers, providing new way to discover and work with the data. The entire catalog metadata (comprising the STAC `*.json` files and directory structure) can be easily fetched and synchronized (i.e. forked and pulled) directly from the dataset, which effectively becomes a standard Git repository. Catalog exploration can then be performed with familiar tooling like [`pystac`](https://pystac.readthedocs.io/en/stable/), while the large data assets can be efficiently fetched using [`git lfs fetch`](https://github.com/git-lfs/git-lfs/wiki/Tutorial).

#### Considerations and Trade-offs

While `stac-repository` greatly simplifies complex STAC catalog management, the underlying architecture introduces limitations. The Git+LFS backend, for instance, provides strong versioning capabilities but introduces a dependency on Git and Git LFS, which may require some foundational understanding for advanced operations or troubleshooting. For extremely large catalogs with millions of items or very high update frequencies, performance characteristics of the current backends will not be enough. While the local filesystem backend simplifies setup, it foregoes the immutable history provided by Git-based backends.

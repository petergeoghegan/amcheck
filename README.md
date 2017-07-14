# amcheck: Verify the logical consistency of PostgreSQL B-Tree indexes

Current version: 0.3

Author: Peter Geoghegan [`<pg@bowt.ie>`](mailto:pg@bowt.ie)

License: <a href="https://opensource.org/licenses/postgresql">PostgreSQL license</a>

Supported versions: PostgreSQL 9.4, PostgreSQL 9.5, PostgreSQL 9.6

## Overview

The `amcheck` module provides functions that allow you to verify the logical
consistency of the structure of PostgreSQL indexes.  If the structure appears
to be valid, no error is raised.  Currently, only B-Tree indexes are supported,
although since in practice the majority of PostgreSQL indexes are B-Tree
indexes, `amcheck` is likely to be effective as a general corruption smoke-test
in production PostgreSQL installations.

See "Using amcheck effectively" below for information about the kinds of
real-world problems `amcheck` is intended to detect.

### Invariants

`amcheck` provides functions that specifically verify various *invariants* in
the structure of the representation of particular indexes.  The correctness of
the access method functions behind index scans and other important operations
is predicated on these invariants always holding.  For example, certain
functions verify, among other things, that all B-Tree pages have items in
"logical", sorted order (e.g., for B-Tree indexes on text, index tuples should
be in collated lexical order).  If that particular invariant somehow fails to
hold, we can expect binary searches on the affected page to incorrectly guide
index scans, resulting in wrong answers to SQL queries.  Problems like this can
be very subtle, and might otherwise remain undetected.

Verification is performed using the same procedures as those used by index
scans themselves, which may be user-defined operator class code.  For example,
B-Tree index verification relies on comparisons made with one or more B-Tree
support function 1 routines, much like B-Tree index scans rely on the routines
to guide the scan to a point in the underlying table;  see
http://www.postgresql.org/docs/current/static/xindex.html for details of
operator class support functions.

### Project background

`amcheck` is a contrib extension for PostgreSQL 10.  This externally maintained
version of the extension exists to target earlier versions of PostgreSQL
(PostgreSQL 9.4 - PostgreSQL 9.6).

### Bugs

Report bugs using the <a
href="https://github.com/petergeoghegan/amcheck/issues">Github issue
tracker</a>.

## Installation

### Building using PGXS (generic)

The module can be built using the standard PGXS infrastructure.  For this to
work, you will need to have the `pg_config` program available in your $PATH.

If you are using a packaged PostgreSQL build and have `pg_config` available
(and in your OS user's $PATH), the procedure is as follows:

```shell
tar xvzf amcheck-0.3.tar.gz
cd amcheck-0.3
make
make install
```

Note that just because `pg_config` is located in one user's $PATH does not
necessarily make it so for the root user.

#### Building Debian/Ubuntu packages

The Makefile also provides a target for building Debian packages. The target
has a dependency on `debhelper`, `devscripts`, `postgresql-server-dev-all`, and
the PostgreSQL source package itself (e.g. `postgresql-server-dev-9.4`).

The packages can be created and installed from the amcheck directory as
follows:

```shell
sudo aptitude install debhelper devscripts postgresql-server-dev-all
make deb
sudo dpkg -i ./build/postgresql-9.4-amcheck_*.deb
```

### Setting up PostgreSQL

Once `amcheck` is built and installed, it should be created as a PostgreSQL
extension in every database that requires it:

`mydb=# CREATE EXTENSION amcheck;`

`amcheck` functions may be used only by superusers.

## Interface

The `amcheck` extension has a simple interface.  `amcheck` consists of just a
few functions that can be used for verification of a named B-Tree index.  Note
that currently, no function inspects the structure of the underlying heap
representation (table).

`regclass` function arguments are used by `amcheck` to identify particular
index relations.  This allows `amcheck` to accept arguments using various
SQL calling conventions:

```sql
  -- Use string literal regclass input:
  SELECT bt_index_check('pg_database_oid_index');
  -- Use oid regclass input (both perform equivalent verification):
  SELECT bt_index_check(2672);
  SELECT bt_index_check(oid) FROM pg_class
  WHERE relname = 'pg_database_oid_index';
```

See the <a
href="http://www.postgresql.org/docs/current/static/datatype-oid.html">PostgreSQL
documentation on Object identifier types</a> for more information.

### `bt_index_check(index regclass) returns void`

`bt_index_check` tests that its target, a B-Tree index, respects a variety of
invariants.  Example usage:

```sql
  SELECT bt_index_check(c.oid), c.relname, c.relpages
  FROM pg_index i
  JOIN pg_opclass op ON i.indclass[0] = op.oid
  JOIN pg_am am ON op.opcmethod = am.oid
  JOIN pg_class c ON i.indexrelid = c.oid
  JOIN pg_namespace n ON c.relnamespace = n.oid
  WHERE am.amname = 'btree' AND n.nspname = 'pg_catalog'
  -- Don't check temp tables, which may be from another session:
  AND c.relpersistence != 't'
  -- Function may throw an error when this is omitted:
  AND i.indisready AND i.indisvalid
  ORDER BY c.relpages DESC LIMIT 10;
```
```shell
   bt_index_check |             relname             | relpages 
  ----------------+---------------------------------+----------
                  | pg_depend_reference_index       |       43
                  | pg_depend_depender_index        |       40
                  | pg_proc_proname_args_nsp_index  |       31
                  | pg_description_o_c_o_index      |       21
                  | pg_attribute_relid_attnam_index |       14
                  | pg_proc_oid_index               |       10
                  | pg_attribute_relid_attnum_index |        9
                  | pg_amproc_fam_proc_index        |        5
                  | pg_amop_opr_fam_index           |        5
                  | pg_amop_fam_strat_index         |        5
```

This example shows a session that performs verification of every catalog index
in the database "test".  Details of just the 10 largest indexes verified are
displayed.  Since no error is raised, all indexes tested appear to be logically
consistent.  Naturally, this query could easily be changed to call
`bt_index_check` for every index in the database where verification is
supported.  An `AccessShareLock` is acquired on the target index by
`bt_index_check`.  This lock mode is the same lock mode acquired on relations
by simple `SELECT` statements.

`bt_index_check` does not verify invariants that span child/parent
relationships, nor does it verify that the target index is consistent with its
heap relation.  When a routine, lightweight test for corruption is required in
a live production environment, using `bt_index_check` often provides the best
trade-off between thoroughness of verification and limiting the impact on
application performance and availability.

### `bt_index_parent_check(index regclass) returns void`

`bt_index_parent_check` tests that its target, a B-Tree index, respects a
variety of invariants.  The checks performed by `bt_index_parent_check` are a
superset of the checks performed by `bt_index_check`.  `bt_index_parent_check`
can be thought of as a more thorough variant of `bt_index_check`: unlike
`bt_index_check`, `bt_index_parent_check` also checks invariants that span
parent/child relationships.  However, it does not verify that the target index
is consistent with its heap relation.  `bt_index_parent_check` follows the
general convention of raising an error if it finds a logical inconsistency or
other problem.

An `ExclusiveLock` is required on the target index by bt_index_parent_check (a
`ShareLock` is also acquired on the heap relation).  These locks prevent
concurrent data modification from `INSERT`, `UPDATE`, and `DELETE` commands.
The locks also prevent the underlying relation from being concurrently
processed by `VACUUM` (and other utility commands).  Note that the function
holds locks for as short a duration as possible, so there is no advantage to
verifying each index individually in a series of transactions, unless long
running queries happen to be of particular concern.

`bt_index_parent_check`'s additional verification is more likely to detect
various pathological cases.  These cases may involve an incorrectly implemented
B-Tree operator class used by the index that is checked, or, hypothetically,
undiscovered bugs in the underlying B-Tree index access method code.  Note that
`bt_index_parent_check` cannot be called when Hot Standby is enabled (i.e., on
read-only physical replicas), unlike `bt_index_check`.

## Using amcheck effectively

### Causes of corruption

`amcheck` can be effective at detecting various types of failure modes that
data page checksums will always fail to catch.  These include:

* Structural inconsistencies caused by incorrect operator class
  implementations.

This includes issues caused by the comparison rules of operating system
collations changing.  Comparisons of datums of a collatable type like `text`
must be immutable (just as all comparisons used for B-Tree index scans must be
immutable), which implies that operating system collation rules must never
change.

Though rare, updates to operating system collation rules can cause these
issues.  More commonly, an inconsistency in the collation order between a
master server and a standby server is implicated, possibly because the *major*
operating system version in use is inconsistent.  Such inconsistencies will
generally only arise on standby servers, and so can generally only be detected
on standby servers.

If a problem like this arises, it may not affect each individual index that is
ordered using an affected collation, simply because *indexed* values might
happen to have the same absolute ordering regardless of the behavioral
inconsistency.

* Corruption caused by hypothetical undiscovered bugs in the underlying
  PostgreSQL access method code or sort code.

Automatic verification of the structural integrity of indexes plays a role in
the general testing of new or proposed PostgreSQL features that could plausibly
allow a logical inconsistency to be introduced.  One obvious testing strategy
is to call `amcheck` functions continuously when running the standard
regression tests.

* Filesystem or storage subsystem faults where checksums happen to simply not
  be enabled.

Note that `amcheck` examines a page as represented in some shared memory buffer
at the time of verification if there is only a shared buffer hit when accessing
the block.  Consequently, `amcheck` does not necessarily examine data read from
the filesystem at the time of verification.  Note that when checksums are
enabled, `amcheck` may raise an error due to a checksum failure when a corrupt
block is read into a buffer.

* Corruption caused by faulty RAM, and the broader memory subsystem and
  operating system.

PostgreSQL does not protect against correctable memory errors and it is assumed
you will operate using RAM that uses industry standard Error Correcting Codes
(ECC) or better protection.  However, ECC memory is typically only immune to
single-bit errors, and should not be assumed to provide *absolute* protection
against failures that result in memory corruption.

### Overhead

The overhead of calling `bt_index_check` for every index on a live production
system is roughly comparable to the overhead of vacuuming; like `VACUUM`,
verification uses a "buffer access strategy", which limits its impact on which
pages are cached within `shared_buffers`.  A major design goal of `amcheck` is
to support routine verification of all indexes on busy production systems.

No `amcheck` routine will ever modify data, and so no pages will ever be
"dirtied", which is not the case with `VACUUM`.  On the other hand, `amcheck`
may be required to verify a large number of indexes all at once, which is
typically not a behavior that autovacuum exhibits.  `amcheck` exhaustively
accesses every page in each index verified.  This behavior is useful in part
because verification may detect a checksum failure, which may have previously
gone undetected only because no process needed data from the corrupt page in
question, including even an autovacuum worker process.

Note also that `bt_index_check` and `bt_index_parent_check` access the contents
of indexes in "logical" order, which, in the worst case, implies that all I/O
operations are performed at random positions on the filesystem.  In contrast,
`VACUUM` always removes dead index tuples from B-Tree indexes while accessing
the contents of B-Tree indexes in sequential order.

### Acting on information about corruption

No error concerning corruption raised by `amcheck` should ever be a false
positive.  In practice, `amcheck` is more likely to find software bugs than
problems with hardware.  `amcheck` raises errors in the event of conditions
that, by definition, should never happen.  It seems unlikely that there could
ever be a useful *general* remediation to problems it detects.

In general, an explanation for the root cause of an invariant violation should
be sought.  The <a
href="https://www.postgresql.org/docs/current/static/pageinspect.html">pageinspect</a>
tool can play a useful role in diagnosing corruption that `amcheck` highlights.
A `REINDEX` may or may not be effective in repairing corruption, depending on
the exact details of how the corruption originated.

In general, `amcheck` can only prove the presence of corruption; it cannot
prove its absence.

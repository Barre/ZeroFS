+++
title = "ZeroFS vs. Amazon S3 Files"
description = "Amazon S3 Files writes to an AWS-managed high-performance storage tier, then exports changes to S3. ZeroFS persists its own filesystem format directly to object storage."
date = 2026-07-11
authors = ["Pierre Barre"]

[extra]
display_date = "July 11, 2026"
reading_time = "8 min read"
summary = "S3 Files maintains ordinary S3 objects through a separate export step. ZeroFS instead packs, compresses, and encrypts file data in an internal format."
og_description = "A comparison of Amazon S3 Files and ZeroFS: storage layout, fsync behavior, S3 interoperability, rename cost, and pricing."
+++

<p class="lede rv">Amazon S3 Files and ZeroFS expose POSIX filesystems backed by object storage, but the shared interface hides opposite bucket layouts. The choice turns on the role of the bucket: if files must remain ordinary S3 objects, S3 Files preserves that identity; if the bucket can be an internal persistence layer, ZeroFS trades direct S3 access for packing, compression, and client-side encryption.</p>

<h2 class="rv" id="storage-layout">Storage layout</h2>
<p class="rv">The defining property of <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files.html">S3 Files, which is built using Amazon EFS</a>, is that <code>images/cat.jpg</code> on the mount corresponds to the same key in the bucket, with changes flowing in both directions. Active data and metadata reside in a low-latency tier that AWS calls “high-performance storage.”</p>
<p class="rv">That one-file, one-object identity is deliberately absent from the <a href="https://www.zerofs.net/docs/architecture">ZeroFS storage format</a>. Metadata lives in an LSM tree; file contents are split into extents, compressed and encrypted, then packed into immutable segment objects. An S3 client sees an opaque internal layout rather than the mounted files.</p>
<figure class="rv">
  <div class="diagram"><pre class="mermaid">
flowchart TB
    subgraph CLIENTS["Client Layer"]
        NFS["NFS Client"]
        P9["9P Client"]
        NBD["NBD Client"]
        WEB["Web Browser"]
    end
    subgraph CORE["ZeroFS Core"]
        NFSD["NFS Server"]
        P9D["9P Server"]
        NBDD["NBD Server"]
        WEBUI["Web UI"]
        VFS["Virtual Filesystem"]
        SEG["Segment store: compressed, encrypted file-data frames"]
        SLATE["LSM tree: metadata and 32-byte extent pointers"]
        CACHE["Local Cache"]
        NFSD --> VFS
        P9D --> VFS
        NBDD --> VFS
        WEBUI --> VFS
        VFS --> SEG
        VFS --> SLATE
        SEG --> CACHE
        SLATE --> CACHE
    end
    subgraph BACKEND["Storage Backend"]
        SEGOBJ["Immutable segment objects"]
        SSTS["Metadata SSTs and manifest"]
        S3["S3 Object Store"]
        CACHE --> SEGOBJ
        CACHE --> SSTS
        SEGOBJ --> S3
        SSTS --> S3
    end
    NFS --> NFSD
    P9 --> P9D
    NBD --> NBDD
    WEB --> WEBUI
  </pre></div>
  <figcaption>ZeroFS keeps file data and filesystem metadata on separate paths until both reach the object store.</figcaption>
</figure>
<p class="rv">Both mounts use the client page cache. The write-path row below starts after the client sends the write to the server.</p>

<div class="table-wrap rv">
  <table>
    <thead><tr><th></th><th>Amazon S3 Files</th><th>ZeroFS</th></tr></thead>
    <tbody>
      <tr><td>Storage model</td><td>AWS-managed high-performance storage synchronized with a bucket; internal layout not documented</td><td>An LSM tree and immutable data segments on object storage</td></tr>
      <tr><td>Object layout</td><td>One file maps to one S3 object</td><td>Metadata in an LSM tree; file-data frames packed into segments</td></tr>
      <tr><td>Write path after client cache</td><td>NFS write to high-performance storage, durable immediately; asynchronous S3 export after write inactivity</td><td>Over 9P, <code>fsync</code> uploads data segments and flushes LSM metadata to object storage before returning success</td></tr>
      <tr><td>Cold read and read-ahead</td><td>Linux NFS read-ahead, directory metadata import, optional small-file import, or direct S3 read</td><td>LSM lookup, then adaptive object and cross-segment frame prefetch</td></tr>
      <tr><td>S3 API access to files</td><td>Yes; filesystem changes appear after asynchronous export</td><td>No; reading files requires ZeroFS and the encryption password</td></tr>
      <tr><td>Client interface</td><td>NFS 4.1/4.2</td><td>9P or NFS for files; NBD for blocks</td></tr>
      <tr><td>Object-store choice</td><td>Amazon S3</td><td>Amazon S3, S3-compatible stores, Azure Blob, or Google Cloud Storage</td></tr>
      <tr><td>Cost model</td><td>S3 plus high-performance storage, file access, and synchronization charges</td><td>Object storage and requests, plus the compute and cache running ZeroFS</td></tr>
    </tbody>
  </table>
</div>

<h2 class="rv" id="object-interoperability">Object interoperability</h2>
<p class="rv">Keeping that one-to-one mapping means a file written through the mount <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization.html">eventually becomes a normal S3 object</a>. Export begins after 60 seconds without a write, so continued writes postpone S3 visibility. Once export completes, existing tools can read the object with <code>GetObject</code>, and S3-side changes flow back into the filesystem. If both sides modify the same file before synchronization, S3 wins and the file-side version moves to <code>lost+found</code>.</p>
<p class="rv">A ZeroFS pathname cannot be fetched with the S3 API, and a segment cannot be scanned as Parquet. In exchange, small files need neither one data object nor one PUT each: their extents are compressed, encrypted, and packed together. The bucket and raw local cache contain ciphertext, so mounting or recovery requires ZeroFS and its encryption password. The <a href="https://www.zerofs.net/docs/encryption">encryption documentation</a> lists the structural metadata that remains visible.</p>
<p class="rv">If other applications need immediate S3 visibility after a filesystem write, neither model provides it: S3 Files exports asynchronously, while ZeroFS never exposes mounted files through the S3 API.</p>

<h2 class="rv" id="cold-access">Cold access</h2>
<p class="rv">The first S3 Files access can trigger an import: listing a directory loads every object's metadata and asynchronously copies files below the import threshold, 128 KiB by default, into high-performance storage. <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization.html">AWS says a first listing of 1,000 objects may take several seconds</a>. Larger files stay in S3, and <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-performance.html">reads of at least 1 MiB go directly to S3</a>.</p>
<p class="rv">ZeroFS instead populates a <a href="https://www.zerofs.net/docs/caching">local RAM and disk cache</a> from reads and uploads. Newly sealed segments enter the cache from bytes already in hand, so read-after-write needs no GET; the cache is not write-back, and writes still reach object storage when segments are sealed and metadata is flushed. A cold miss resolves the requested extents through the LSM tree and coalesces adjacent frames into ranged GETs. It pays the first object-store round trip without importing the rest of the file or every small file in the directory; once the working set fits locally, data reads generate few S3 requests.</p>
<p class="rv">Both paths add read-ahead for sequential traffic: ZeroFS prefetches within and across segment objects, while S3 Files relies on Linux NFS read-ahead alongside its direct-S3 routing.</p>

<h2 class="rv" id="cost">Cost</h2>
<p class="rv">S3 storage and request charges apply in either case. S3 Files also charges for its high-performance storage tier: resident storage per GB-month and reads and writes per GB. The <a href="https://aws.amazon.com/s3/pricing/">AWS pricing example</a> current at publication uses these rates:</p>
<div class="table-wrap rv">
  <table>
    <thead><tr><th>S3 Files charge</th><th>Rate used in AWS's example</th><th>When it applies</th></tr></thead>
    <tbody>
      <tr><td>High-performance storage</td><td>$0.30 per GB-month</td><td>Files have a 10 KiB minimum billable size</td></tr>
      <tr><td>File reads</td><td>$0.03 per GB</td><td>Reads from high-performance storage, including exports to S3</td></tr>
      <tr><td>File writes</td><td>$0.06 per GB</td><td>Writes to high-performance storage, including imports from S3</td></tr>
    </tbody>
  </table>
</div>
<p class="rv">The amount resident in high-performance storage depends on the configured <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization-customizing.html">import threshold and expiration window</a>. Listing imports metadata and eligible small files, reads can load more data, and <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization.html">file metadata does not expire</a>. Imports are metered as filesystem writes and exports as reads; the <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-metering.html">minimum operation sizes</a> are 32 KiB for file data and 4 KiB for metadata. AWS reports resident bytes and inodes in <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-monitoring-cloudwatch.html">CloudWatch</a>, but total bucket size alone cannot forecast the bill.</p>
<h3 class="rv" id="cost-example-10-tb">Illustrative model: store 10,000 GiB and read it once</h3>
<p class="rv"><strong>This is an illustrative scenario, not a general cost comparison.</strong> It uses 10,000 AWS billable GB (GiB) of logical data in S3 Standard in <code>us-east-1</code>, at <a href="https://aws.amazon.com/s3/pricing/">$0.023 per GB-month</a>. The direct case assumes 1 MiB reads from data already in S3; the resident case assumes reads below 1 MiB with all data also in high-performance storage. Internet egress is excluded.</p>
<div class="table-wrap wide-table rv">
  <table>
    <thead><tr><th>Case</th><th>Storage/month</th><th>Read once</th><th>Illustrative subtotal</th></tr></thead>
    <tbody>
      <tr><td>ZeroFS (2:1)</td><td>~$115 + overhead</td><td>S3 GETs</td><td>~$115 + infrastructure</td></tr>
      <tr><td>ZeroFS (1:1)</td><td>~$230 + overhead</td><td>S3 GETs</td><td>~$230 + infrastructure</td></tr>
      <tr><td>S3 Files (1 MiB direct reads)</td><td>~$230 + metadata</td><td>~$1.17 + S3 GETs</td><td>~$231 + metadata and requests</td></tr>
      <tr><td>S3 Files (small resident reads)</td><td>$3,230</td><td>$300</td><td>$3,530<br>+$600 if imported</td></tr>
    </tbody>
  </table>
</div>
<p class="rv">The ZeroFS figures are estimates: compression reduces payload bytes, while metadata, temporary compaction data, and requests add cost. At an ideal 8 MiB GET size, reading the file data costs about $0.26 at 2:1 compression or $0.51 at 1:1, before metadata and short or random reads. ZeroFS also requires one server, or two for high availability, plus local disk cache; each node needs about 2 GB of RAM beyond its configured memory cache. S3 Files may therefore cost less for large-file streaming. The difference grows when data is imported into, or written through, high-performance storage: writing 10,000 GiB through S3 Files adds $600 in filesystem writes and $300 for export, while a month of residency adds $3,000.</p>
<p class="rv">Actual costs depend on file count, compression, I/O size, residency, <code>fsync</code> frequency, and infrastructure. S3 Files also <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization.html">requires bucket versioning</a>, so noncurrent versions need an appropriate lifecycle rule.</p>

<h2 class="rv" id="fsync-and-s3-visibility"><code>fsync</code> and S3 visibility</h2>
<p class="rv">Durability and S3 visibility are separate events in S3 Files. <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-performance.html">Writes to high-performance storage are durable immediately</a>, but export starts only after <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization.html">approximately 60 seconds without a write</a>; calling <code>fsync</code> does not make the object immediately visible through the S3 API. ZeroFS has no second visibility event because there is no export step. Over 9P, a <a href="https://www.zerofs.net/docs/durability">successful <code>fsync</code></a> returns after object storage acknowledges the data segment and its LSM metadata, leaving the file recoverable after a cold restart.</p>

<h2 class="rv" id="rename">Rename</h2>
<p class="rv"><a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-synchronization.html">S3 has no directories or atomic rename for general-purpose buckets</a>. S3 Files renames on the mount, then copies each affected object to a new key and deletes the old one; during a directory rename, both prefixes can be visible. AWS says <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-performance.html">synchronizing 100,000 renamed files takes a few minutes</a>. ZeroFS changes only its LSM directory entries, with no copy per descendant and no S3 prefix exposed to object clients.</p>

<hr class="rule">
<p class="rv">The mount points may look similar, but the buckets make different promises: S3 Files preserves ordinary objects for the surrounding S3 ecosystem; ZeroFS treats object storage as the private substrate of the filesystem.</p>

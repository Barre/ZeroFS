+++
title = "Making POSIX filesystems replicated and highly available"
description = "How ZeroFS makes a filesystem on S3 highly available with writer fencing, replication, automatic failover, and durability-aware fsync semantics."
date = 2026-06-28
authors = ["Pierre Barre"]

[extra]
display_date = "June 28, 2026"
reading_time = "9 min read"
summary = "ZeroFS pairs writer-epoch fencing with semi-synchronous replication so a filesystem can fail over without split-brain or a falsely successful <code>fsync</code>."
og_description = "A leader, a standby, writer-epoch fencing, semi-synchronous replication, and an fsync that returns an error rather than report durability it cannot prove."
+++

<p class="lede rv">ZeroFS stores a POSIX filesystem in a log-structured database in S3. On one node, acknowledged but unflushed writes live only in memory, and the filesystem is unavailable whenever that node is down. A standby must cover both gaps without weakening the durability promised by <code>fsync</code>.</p>

<h2 class="rv" id="where-a-single-node-runs-out">Where a single node runs out</h2>
<p class="rv">Object storage is durable, but each request is slow and billable. Committing every filesystem operation directly would add tens of milliseconds and a PUT charge to each write. ZeroFS instead buffers writes in an in-memory memtable and flushes batches on <code>fsync</code>, when the memtable fills, and on a periodic timer. A <code>write()</code> returns once the data reaches the memtable; <code>fsync()</code> pushes it to S3.</p>
<p class="rv">This creates two failure modes. A crash before a flush loses acknowledged writes, which POSIX permits because <code>fsync</code>, not <code>write</code>, is the durability boundary. Data already flushed remains in S3, but no server exposes the filesystem while the node is down. A standby can retain the in-memory tail and take over serving, provided it never becomes a second writer.</p>

<h2 class="rv" id="only-one-writer-ever">Only one writer, ever</h2>
<p class="rv">Two nodes over one bucket can split-brain: both believe they are the leader and write concurrently. Once their updates interleave, the filesystem is corrupt. Preventing that cannot depend on timing, network reachability, or synchronized clocks.</p>
<p class="rv">The storage layer supplies the final boundary. Opening a database for writing conditionally updates its manifest, increments a <code>writer_epoch</code>, and fences the previous holder. The old leader's next manifest refresh or write observes the newer term and stops. Any SST files it uploaded in the meantime remain unreferenced and are later reclaimed by garbage collection. Only one writer epoch can commit durable state.</p>
<p class="rv">A second mechanism controls who may serve. A conditional marker object beside the database moves through <code>Active</code>, <code>Claiming</code>, and <code>Opening</code>. Every transition names one generation and uses the exact object-store version it read. A candidate must win the marker claim, wait out the previous leader's bounded serving authority, open the database to publish a higher writer epoch, reconcile its tail, and become <code>Active</code> before accepting clients.</p>
<div class="callout rv">
  <p>Fencing and marker ownership use conditional writes, supported directly by S3, Azure Blob, and Google Cloud Storage. For S3-compatible stores without that primitive, ZeroFS can use Redis. At startup, each node also asks its peer which one is active instead of trusting its configured role.</p>
</div>

<h2 class="rv" id="why-two-nodes-not-three">Why two nodes, not three</h2>
<p class="rv">Raft and Paxos clusters use an odd number of members because the nodes vote on leadership and committed state. ZeroFS has no node quorum: exact conditional updates in the object store order marker ownership and writer epochs.</p>
<p class="rv">The supported HA topology is exactly two participants. One node serves and the sole standby supplies the heartbeat acknowledgement and complete-tail proof used by the leader's fast path. A third participant would not add a vote; it would violate that sole-standby assumption. Independent read-only nodes remain available for read scaling.</p>

<h2 class="rv" id="keeping-a-standby-in-step">Keeping a standby in step</h2>
<p class="rv">Replication is semi-synchronous and ordered. While the pair is Connected, the leader sends each mutation to the standby, waits for an acknowledgement, applies it locally, and then answers the client. The receiver accepts sequence <em>N</em> only when durable storage and its retained tail account for every earlier sequence. A restarted receiver that lost its in-memory base rejects a torn suffix, causing the leader to flush the missing base and retry the same sequence.</p>
<p class="rv">File contents need more than metadata replication. Until an immutable segment reaches object storage, a replicated write carries its sealed, compressed and encrypted frame bytes with the extent pointer. On takeover the standby materializes any missing segment at the original offsets before serving.</p>
<p class="rv">If the standby falls behind or disconnects, the leader enters Solo instead of blocking the filesystem. Replication resumes when the standby returns. Before acknowledging its first Solo write, the leader marks the current durability lineage non-inheritable. Solo writes then have single-node durability: anything not yet flushed has no second copy. Fencing still prevents another writer from committing durable state.</p>

<h2 class="rv" id="when-the-leader-dies">When the leader dies</h2>
<p class="rv">The leader sends a coverage heartbeat every 100 ms. It includes the exact writer epoch and the applied and durable replication frontiers; the standby acknowledges only when shared storage plus its retained tail cover the complete applied frontier. After roughly two seconds without coverage, the standby may attempt a handoff. Silence is a suspicion signal, not permission to serve.</p>
<figure class="rv">
  <div class="diagram"><pre class="mermaid">
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant S as Standby
    participant O as Object storage
    Note over C,O: Connected
    C->>L: mutation
    L->>S: sequence N and any sealed frame bytes
    S->>S: validate complete prefix, retain tail
    S-->>L: accepted
    L->>L: apply sequence N
    L-->>C: result
    L->>S: coverage heartbeat
    S-->>L: exact-epoch coverage ACK
    Note over L: Leader crashes or is partitioned
    L--xS: heartbeats stop
    Note over S: no coverage for ~2 s
    S->>S: quiesce heartbeat ACKs
    S->>O: exact CAS Active to Claiming
    Note over S,O: wait the full 9 s claim grace
    S->>O: exact CAS Claiming to Opening
    S->>O: open writer, publish a higher epoch
    S->>S: validate and replay retained tail
    S->>O: exact CAS Opening to Active
    S->>O: validate exact Active identity
    C->>S: reconnect, replay session, resend same op-id
    S-->>C: definitive result (deduplicated)
    Note over L: a returning old leader stays fenced
  </pre></div>
  <figcaption>A leader failure end to end: commit-before-apply replication, phased takeover, and session replay.</figcaption>
</figure>
<p class="rv">The claim waits nine seconds before entering <code>Opening</code>. That interval is derived from the previous leader's last three-second authority grant, a three-second final marker-validation opportunity, a two-second response drain, and one second of scheduling margin. These values belong to one static policy rather than independent runtime knobs. Opening the writer then publishes the higher epoch that independently fences durable commits. Only after tail reconciliation and an exact <code>Active</code> validation does the successor serve.</p>
<p class="rv">The old leader must stop reads as well as writes. During healthy replication, an exact-epoch heartbeat ACK no more than 300 ms old renews serving authority without polling the marker. Once ACKs are stale, the leader immediately validates its exact <code>Active</code> marker on a one-second cadence. Each read has a separate three-second allowance, while authority remains anchored to the request start. An expired grant suspends successful responses while one final validation runs; a changed marker, final error or timeout, or writer fence retires the process.</p>
<p class="rv">The bundled mount and the Python, TypeScript, and Go clients accept both node addresses as one comma-separated target set; Rust also exposes <code>connect_multi</code>. Targets are probed concurrently and re-probed after a disconnect. Session replay rebinds linked open handles by inode id and re-acquires recorded byte-range locks before publishing the new connection. An open file whose final link was removed cannot be replayed, so its loss makes that logical session terminal with <code>ESTALE</code>.</p>
<p class="rv">A request in flight during failure may already have committed. Every replay-sensitive mutation therefore carries a stable operation id, attempt state, and origin epoch, while the replicated batch carries its exact result. A successor with complete coverage can answer a retried <code>mkdir</code> or <code>rename</code> without applying it twice.</p>

<h2 class="rv" id="when-fsync-cant-prove-durability">When fsync can't prove durability</h2>
<p class="rv">Replication reduces but cannot eliminate the un-<code>fsync</code>'d window. Losing both nodes, or losing a leader in Solo, discards acknowledged writes that never reached S3. A successful <code>fsync</code> means every write acknowledged on that descriptor is in object storage and, under HA, survives failover. If recovery did not carry those writes, <code>fsync</code> returns <code>ESTALE</code>.</p>
<p class="rv">A <strong>lineage token</strong> identifies an uninterrupted durable history. A connected standby that replays the leader's tail retains the token. A cold restart or takeover after Solo gets a new token because it cannot prove that it inherited every write. Before acknowledging its first Solo write, the leader marks the lineage un-inheritable in object storage. The client tags un-<code>fsync</code>'d changes with the current token; on <code>fsync</code>, the leader flushes and compares the oldest outstanding token with the live one. A mismatch fails the call.</p>
<div class="table-wrap rv">
  <table>
    <thead><tr><th>What happened</th><th>Token</th><th>fsync result</th></tr></thead>
    <tbody>
      <tr><td>Connected leader fails</td><td>Kept: the standby held the writes and replayed the tail</td><td>Succeeds after failover</td></tr>
      <tr><td>Both nodes lost before a flush</td><td>Regenerated: a cold restart can't prove it inherited anything</td><td><code>ESTALE</code>: the writes were only in memory</td></tr>
      <tr><td>Takeover after Solo</td><td>Regenerated: the Solo leader tainted the lineage first</td><td><code>ESTALE</code>: the new leader never got the writes</td></tr>
    </tbody>
  </table>
</div>
<p class="rv">The token check follows the descriptor POSIX makes responsible. An <code>fsync</code> on a file covers that file's data and metadata across every open handle; an <code>fsync</code> on a directory covers its directory-entry changes, and a cross-directory <code>rename</code> marks both directories. On a lineage mismatch, the next responsible <code>fsync</code> returns <code>ESTALE</code>: work since its last successful durability boundary is gone. Client libraries expose this as a distinct stale error.</p>

<h2 class="rv" id="trying-to-break-it">Trying to break it</h2>
<p class="rv">ZeroFS tests these guarantees with <a href="https://jepsen.io">Jepsen</a>. The single-node suite generates filesystem operations over a 9P mount and checks them against a reference model. Its crash mode kills the server, drops the memtable, recovers from object storage, and verifies the result against the last <code>fsync</code>. This suite runs in CI on every change.</p>
<p class="rv">The HA suite runs a leader and standby over MinIO while a nemesis kills the leader, the standby, or both; partitions the replication link; pauses the object store; and freezes then thaws a leader after a successor promotes. The workloads cover concurrent add and remove operations, file contents, filesystem statistics, and lineage-aware <code>fsync</code> for file data, metadata, multiple handles, directory entries, and cross-directory rename. The checkers reject lost, resurrected, or corrupted acknowledged state and any successful <code>fsync</code> that approved a lineage recovery which dropped its writes. The <a href="https://github.com/Barre/ZeroFS/tree/main/jepsen/ha">suite is in the repository</a>.</p>

<h2 class="rv" id="what-it-doesnt-do">What it doesn't do</h2>
<p class="rv">HA improves availability, not throughput. There is still one writer, reads are not distributed across the pair, and exactly two HA participants are supported.</p>
<p class="rv">Crash failover is not a two-second event. The two-second suspicion window is followed by the nine-second claim grace, writer open, tail reconciliation, activation, and client reconnect. Clients block and retry during that interval. The shared object store and its conditional-update path also remain availability dependencies.</p>
<p class="rv">Advisory byte-range locks live in server memory and are not replicated. The bundled client records and re-acquires them before completing session replay, but ownership is not continuous: another session may acquire a lock during the gap. Applications that need fencing across failover cannot use an advisory file lock as a distributed mutex.</p>
<p class="rv"><code>fsync</code> remains the durability boundary. A connected standby also protects unflushed writes, but applications should not rely on that extra copy. Configuration and a complete failure-case table are in the <a href="https://www.zerofs.net/docs/high-availability">high availability documentation</a>.</p>

<hr class="rule">
<p class="rv">The phased marker retires the old serving term before the next one opens, while the writer epoch remains the independent storage fence. Replication covers ordinary Connected failover; lineage tokens turn failures outside that coverage into <code>ESTALE</code> instead of false success.</p>

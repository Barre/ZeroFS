+++
title = "Making POSIX filesystems replicated and highly available"
description = "How ZeroFS makes a filesystem on S3 highly available with writer fencing, replication, automatic failover, and durability-aware fsync semantics."
date = 2026-06-28
authors = ["Pierre Barre"]

[extra]
display_date = "June 28, 2026"
reading_time = "8 min read"
summary = "ZeroFS pairs writer-epoch fencing with semi-synchronous replication so a filesystem can fail over without split-brain or a falsely successful <code>fsync</code>."
og_description = "A leader, a standby, writer-epoch fencing, semi-synchronous replication, and an fsync that returns an error rather than report durability it cannot prove."
+++

<p class="lede rv">ZeroFS stores a POSIX filesystem in a log-structured database in S3. On one node, acknowledged but unflushed writes live only in memory, and the filesystem is unavailable whenever that node is down. A standby must cover both gaps without weakening the durability promised by <code>fsync</code>.</p>

<h2 class="rv" id="where-a-single-node-runs-out">Where a single node runs out</h2>
<p class="rv">Object storage is durable, but each request is slow and billable. Committing every filesystem operation directly would add tens of milliseconds and a PUT charge to each write. ZeroFS instead buffers writes in an in-memory memtable and flushes batches on <code>fsync</code>, when the memtable fills, and on a periodic timer. A <code>write()</code> returns once the data reaches the memtable; <code>fsync()</code> pushes it to S3.</p>
<p class="rv">This creates two failure modes. A crash before a flush loses acknowledged writes, which POSIX permits because <code>fsync</code>, not <code>write</code>, is the durability boundary. Data already flushed remains in S3, but no server exposes the filesystem while the node is down. A standby can retain the in-memory tail and take over serving, provided it never becomes a second writer.</p>

<h2 class="rv" id="only-one-writer-ever">Only one writer, ever</h2>
<p class="rv">Two nodes over one bucket can split-brain: both believe they are the leader and write concurrently. Once their updates interleave, the filesystem is corrupt. Preventing that cannot depend on timing, network reachability, or synchronized clocks.</p>
<p class="rv">The storage layer chooses the single writer. Opening a database for writing conditionally updates its manifest, increments a <code>writer_epoch</code>, and fences the previous holder. The old leader's next manifest write fails and it stops. Any SST files it uploaded in the meantime remain unreferenced and are later reclaimed by garbage collection. During a race, both nodes may briefly claim leadership, but only one epoch can commit.</p>
<div class="callout rv">
  <p>Fencing uses conditional writes, supported directly by S3, Azure Blob, and Google Cloud Storage. For S3-compatible stores without that primitive, ZeroFS uses Redis. At startup, each node also asks its peer which one is active instead of trusting its configured role.</p>
</div>

<h2 class="rv" id="why-two-nodes-not-three">Why two nodes, not three</h2>
<p class="rv">Raft and Paxos clusters use an odd number of members because the nodes vote on leadership and committed state. ZeroFS has no node quorum. A conditional update to the manifest in the bucket decides which node owns the writer epoch.</p>
<p class="rv">The object store's linearizable compare-and-swap can choose one winner among any number of contenders. One ZeroFS node serves; the second holds the un-<code>fsync</code>'d tail and can take over. A third would add no voting safety because the vote already happens in the bucket.</p>

<h2 class="rv" id="keeping-a-standby-in-step">Keeping a standby in step</h2>
<p class="rv">Replication is semi-synchronous and ordered. For each write, the leader sends it to the standby, waits for an acknowledgement, applies it locally, and then answers the client. Anything visible to a client has therefore reached the standby. Unapplied writes remain in a tail buffer for replay during takeover.</p>
<p class="rv">If the standby falls behind or disconnects, the leader enters Solo instead of blocking the filesystem. Replication resumes when the standby returns. Solo writes have single-node durability: writes not yet flushed have no second copy. Fencing still prevents another writer from committing.</p>

<h2 class="rv" id="when-the-leader-dies">When the leader dies</h2>
<p class="rv">The leader sends a heartbeat every 100 ms with its writer epoch. After roughly two seconds without one, the standby opens the database for writing, fences the old leader, replays its buffered tail, and starts serving. Heartbeats from the old epoch are ignored. If a standby with an unreplayed tail hears from a stale leader, it promotes immediately rather than waiting for the timer.</p>
<figure class="rv">
  <div class="diagram"><pre class="mermaid">
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant S as Standby
    participant O as Object storage
    Note over C,O: Steady state
    C->>L: write
    L->>S: ship (semi-sync)
    S-->>L: replicated
    L-->>C: ack (after local apply)
    L->>O: flush on fsync
    L->>S: heartbeats (writer epoch)
    Note over L: Leader crashes or is partitioned
    L--xS: heartbeats stop
    Note over S: no heartbeat for ~2s (takeover TTL)
    S->>O: open as writer, fence old leader
    S->>S: replay buffered tail
    Note over S: now serving as the new leader
    C->>L: in-flight write
    L--xC: fail, timeout, or not-leader
    C->>S: reroute, resend with op-id
    S-->>C: definitive result (deduplicated)
    Note over L: a returning old leader stays fenced
  </pre></div>
  <figcaption>A leader failure end to end: steady state, takeover, and the client's rerouted retry.</figcaption>
</figure>
<p class="rv">Clients know both addresses and reroute when one node stops serving. A request in flight during failure may already have committed, so every mutating call carries a stable operation id. The client can retry a <code>mkdir</code> or <code>rename</code> on the new leader without duplicating the effect.</p>
<p class="rv">The old leader must stop reads as well as writes. A node serves only while its lease is valid, and the lease follows the storage engine's view of the database. A manifest poll sees the new epoch and closes the stale database within one interval, even if the old leader is idle.</p>

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
<p class="rv">The token check is per descriptor but covers the whole file across open handles, plus metadata, directory entries, and both sides of a <code>rename</code>. On a mismatch, the next <code>fsync</code> returns <code>ESTALE</code>: work since the last successful <code>fsync</code> is gone. Client libraries expose this as a distinct stale-handle error.</p>

<h2 class="rv" id="trying-to-break-it">Trying to break it</h2>
<p class="rv">ZeroFS tests these guarantees with <a href="https://jepsen.io">Jepsen</a>. The single-node suite generates filesystem operations over a 9P mount and checks them against a reference model. Its crash mode kills the server, drops the memtable, recovers from object storage, and verifies the result against the last <code>fsync</code>. This suite runs in CI on every change.</p>
<p class="rv">The HA suite runs a leader and standby over MinIO while a nemesis kills either node or both, partitions the pair, pauses the object store, and freezes the leader beyond its lease. Meanwhile, a client continues to write and call <code>fsync</code>. The checker looks for overlapping commit epochs, lost durable writes, duplicates, stale reads, and a thawed leader that fails to recognize its fencing. The <a href="https://github.com/Barre/ZeroFS/tree/main/jepsen/ha">suite is in the repository</a>.</p>

<h2 class="rv" id="what-it-doesnt-do">What it doesn't do</h2>
<p class="rv">HA improves availability, not throughput. There is still one writer, and reads are not distributed across nodes.</p>
<p class="rv">Crash failover takes roughly two seconds while the standby waits for the takeover TTL; clients block and retry during that interval. Advisory byte-range locks are not replicated and disappear on failover.</p>
<p class="rv"><code>fsync</code> remains the durability boundary. A connected standby also protects unflushed writes, but applications should not rely on that extra copy. Configuration and a complete failure-case table are in the <a href="https://www.zerofs.net/docs/high-availability">high availability documentation</a>.</p>

<hr class="rule">
<p class="rv">Fencing makes the writer a storage-layer decision rather than a timing assumption. Replication covers ordinary failover; lineage tokens turn failures outside that coverage into <code>ESTALE</code> instead of false success.</p>

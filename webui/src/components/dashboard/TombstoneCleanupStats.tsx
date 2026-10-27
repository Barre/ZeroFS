import type { StatsSnapshot } from "../../lib/grpc/gen/admin_pb";

interface TombstoneCleanupStatsProps {
  snapshot: StatsSnapshot;
}

export function TombstoneCleanupStats({ snapshot }: TombstoneCleanupStatsProps) {
  const items = [
    { label: "Cleanup Runs", value: snapshot.tombstoneCleanupRuns },
    { label: "Extents Deleted", value: snapshot.tombstoneCleanupExtentsDeleted },
    { label: "Tombstones Created", value: snapshot.tombstonesCreated },
    { label: "Tombstones Processed", value: snapshot.tombstonesProcessed },
  ];

  return (
    <div className="card-surface rounded-lg p-5">
      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-4">Tombstone Cleanup</p>
      {items.map((item) => (
        <div key={item.label} className="flex justify-between text-sm py-1.5">
          <span className="text-muted">{item.label}</span>
          <span className="font-mono tabular-nums text-foreground">{item.value.toString()}</span>
        </div>
      ))}
    </div>
  );
}

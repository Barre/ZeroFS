import type { StatsSnapshot } from "../../lib/grpc/gen/admin_pb";

interface OperationCountersProps {
  snapshot: StatsSnapshot;
}

export function OperationCounters({ snapshot }: OperationCountersProps) {
  const groups = [
    {
      label: "Files",
      items: [
        { label: "Created", value: snapshot.filesCreated },
        { label: "Deleted", value: snapshot.filesDeleted },
        { label: "Renamed", value: snapshot.filesRenamed },
      ],
    },
    {
      label: "Directories",
      items: [
        { label: "Created", value: snapshot.directoriesCreated },
        { label: "Deleted", value: snapshot.directoriesDeleted },
        { label: "Renamed", value: snapshot.directoriesRenamed },
      ],
    },
    {
      label: "Links",
      items: [
        { label: "Created", value: snapshot.linksCreated },
        { label: "Deleted", value: snapshot.linksDeleted },
        { label: "Renamed", value: snapshot.linksRenamed },
      ],
    },
  ];

  return (
    <div className="card-surface rounded-lg p-5">
      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-4">Operations</p>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
        {groups.map((group) => (
          <div key={group.label}>
            <p className="text-sm font-medium text-foreground mb-2">{group.label}</p>
            {group.items.map((item) => (
              <div key={item.label} className="flex justify-between text-sm py-1">
                <span className="text-muted">{item.label}</span>
                <span className="font-mono tabular-nums text-foreground">{item.value.toString()}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

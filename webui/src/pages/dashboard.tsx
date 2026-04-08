import { Activity, Database, HardDrive, Loader2, Zap, Weight } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { useStatsStream } from "../hooks/use-stats";
import { formatSize } from "../lib/format";
import { StatCard } from "../components/dashboard/StatCard";
import { IOChart } from "../components/dashboard/IOChart";
import { IOPSChart } from "../components/dashboard/IOPSChart";
import { OperationCounters } from "../components/dashboard/OperationCounters";
import { GCStats } from "../components/dashboard/GCStats";
import { FileAccessTracer } from "../components/dashboard/FileAccessTracer";

export function DashboardPage() {
  useTitle("Monitor");
  const { snapshot, history, error } = useStatsStream(1000);

  if (error) {
    return (
      <div className="p-6">
        <div className="card-surface rounded-lg p-5">
          <p className="text-destructive text-sm font-mono">{error.message}</p>
        </div>
      </div>
    );
  }

  if (!snapshot) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-muted">
        <Loader2 size={20} className="animate-spin" />
        <span className="text-sm">Connecting to stats stream...</span>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-5 max-w-[1400px] overflow-hidden">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold">Monitor</h1>
          <span className="flex items-center gap-1.5 text-xs text-success bg-success/10 px-2 py-0.5 rounded-full">
            <span className="w-1.5 h-1.5 rounded-full bg-success animate-pulse" />
            Live
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-4">
        <StatCard
          title="Total Operations"
          value={snapshot.totalOperations.toLocaleString()}
          icon={<Zap size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Bytes Read"
          value={formatSize(snapshot.bytesRead)}
          icon={<Activity size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Bytes Written"
          value={formatSize(snapshot.bytesWritten)}
          icon={<HardDrive size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Storage Used"
          value={formatSize(snapshot.usedBytes)}
          icon={<Weight size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Inodes Used"
          value={snapshot.usedInodes.toLocaleString()}
          icon={<Database size={18} strokeWidth={1.5} />}
        />
      </div>

      <IOChart history={history} />
      <IOPSChart history={history} />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <OperationCounters snapshot={snapshot} />
        <GCStats snapshot={snapshot} />
        <div className="card-surface rounded-lg p-5">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-3">Properties</p>
          <dl className="space-y-2 text-sm">
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Files created</dt>
              <dd className="font-mono tabular-nums">{snapshot.filesCreated.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Files deleted</dt>
              <dd className="font-mono tabular-nums">{snapshot.filesDeleted.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Files renamed</dt>
              <dd className="font-mono tabular-nums">{snapshot.filesRenamed.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Dirs created</dt>
              <dd className="font-mono tabular-nums">{snapshot.directoriesCreated.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Dirs deleted</dt>
              <dd className="font-mono tabular-nums">{snapshot.directoriesDeleted.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Links created</dt>
              <dd className="font-mono tabular-nums">{snapshot.linksCreated.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Tombstones</dt>
              <dd className="font-mono tabular-nums">{snapshot.tombstonesCreated.toLocaleString()} / {snapshot.tombstonesProcessed.toLocaleString()}</dd>
            </div>
          </dl>
        </div>
      </div>

      <FileAccessTracer />
    </div>
  );
}

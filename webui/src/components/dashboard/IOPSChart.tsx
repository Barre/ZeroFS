import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { Loader2 } from "lucide-react";
import type { StatsSnapshot } from "../../lib/grpc/gen/admin_pb";

interface IOPSChartProps {
  history: StatsSnapshot[];
}

export function IOPSChart({ history }: IOPSChartProps) {
  const fontFamily = "'Geist Mono', 'SF Mono', ui-monospace, monospace";
  if (history.length < 2) return (
    <div className="card-surface rounded-lg p-5 h-[260px] flex items-center justify-center">
      <Loader2 size={18} className="animate-spin text-muted-foreground" />
    </div>
  );
  const data = history.slice(1).map((snap, i) => {
    const prev = history[i];
    const dtSec = deltaSeconds(prev, snap);
    if (dtSec <= 0) return { t: i, readOps: 0, writeOps: 0 };
    return {
      t: i,
      readOps: Number(snap.readOperations - prev.readOperations) / dtSec,
      writeOps: Number(snap.writeOperations - prev.writeOperations) / dtSec,
    };
  });

  return (
    <div className="card-surface rounded-lg p-5">
      <div className="flex items-center justify-between mb-4">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">IOPS</p>
        <div className="flex items-center gap-4 text-xs">
          <span className="flex items-center gap-1.5">
            <span className="w-2.5 h-0.5 rounded-full bg-blue-400 inline-block" />
            <span className="text-muted">Read</span>
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-2.5 h-0.5 rounded-full bg-emerald-400 inline-block" />
            <span className="text-muted">Write</span>
          </span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={200} style={{ marginLeft: -10 }}>
        <AreaChart data={data} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="readOpsGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#60a5fa" stopOpacity={0.15} />
              <stop offset="100%" stopColor="#60a5fa" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="writeOpsGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#34d399" stopOpacity={0.15} />
              <stop offset="100%" stopColor="#34d399" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#3d444d" vertical={false} />
          <XAxis
            dataKey="t"
            tick={{ fontSize: 10, fill: "#656c76", fontFamily }}
            axisLine={false}
            tickLine={false}
            interval={Math.max(0, Math.floor(data.length / 6) - 1)}
            tickFormatter={(v) => `${v - data.length + 1}s`}
          />
          <YAxis
            tickFormatter={(v) => `${Math.round(v)} op/s`}
            width={70}
            tick={{ fontSize: 10, fill: "#656c76", fontFamily }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "#2a313c",
              border: "1px solid #3d444d",
              borderRadius: 6,
              fontSize: 11,
              fontFamily,
              boxShadow: "0 4px 24px rgba(0,0,0,0.3)",
            }}
            formatter={(value) => `${Math.round(Number(value))} ops/s`}
            labelFormatter={() => ""}
          />
          <Area
            type="monotone"
            dataKey="readOps"
            name="Read"
            stroke="#60a5fa"
            fill="url(#readOpsGrad)"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
          <Area
            type="monotone"
            dataKey="writeOps"
            name="Write"
            stroke="#34d399"
            fill="url(#writeOpsGrad)"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function deltaSeconds(a: StatsSnapshot, b: StatsSnapshot): number {
  if (!a.timestamp || !b.timestamp) return 1;
  const aSec = Number(a.timestamp.seconds) + Number(a.timestamp.nanos) / 1e9;
  const bSec = Number(b.timestamp.seconds) + Number(b.timestamp.nanos) / 1e9;
  return bSec - aSec;
}

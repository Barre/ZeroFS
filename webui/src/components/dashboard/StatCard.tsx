import type { ReactNode } from "react";

interface StatCardProps {
  title: string;
  value: string;
  icon?: ReactNode;
  trend?: "up" | "down" | "neutral";
}

export function StatCard({ title, value, icon }: StatCardProps) {
  return (
    <div className="card-surface rounded-lg p-5 flex items-start justify-between min-w-0">
      <div className="min-w-0">
        <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground mb-2 leading-tight truncate">
          {title}
        </p>
        <p className="text-2xl font-semibold tabular-nums font-mono tracking-tight truncate">{value}</p>
      </div>
      {icon && <div className="text-muted-foreground mt-0.5 shrink-0">{icon}</div>}
    </div>
  );
}

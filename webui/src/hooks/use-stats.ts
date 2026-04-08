import { useState, useEffect } from "react";
import { adminClient } from "../lib/grpc/client";
import type { StatsSnapshot } from "../lib/grpc/gen/admin_pb";

const HISTORY_SIZE = 120;
const RECONNECT_DELAY = 2000;

export function useStatsStream(intervalMs = 1000) {
  const [snapshot, setSnapshot] = useState<StatsSnapshot | null>(null);
  const [history, setHistory] = useState<StatsSnapshot[]>([]);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const abort = new AbortController();
    let seeded = false;

    const connect = async () => {
      while (!abort.signal.aborted) {
        try {
          setError(null);
          for await (const snap of adminClient.streamStats({ intervalMs }, { signal: abort.signal })) {
            setSnapshot(snap);
            setHistory((prev) => {
              if (!seeded) {
                seeded = true;
                return Array.from({ length: HISTORY_SIZE }, () => snap);
              }
              const next = [...prev, snap];
              if (next.length > HISTORY_SIZE) next.shift();
              return next;
            });
          }
        } catch (e) {
          if (abort.signal.aborted) return;
          setError(e instanceof Error ? e : new Error(String(e)));
        }
        if (!abort.signal.aborted) {
          seeded = false;
          await new Promise((r) => setTimeout(r, RECONNECT_DELAY));
        }
      }
    };

    connect();
    return () => abort.abort();
  }, [intervalMs]);

  return { snapshot, history, error };
}

import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { p9client, type FileEntry, type FileInfo, type TrafficStats } from "../lib/ninep/client";
import { useNinePConnection } from "./use-ninep-connection";

export function useDirectory(path: string) {
  const state = useNinePConnection();
  return useQuery<FileEntry[]>({
    queryKey: ["9p", "ls", path],
    queryFn: () => p9client.listDirectory(path),
    enabled: state === "connected",
    retry: false,
  });
}

export function useStat(path: string) {
  const state = useNinePConnection();
  return useQuery<FileInfo>({
    queryKey: ["9p", "stat", path],
    queryFn: () => p9client.stat(path),
    enabled: state === "connected",
  });
}

export function useMkdir() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ path, mode }: { path: string; mode?: number }) => p9client.mkdir(path, mode),
    onSuccess: (_data, { path }) => {
      const parent = path.substring(0, path.lastIndexOf("/")) || "/";
      qc.invalidateQueries({ queryKey: ["9p", "ls", parent] });
    },
  });
}

export function useRemove() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ path, isDir }: { path: string; isDir: boolean }) =>
      isDir ? p9client.removeDir(path) : p9client.remove(path),
    onSuccess: (_data, { path }) => {
      const parent = path.substring(0, path.lastIndexOf("/")) || "/";
      qc.invalidateQueries({ queryKey: ["9p", "ls", parent] });
    },
  });
}

export function useRename() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ oldPath, newPath }: { oldPath: string; newPath: string }) => p9client.rename(oldPath, newPath),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["9p", "ls"] });
    },
  });
}

export function useTrafficStats(): TrafficStats {
  const [stats, setStats] = useState<TrafficStats>({ bytesSent: 0, bytesReceived: 0, ops: 0 });
  useEffect(() => p9client.onStats(setStats), []);
  return stats;
}

export function useUpload() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      dirPath,
      file,
      onProgress,
      signal,
    }: {
      dirPath: string;
      file: File;
      onProgress?: (sent: number, total: number) => void;
      signal?: AbortSignal;
    }) => {
      const targetPath = dirPath.replace(/\/$/, "") + "/" + file.name;
      return p9client.uploadBlob(targetPath, file, onProgress, signal);
    },
    onSuccess: (_data, { dirPath }) => {
      qc.invalidateQueries({ queryKey: ["9p", "ls", dirPath] });
    },
  });
}

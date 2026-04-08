import { useState, useCallback, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { useUpload } from "../../../hooks/use-ninep";
import { p9client, type FileEntry } from "../../../lib/ninep/client";
import { useFileTransfers } from "../../transfers/use-file-transfers";
import { formatSize, joinPath } from "../../../lib/format";
import { formatError } from "../../../lib/errors";
import { pooled } from "../../../lib/async";
import { readDroppedItems, getDirectoriesToCreate } from "../../../lib/folder-upload";

export interface OverwritePrompt {
  existing: string[];
  allFiles: File[];
  resolve: (action: "overwrite" | "skip" | "cancel") => void;
}

export function useUploads(path: string, entries: FileEntry[] | undefined) {
  const qc = useQueryClient();
  const transfers = useFileTransfers();
  const uploadMut = useUpload();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [overwritePrompt, setOverwritePrompt] = useState<OverwritePrompt | null>(null);

  const startDownload = useCallback(
    async (filePath: string, isDir = false) => {
      const name = filePath.split("/").pop() ?? "download";
      const retry = () => startDownload(filePath, isDir);

      if (isDir) {
        const handle = transfers.startDownload(`${name}.zip`, retry);
        try {
          let collected = 0;
          let totalBytes = 0;
          const files = await p9client.collectFiles(
            filePath,
            "",
            (f) => {
              collected++;
              handle.update(0, 0, `${collected} files found: ${f}`);
            },
            handle.signal,
          );
          totalBytes = files.reduce((sum, f) => sum + f.data.length, 0);
          handle.update(0, 0, `Zipping ${collected} files (${formatSize(totalBytes)})...`);
          const { zipSync } = await import("fflate");
          const zipData: Record<string, Uint8Array> = {};
          for (const f of files) zipData[f.path] = f.data;
          const zipped = zipSync(zipData);
          const blob = new Blob([new Uint8Array(zipped)], { type: "application/zip" });
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `${name}.zip`;
          a.click();
          URL.revokeObjectURL(url);
          handle.update(zipped.length, zipped.length);
          handle.finish();
        } catch (err) {
          if (err instanceof DOMException && err.name === "AbortError") return;
          handle.fail(formatError(err));
        }
      } else {
        const handle = transfers.startDownload(name, retry);
        try {
          await p9client.download(
            filePath,
            (received, total) => {
              handle.update(received, total);
            },
            handle.signal,
          );
          handle.finish();
        } catch (err) {
          if (err instanceof DOMException && err.name === "AbortError") return;
          handle.fail(formatError(err));
        }
      }
    },
    [transfers],
  );

  const deleteEntry = useCallback(
    async (
      entryName: string,
      isDir: boolean,
      removeMut: { mutateAsync: (args: { path: string; isDir: boolean }) => Promise<unknown> },
    ) => {
      const entryPath = joinPath(path, entryName);
      if (isDir) {
        const doDelete = async () => {
          const retry = () => doDelete();
          const handle = transfers.startDelete(entryName, retry);
          try {
            const count = await p9client.removeDirRecursive(
              entryPath,
              (deleted, current) => handle.update(deleted, 0, current.split("/").pop()),
              handle.signal,
            );
            handle.update(count, count);
            handle.finish();
          } catch (err) {
            if (err instanceof DOMException && err.name === "AbortError") return;
            handle.fail(formatError(err));
          }
        };
        await doDelete();
      } else {
        try {
          await removeMut.mutateAsync({ path: entryPath, isDir: false });
          toast.success(`Deleted ${entryName}`);
        } catch (err) {
          toast.error(`Failed to delete ${entryName}`, { description: formatError(err) });
        }
      }
    },
    [path, transfers],
  );

  const doUpload = useCallback(
    async (filesToUpload: File[]) => {
      await pooled(filesToUpload, 8, async (file) => {
        const uploadOne = async () => {
          const retry = () => uploadOne();
          const handle = transfers.startUpload(file.name, file.size, retry);
          try {
            await uploadMut.mutateAsync({
              dirPath: path,
              file,
              onProgress: (sent, total) => handle.update(sent, total),
              signal: handle.signal,
            });
            handle.finish();
          } catch (err) {
            if (err instanceof DOMException && err.name === "AbortError") return;
            handle.fail(formatError(err));
          }
        };
        await uploadOne();
      });
    },
    [path, uploadMut, transfers],
  );

  const uploadFiles = useCallback(
    async (files: FileList) => {
      const allFiles = Array.from(files);
      const existingNames = new Set(entries?.map((e) => e.name) ?? []);
      const conflicts = allFiles.filter((f) => existingNames.has(f.name));

      if (conflicts.length === 0) {
        doUpload(allFiles);
        return;
      }

      const action = await new Promise<"overwrite" | "skip" | "cancel">((resolve) => {
        setOverwritePrompt({
          existing: conflicts.map((f) => f.name),
          allFiles,
          resolve,
        });
      });
      setOverwritePrompt(null);

      if (action === "cancel") return;
      if (action === "overwrite") {
        doUpload(allFiles);
      } else {
        const skipNames = new Set(conflicts.map((f) => f.name));
        doUpload(allFiles.filter((f) => !skipNames.has(f.name)));
      }
    },
    [entries, doUpload],
  );

  const handleFileUpload = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files) uploadFiles(e.target.files);
      e.target.value = "";
    },
    [uploadFiles],
  );

  const handleFolderUpload = useCallback(
    async (items: DataTransferItemList) => {
      try {
        const handle = transfers.startUpload("Scanning folder...", 0);
        const uploadItems = await readDroppedItems(
          items,
          (progress) => {
            handle.update(0, 0, `${progress.filesFound} files found: ${progress.currentPath}`);
          },
          handle.signal,
        );

        if (handle.signal.aborted) return;

        if (uploadItems.length === 0) {
          handle.finish();
          toast.info("No files found in folder");
          return;
        }

        const totalBytes = uploadItems.reduce((sum, item) => sum + item.file.size, 0);
        handle.setName(`${uploadItems.length} files`);
        handle.update(0, totalBytes);
        let bytesUploaded = 0;
        let errors = 0;

        const dirs = getDirectoriesToCreate(uploadItems);
        let created = 0;
        const byDepth = new Map<number, string[]>();
        for (const dir of dirs) {
          const depth = dir.split("/").length;
          if (!byDepth.has(depth)) byDepth.set(depth, []);
          byDepth.get(depth)!.push(dir);
        }
        for (const depth of [...byDepth.keys()].sort((a, b) => a - b)) {
          if (handle.signal.aborted) break;
          const batch = byDepth.get(depth)!;
          await pooled(batch, 16, async (dir) => {
            if (handle.signal.aborted) return;
            const fullDir = joinPath(path, dir);
            try {
              await p9client.mkdir(fullDir);
            } catch {
              /* may already exist */
            }
            created++;
            handle.update(0, totalBytes, `Creating folders ${created}/${dirs.length}`);
          });
        }

        let filesUploaded = 0;
        await pooled(uploadItems, 16, async (item) => {
          if (handle.signal.aborted) return;
          const targetPath = joinPath(path, item.relativePath);
          try {
            await p9client.uploadBlob(
              targetPath,
              item.file,
              (sent) => {
                handle.update(bytesUploaded + sent, totalBytes, `${filesUploaded}/${uploadItems.length} files`);
              },
              handle.signal,
            );
            bytesUploaded += item.file.size;
          } catch (err) {
            if (err instanceof DOMException && err.name === "AbortError") return;
            errors++;
            toast.error(`Failed to upload ${item.relativePath}`, { description: formatError(err) });
          }
          filesUploaded++;
          handle.update(bytesUploaded, totalBytes, `${filesUploaded}/${uploadItems.length} files`);
        });

        if (errors === uploadItems.length) {
          handle.fail(`All ${errors} uploads failed`);
        } else {
          handle.finish();
          if (errors > 0) toast.warning(`${errors} file${errors > 1 ? "s" : ""} failed to upload`);
        }
        qc.invalidateQueries({ queryKey: ["9p", "ls"] });
      } catch (err) {
        toast.error("Failed to read dropped items", { description: formatError(err) });
      }
    },
    [path, qc, transfers],
  );

  return {
    startDownload,
    deleteEntry,
    doUpload,
    uploadFiles,
    handleFileUpload,
    handleFolderUpload,
    fileInputRef,
    overwritePrompt,
    setOverwritePrompt,
  };
}

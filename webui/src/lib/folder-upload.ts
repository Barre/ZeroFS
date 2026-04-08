export interface UploadItem {
  relativePath: string;
  file: File;
}

export interface ScanProgress {
  filesFound: number;
  currentPath: string;
}

export async function readDroppedItems(
  items: DataTransferItemList,
  onProgress?: (progress: ScanProgress) => void,
  signal?: AbortSignal,
): Promise<UploadItem[]> {
  const results: UploadItem[] = [];
  const entries: FileSystemEntry[] = [];

  for (let i = 0; i < items.length; i++) {
    const entry = items[i].webkitGetAsEntry?.();
    if (entry) entries.push(entry);
  }

  await Promise.all(entries.map((entry) => readEntry(entry, "", results, onProgress, signal)));

  return results;
}

async function readEntry(
  entry: FileSystemEntry,
  basePath: string,
  results: UploadItem[],
  onProgress?: (progress: ScanProgress) => void,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted) return;

  if (entry.isFile) {
    const file = await getFile(entry as FileSystemFileEntry);
    const relativePath = basePath ? `${basePath}/${entry.name}` : entry.name;
    results.push({ relativePath, file });
    onProgress?.({ filesFound: results.length, currentPath: relativePath });
  } else if (entry.isDirectory) {
    const dirPath = basePath ? `${basePath}/${entry.name}` : entry.name;
    onProgress?.({ filesFound: results.length, currentPath: dirPath + "/" });
    const dirReader = (entry as FileSystemDirectoryEntry).createReader();
    const children = await readAllEntries(dirReader);
    await Promise.all(children.map((child) => readEntry(child, dirPath, results, onProgress, signal)));
  }
}

function getFile(entry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

function readAllEntries(reader: FileSystemDirectoryReader): Promise<FileSystemEntry[]> {
  return new Promise((resolve, reject) => {
    const all: FileSystemEntry[] = [];
    const readBatch = () => {
      reader.readEntries((entries) => {
        if (entries.length === 0) {
          resolve(all);
        } else {
          all.push(...entries);
          readBatch(); // readEntries may not return all entries in one call
        }
      }, reject);
    };
    readBatch();
  });
}

export function getDirectoriesToCreate(items: UploadItem[]): string[] {
  const dirs = new Set<string>();
  for (const item of items) {
    const parts = item.relativePath.split("/");
    parts.pop(); // remove filename
    for (let i = 1; i <= parts.length; i++) {
      dirs.add(parts.slice(0, i).join("/"));
    }
  }
  return [...dirs].sort((a, b) => a.split("/").length - b.split("/").length);
}

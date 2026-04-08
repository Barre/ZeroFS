import { createContext } from "react";

export interface TransferHandle {
  id: number;
  signal: AbortSignal;
  setName: (name: string) => void;
  update: (received: number, total: number, detail?: string) => void;
  finish: () => void;
  fail: (error: string) => void;
}

export interface TransferContextValue {
  startDownload: (name: string, retry?: () => void) => TransferHandle;
  startUpload: (name: string, total: number, retry?: () => void) => TransferHandle;
  startDelete: (name: string, retry?: () => void) => TransferHandle;
}

const noop: TransferHandle = {
  id: 0,
  signal: new AbortController().signal,
  setName() {},
  update() {},
  finish() {},
  fail() {},
};

export const TransferContext = createContext<TransferContextValue>({
  startDownload: () => noop,
  startUpload: () => noop,
  startDelete: () => noop,
});

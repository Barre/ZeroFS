import { useContext } from "react";
import { TransferContext } from "./TransferContext";

export function useFileTransfers() {
  return useContext(TransferContext);
}

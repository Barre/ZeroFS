import { useState } from "react";

/** Keep last non-null value so content can render during exit animations. */
export function useFrozen<T>(value: T | null): T | null {
  const [frozen, setFrozen] = useState(value);
  if (value !== null && value !== frozen) setFrozen(value);
  return value ?? frozen;
}

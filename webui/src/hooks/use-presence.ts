import { useState } from "react";

/**
 * Delays unmounting so exit animations can play.
 * Returns `mounted` (whether to render) and `open` (whether to show enter vs exit state).
 */
export function usePresence(present: boolean, duration = 150) {
  const [state, setState] = useState({ mounted: present, timer: 0 as ReturnType<typeof setTimeout> | 0 });

  if (present && !state.mounted) {
    // Mount immediately when present becomes true (allowed: setState during render for derived state)
    clearTimeout(state.timer);
    setState({ mounted: true, timer: 0 });
  } else if (!present && state.mounted && !state.timer) {
    // Schedule unmount when present goes false
    const timer = setTimeout(() => setState({ mounted: false, timer: 0 }), duration);
    setState({ mounted: true, timer });
  } else if (present && state.timer) {
    // Cancel pending unmount if present comes back
    clearTimeout(state.timer);
    setState({ mounted: true, timer: 0 });
  }

  return { mounted: state.mounted, open: present };
}

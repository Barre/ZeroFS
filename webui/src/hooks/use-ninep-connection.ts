import { useState, useEffect, useRef, useSyncExternalStore } from "react";
import { p9client, type ConnectionState } from "../lib/ninep/client";

function getWsUrl() {
  const wsProto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${wsProto}//${window.location.host}/ws/9p`;
}

const subscribe = (cb: () => void) => p9client.onStateChange(cb);
const getSnapshot = () => p9client.state;

export function useNinePConnection(): ConnectionState {
  useEffect(() => {
    p9client.enableAutoReconnect(getWsUrl());
  }, []);

  return useSyncExternalStore(subscribe, getSnapshot);
}

// Debounced visual state: delays "disconnected" by 1.5s to avoid flicker on reconnect.
export function useNinePVisualState(): ConnectionState {
  const rawState = useNinePConnection();
  const [visualState, setVisualState] = useState<ConnectionState>("connecting");
  const hasConnected = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (rawState === "connected") {
      hasConnected.current = true;
      if (timer.current) {
        clearTimeout(timer.current);
        timer.current = null;
      }
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setVisualState("connected");
    } else if (!hasConnected.current) {
      // First load
      setVisualState("connecting");
    } else {
      // Lost connection, delay before showing disconnected
      if (!timer.current && visualState === "connected") {
        timer.current = setTimeout(() => {
          timer.current = null;
          setVisualState("disconnected");
        }, 1500);
      }
    }
  }, [rawState, visualState]);

  useEffect(() => {
    return () => {
      if (timer.current) clearTimeout(timer.current);
    };
  }, []);

  return visualState;
}

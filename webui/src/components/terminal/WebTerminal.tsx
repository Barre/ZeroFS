import { useEffect, useRef, useState, useCallback } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import { Loader2, RotateCcw, Cpu, ArrowDownUp } from "lucide-react";
import * as Tooltip from "@radix-ui/react-tooltip";
import { Button } from "../ui/Button";
import { Tip } from "../ui/Tip";
import { useNinePConnection } from "../../hooks/use-ninep-connection";

declare class V86 {
  constructor(options: Record<string, unknown>);
  add_listener(event: string, callback: (...args: unknown[]) => void): void;
  serial0_send(text: string): void;
  destroy(): void;
}

export function WebTerminal() {
  const [instanceKey, setInstanceKey] = useState(0);
  const [status, setStatus] = useState<"loading" | "booting" | "ready" | "error">("loading");
  const [cpuPercent, setCpuPercent] = useState(0);
  const [ioActive, setIoActive] = useState(false);
  const connState = useNinePConnection();
  const prevConnState = useRef(connState);
  const hasBooted = useRef(false);

  useEffect(() => {
    if (status === "ready") hasBooted.current = true;
  }, [status]);

  useEffect(() => {
    const prev = prevConnState.current;
    prevConnState.current = connState;
    if (prev !== "connected" && connState === "connected" && hasBooted.current) {
      hasBooted.current = false;
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setStatus("loading");
      setInstanceKey((k) => k + 1);
    }
  }, [connState]);

  const onCpuUpdate = useCallback((pct: number) => {
    setCpuPercent(pct);
  }, []);

  const ioTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const onIoFlicker = useCallback(() => {
    setIoActive((prev) => !prev);
    if (ioTimer.current) clearTimeout(ioTimer.current);
    ioTimer.current = setTimeout(() => setIoActive(false), 200);
  }, []);

  useEffect(() => {
    return () => {
      if (ioTimer.current) clearTimeout(ioTimer.current);
    };
  }, []);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-4 h-10 border-b border-border bg-card/40 shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium">Terminal</span>
          <span
            className={`inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded-full ${
              status === "ready"
                ? "bg-success/10 text-success"
                : status === "error"
                  ? "bg-destructive/10 text-destructive"
                  : "bg-accent text-muted"
            }`}
          >
            {status === "loading" && (
              <>
                <Loader2 size={10} className="animate-spin" /> Loading
              </>
            )}
            {status === "booting" && (
              <>
                <Loader2 size={10} className="animate-spin" /> Booting
              </>
            )}
            {status === "ready" && "Running"}
            {status === "error" && "Failed"}
          </span>
          {status === "ready" && (
            <div className="flex items-center gap-2 ml-1 text-xs">
              <span
                className={`flex items-center gap-1 font-mono tabular-nums ${cpuPercent > 50 ? "text-amber-400" : cpuPercent > 0 ? "text-muted" : "text-muted-foreground/40"}`}
              >
                <Cpu size={12} strokeWidth={1.5} />
                <span className="w-[4ch] text-right">{cpuPercent}%</span>
              </span>
              <ArrowDownUp
                size={12}
                strokeWidth={1.5}
                className={`transition-colors duration-100 ${ioActive ? "text-blue-400" : "text-muted-foreground/40"}`}
              />
            </div>
          )}
        </div>
        <Tooltip.Provider delayDuration={400} skipDelayDuration={100}>
          <div className="flex items-center gap-1">
            <Tip label="Restart VM">
              <Button
                variant="icon"
                onClick={() => {
                  setStatus("loading");
                  setInstanceKey((k) => k + 1);
                }}
              >
                <RotateCcw size={14} strokeWidth={1.5} />
              </Button>
            </Tip>
          </div>
        </Tooltip.Provider>
      </div>
      <VMInstance key={instanceKey} onStatusChange={setStatus} onCpuUpdate={onCpuUpdate} onIoFlicker={onIoFlicker} />
    </div>
  );
}

function VMInstance({
  onStatusChange,
  onCpuUpdate,
  onIoFlicker,
}: {
  onStatusChange: (status: "loading" | "booting" | "ready" | "error") => void;
  onCpuUpdate: (pct: number) => void;
  onIoFlicker: () => void;
}) {
  const termRef = useRef<HTMLDivElement>(null);
  const emulatorRef = useRef<V86 | null>(null);
  const readyRef = useRef(false);
  const origWsRef = useRef<typeof WebSocket>(WebSocket);

  useEffect(() => {
    if (!termRef.current) return;

    const term = new Terminal({
      cursorBlink: true,
      fontSize: 13,
      fontFamily: "'Geist Mono', 'SF Mono', monospace",
      convertEol: true,
      theme: {
        background: "#151b23",
        foreground: "#d1d7e0",
        cursor: "#478be6",
        selectionBackground: "#478be640",
        black: "#151b23",
        brightBlack: "#656c76",
        red: "#e5534b",
        brightRed: "#f47067",
        green: "#57ab5a",
        brightGreen: "#8ddb8c",
        yellow: "#c69026",
        brightYellow: "#daaa3f",
        blue: "#478be6",
        brightBlue: "#6cb6ff",
        magenta: "#b083f0",
        brightMagenta: "#dcbdfb",
        cyan: "#39c5cf",
        brightCyan: "#56d4dd",
        white: "#d1d7e0",
        brightWhite: "#f0f6fc",
      },
    });

    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(termRef.current);
    fit.fit();

    const script = document.createElement("script");
    script.src = "/v86/libv86.js";
    script.onload = () => {
      onStatusChange("booting");

      const wsProto = window.location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${wsProto}//${window.location.host}/ws/9p`;

      // Intercept v86's 9P WebSocket to track IO
      origWsRef.current = window.WebSocket;
      const RealWS = window.WebSocket;
      const ioThrottle = { last: 0 };
      const flicker = () => {
        const now = performance.now();
        if (now - ioThrottle.last > 80) {
          ioThrottle.last = now;
          onIoFlicker();
        }
      };
      window.WebSocket = class extends RealWS {
        constructor(url: string | URL, protocols?: string | string[]) {
          super(url, protocols);
          this.addEventListener("message", flicker);
        }
        send(data: string | ArrayBufferLike | Blob | ArrayBufferView) {
          flicker();
          super.send(data);
        }
      };

      const emulator = new V86({
        wasm_path: "/v86/v86.wasm",
        bios: { url: "/v86/seabios.bin" },
        bzimage: { url: "/v86/vmlinuz-virt" },
        initrd: { url: "/v86/initramfs-virt" },
        cmdline:
          "console=ttyS0 rw init=/bin/sh mitigations=off noapic notsc norandmaps acpi=off lpj=1000000 clocksource=kvm-clock libata.force=disable scsi_mod.scan=none i8042.noaux i8042.nokbd nmi_watchdog=0 nosmp audit=0 selinux=0 apparmor=0 pnp.do_scan=0 net.ifnames=0 ipv6.disable=1 modprobe.blacklist=ne2k_pci,8021q pci=norom",
        memory_size: 128 * 1024 * 1024,
        vga_memory_size: 0,
        filesystem: {
          proxy_url: wsUrl,
        },
        autostart: true,
        disable_keyboard: true,
        disable_mouse: true,
        disable_speaker: true,
        screen_dummy: true,
      });

      emulatorRef.current = emulator;

      emulator.add_listener("serial0-output-byte", (byte: unknown) => {
        term.write(new Uint8Array([byte as number]));
        if (!readyRef.current) {
          readyRef.current = true;
          onStatusChange("ready");
        }
      });

      // Parse terminal title updates from the init's stats reporter
      term.onTitleChange((title) => {
        // Format: "cpu=23;load=0.05"
        const parts = title.split(";");
        for (const part of parts) {
          const [key, val] = part.split("=");
          if (key === "cpu") onCpuUpdate(parseInt(val, 10) || 0);
        }
      });

      term.onData((data) => {
        emulator.serial0_send(data);
      });
    };

    script.onerror = () => onStatusChange("error");
    document.head.appendChild(script);

    const onResize = () => fit.fit();
    window.addEventListener("resize", onResize);
    const resizeObs = new ResizeObserver(() => fit.fit());
    resizeObs.observe(termRef.current);

    return () => {
      window.removeEventListener("resize", onResize);
      resizeObs.disconnect();
      emulatorRef.current?.destroy();
      emulatorRef.current = null;
      term.dispose();
      script.remove();
      window.WebSocket = origWsRef.current;
    };
  }, [onStatusChange, onCpuUpdate, onIoFlicker]);

  return <div ref={termRef} className="flex-1 min-h-0 bg-[#151b23] p-2" />;
}

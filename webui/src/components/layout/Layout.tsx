import { NavLink, Outlet } from "react-router";
import { FolderOpen, BarChart3, TerminalSquare, Loader2, WifiOff } from "lucide-react";
import * as Tooltip from "@radix-ui/react-tooltip";
import { useNinePVisualState } from "../../hooks/use-ninep-connection";

const navItems = [
  { to: "/files", label: "Files", icon: FolderOpen },
  { to: "/dashboard", label: "Monitor", icon: BarChart3 },
  { to: "/terminal", label: "Terminal", icon: TerminalSquare },
];

export function Layout() {
  const connectionState = useNinePVisualState();

  return (
    <Tooltip.Provider delayDuration={300}>
      <div className="flex h-screen">
        <nav className="flex flex-col items-center w-14 border-r border-border bg-card py-3 gap-1 shrink-0">
          <div className="w-10 h-10 flex items-center justify-center mb-3">
            <img src="/favicon.svg" alt="ZeroFS" className="w-7 h-7 invert" />
          </div>
          {navItems.map(({ to, label, icon: Icon }) => (
            <Tooltip.Root key={to}>
              <Tooltip.Trigger asChild>
                <div>
                  <NavLink
                    to={to}
                    className={({ isActive }) =>
                      isActive
                        ? "flex items-center justify-center w-10 h-10 rounded-lg bg-accent text-foreground border border-border-bright"
                        : "flex items-center justify-center w-10 h-10 rounded-lg text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                    }
                  >
                    <Icon size={18} strokeWidth={1.5} />
                  </NavLink>
                </div>
              </Tooltip.Trigger>
              <Tooltip.Portal>
                <Tooltip.Content
                  side="right"
                  sideOffset={8}
                  className="bg-[#3d444d] text-[#e6edf3] rounded-md px-2.5 py-1 text-xs font-medium z-50 select-none shadow-md animate-[tooltipIn_0.1s_ease-out]"
                >
                  {label}
                </Tooltip.Content>
              </Tooltip.Portal>
            </Tooltip.Root>
          ))}
        </nav>
        <main className="flex-1 overflow-auto bg-background relative">
          <Outlet />
          {connectionState !== "connected" && (
            <div className="absolute inset-0 bg-background/80 backdrop-blur-sm flex items-center justify-center z-[100]">
              <div className="flex flex-col items-center gap-4 text-center">
                {connectionState === "connecting" ? (
                  <>
                    <Loader2 size={28} className="animate-spin text-primary" />
                    <div>
                      <p className="text-sm font-medium">Connecting to server</p>
                      <p className="text-xs text-muted mt-1">Establishing 9P connection...</p>
                    </div>
                  </>
                ) : (
                  <>
                    <WifiOff size={28} className="text-destructive" />
                    <div>
                      <p className="text-sm font-medium">Connection lost</p>
                      <p className="text-xs text-muted mt-1">Attempting to reconnect...</p>
                    </div>
                    <Loader2 size={16} className="animate-spin text-muted" />
                  </>
                )}
              </div>
            </div>
          )}
        </main>
      </div>
    </Tooltip.Provider>
  );
}

import { lazy, Suspense, useState } from "react";
import { Routes, Route, useLocation } from "react-router";
import { Layout } from "./components/layout/Layout";
import { FilesPage } from "./pages/files";

const DashboardPage = lazy(() => import("./pages/dashboard").then((m) => ({ default: m.DashboardPage })));
const TerminalPage = lazy(() => import("./pages/terminal").then((m) => ({ default: m.TerminalPage })));

// Render a page once activated, keep it mounted (hidden) afterwards.
function KeepAlive({ active, children }: { active: boolean; children: React.ReactNode }) {
  const [mounted, setMounted] = useState(active);
  if (active && !mounted) setMounted(true);
  if (!mounted) return null;
  return <div className={`h-full ${active ? "" : "hidden"}`}>{children}</div>;
}

export default function App() {
  const { pathname } = useLocation();
  const page = pathname.startsWith("/dashboard")
    ? "dashboard"
    : pathname.startsWith("/terminal")
      ? "terminal"
      : "files";

  return (
    <Routes>
      <Route
        element={<Layout />}
      >
        <Route
          path="*"
          element={
            <>
              <KeepAlive active={page === "files"}>
                <FilesPage />
              </KeepAlive>
              <KeepAlive active={page === "dashboard"}>
                <Suspense fallback={<div className="p-6 text-muted">Loading dashboard...</div>}>
                  <DashboardPage />
                </Suspense>
              </KeepAlive>
              <KeepAlive active={page === "terminal"}>
                <Suspense fallback={<div className="p-6 text-muted">Loading terminal...</div>}>
                  <TerminalPage />
                </Suspense>
              </KeepAlive>
            </>
          }
        />
      </Route>
    </Routes>
  );
}

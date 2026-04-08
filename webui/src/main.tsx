import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "sonner";
import { TransferPanel } from "./components/transfers/TransferPanel";
import "./index.css";
import App from "./App";

const queryClient = new QueryClient();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <TransferPanel>
          <App />
        </TransferPanel>
        <Toaster
          theme="dark"
          position="bottom-center"
          closeButton
          toastOptions={{
            style: {
              background: "#1c2128",
              border: "1px solid #3d444d",
              color: "#d1d7e0",
              fontFamily: "var(--font-sans)",
              fontSize: "14px",
            },
          }}
        />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);

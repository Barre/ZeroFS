import { useTitle } from "../hooks/use-title";
import { WebTerminal } from "../components/terminal/WebTerminal";

export function TerminalPage() {
  useTitle("Terminal");
  return (
    <div className="h-full">
      <WebTerminal />
    </div>
  );
}

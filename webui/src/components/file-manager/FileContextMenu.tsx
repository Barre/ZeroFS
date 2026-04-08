import { useRef } from "react";
import * as ContextMenu from "@radix-ui/react-context-menu";
import { type LucideIcon } from "lucide-react";

export interface ContextMenuAction {
  label: string;
  icon: LucideIcon;
  onClick: () => void;
  destructive?: boolean;
  // Prevent Radix from restoring focus to the trigger when this action closes the menu.
  preventCloseAutoFocus?: boolean;
}

export function FileContextMenuWrapper({
  children,
  actions,
}: {
  children: React.ReactNode;
  actions: ContextMenuAction[];
}) {
  const preventAutoFocusRef = useRef(false);

  return (
    <ContextMenu.Root>
      <ContextMenu.Trigger asChild>{children}</ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Content
          className="z-50 min-w-[160px] card-surface-raised rounded-lg py-1 backdrop-blur-sm animate-[slideIn_0.1s_ease-out] select-none focus-visible:outline-none"
          onCloseAutoFocus={(e) => {
            if (preventAutoFocusRef.current) {
              e.preventDefault();
              preventAutoFocusRef.current = false;
            }
          }}
        >
          {actions.map((action, i) => (
            <div key={action.label}>
              {action.destructive && i > 0 && <ContextMenu.Separator className="my-1 border-t border-border" />}
              <ContextMenu.Item
                onSelect={() => {
                  if (action.preventCloseAutoFocus) preventAutoFocusRef.current = true;
                  action.onClick();
                }}
                className={`flex items-center gap-2.5 px-3 py-1.5 text-sm !outline-none cursor-default transition-colors ${
                  action.destructive
                    ? "text-destructive data-[highlighted]:bg-destructive/10"
                    : "text-foreground data-[highlighted]:bg-border/25"
                }`}
              >
                <action.icon size={14} strokeWidth={1.5} />
                {action.label}
              </ContextMenu.Item>
            </div>
          ))}
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  );
}

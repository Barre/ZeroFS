import * as Dialog from "@radix-ui/react-dialog";
import { Button } from "../ui/Button";
import { useFrozen } from "../../hooks/use-frozen";
import { overlayClass, contentClass } from "./dialog-classes";
import type { OverwritePrompt } from "./hooks/use-uploads";

interface DiscardConfirm {
  resolve: (ok: boolean) => void;
}

interface DeleteConfirm {
  names: string[];
  resolve: (ok: boolean) => void;
}

interface RenameConfirm {
  newName: string;
  resolve: (ok: boolean) => void;
}

interface ConfirmDialogsProps {
  discardConfirm: DiscardConfirm | null;
  deleteConfirm: DeleteConfirm | null;
  renameConfirm: RenameConfirm | null;
  overwritePrompt: OverwritePrompt | null;
}

const dialogContentClass = `${contentClass} w-[420px] max-w-[calc(100vw-2rem)]`;

function ConfirmDialog({
  open,
  onCancel,
  title,
  children,
  actions,
}: {
  open: boolean;
  onCancel: () => void;
  title: React.ReactNode;
  children: React.ReactNode;
  actions: React.ReactNode;
}) {
  return (
    <Dialog.Root open={open} onOpenChange={(o) => { if (!o) onCancel(); }}>
      <Dialog.Portal>
        <Dialog.Overlay className={overlayClass} />
        <Dialog.Content className={dialogContentClass}>
          <div className="px-5 py-4 border-b border-border">
            <Dialog.Title className="text-sm font-semibold truncate">{title}</Dialog.Title>
          </div>
          <div className="px-5 py-4">{children}</div>
          <div className="flex justify-end gap-2 px-5 py-3 border-t border-border">
            <Button variant="ghost" onClick={onCancel}>Cancel</Button>
            {actions}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

export function ConfirmDialogs({ discardConfirm, deleteConfirm, renameConfirm, overwritePrompt }: ConfirmDialogsProps) {
  const discard = useFrozen(discardConfirm);
  const del = useFrozen(deleteConfirm);
  const rename = useFrozen(renameConfirm);
  const overwrite = useFrozen(overwritePrompt);

  return (
    <>
      <ConfirmDialog
        open={!!discardConfirm}
        onCancel={() => discard?.resolve(false)}
        title="Unsaved changes"
        actions={
          <Button variant="destructive-filled" onClick={() => discard?.resolve(true)}>
            Discard
          </Button>
        }
      >
        <p className="text-sm text-muted">You have unsaved changes that will be lost.</p>
      </ConfirmDialog>

      <ConfirmDialog
        open={!!deleteConfirm}
        onCancel={() => del?.resolve(false)}
        title={
          del?.names.length === 1 ? (
            <span title={del.names[0]}>
              Delete <span className="font-mono">{del.names[0]}</span>?
            </span>
          ) : (
            `Delete ${del?.names.length ?? 0} items?`
          )
        }
        actions={
          <Button variant="destructive-filled" onClick={() => del?.resolve(true)}>
            Delete
          </Button>
        }
      >
        {del?.names.length === 1 ? (
          <p className="text-sm text-muted">This action cannot be undone.</p>
        ) : (
          <>
            <p className="text-sm text-muted mb-3">The following items will be permanently deleted:</p>
            <div className="bg-accent/50 rounded p-2 max-h-32 overflow-auto">
              {del?.names.map((name) => (
                <p key={name} className="text-sm font-mono py-0.5 truncate" title={name}>
                  {name}
                </p>
              ))}
            </div>
          </>
        )}
      </ConfirmDialog>

      <ConfirmDialog
        open={!!renameConfirm}
        onCancel={() => rename?.resolve(false)}
        title="Replace existing item?"
        actions={
          <Button variant="destructive-filled" onClick={() => rename?.resolve(true)}>
            Replace
          </Button>
        }
      >
        <p className="text-sm text-muted">
          <span className="font-mono text-foreground break-all">{rename?.newName}</span> already exists. Renaming
          will replace it.
        </p>
      </ConfirmDialog>

      <ConfirmDialog
        open={!!overwritePrompt}
        onCancel={() => overwrite?.resolve("cancel")}
        title={
          (overwrite?.existing.length ?? 0) === 1
            ? "File already exists"
            : `${overwrite?.existing.length ?? 0} files already exist`
        }
        actions={
          <>
            <Button variant="ghost" onClick={() => overwrite?.resolve("skip")}>
              Skip existing
            </Button>
            <Button variant="destructive-filled" onClick={() => overwrite?.resolve("overwrite")}>
              Overwrite
            </Button>
          </>
        }
      >
        <Dialog.Description className="text-sm text-muted mb-3">
          {(overwrite?.existing.length ?? 0) === 1 ? (
            <>
              The file <span className="font-mono text-foreground break-all">{overwrite?.existing[0]}</span>{" "}
              already exists in this directory.
            </>
          ) : (
            "The following files already exist in this directory:"
          )}
        </Dialog.Description>
        {(overwrite?.existing.length ?? 0) > 1 && (
          <div className="bg-accent/50 rounded p-2 max-h-32 overflow-auto">
            {overwrite?.existing.map((name) => (
              <p key={name} className="text-sm font-mono py-0.5 break-all">
                {name}
              </p>
            ))}
          </div>
        )}
      </ConfirmDialog>
    </>
  );
}

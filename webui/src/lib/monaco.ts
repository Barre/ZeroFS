import * as monaco from "monaco-editor";
import { loader } from "@monaco-editor/react";

import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import jsonWorker from "monaco-editor/esm/vs/language/json/json.worker?worker";
import cssWorker from "monaco-editor/esm/vs/language/css/css.worker?worker";
import htmlWorker from "monaco-editor/esm/vs/language/html/html.worker?worker";
import tsWorker from "monaco-editor/esm/vs/language/typescript/ts.worker?worker";

self.MonacoEnvironment = {
  getWorker(_: unknown, label: string) {
    if (label === "json") return new jsonWorker();
    if (label === "css" || label === "scss" || label === "less") return new cssWorker();
    if (label === "html" || label === "handlebars" || label === "razor") return new htmlWorker();
    if (label === "typescript" || label === "javascript") return new tsWorker();
    return new editorWorker();
  },
};

monaco.editor.defineTheme("github-dark", {
  base: "vs-dark",
  inherit: true,
  rules: [
    { token: "", foreground: "d1d7e0" },
    { token: "comment", foreground: "9198a1", fontStyle: "italic" },
    { token: "keyword", foreground: "f47067" },
    { token: "keyword.control", foreground: "f47067" },
    { token: "storage", foreground: "f47067" },
    { token: "storage.type", foreground: "f47067" },
    { token: "string", foreground: "96d0ff" },
    { token: "string.escape", foreground: "6cb6ff" },
    { token: "number", foreground: "6cb6ff" },
    { token: "constant", foreground: "6cb6ff" },
    { token: "constant.language", foreground: "6cb6ff" },
    { token: "variable", foreground: "f69d50" },
    { token: "variable.predefined", foreground: "6cb6ff" },
    { token: "entity.name.function", foreground: "dcbdfb" },
    { token: "entity.name.type", foreground: "f69d50" },
    { token: "entity.name.tag", foreground: "8ddb8c" },
    { token: "attribute.name", foreground: "6cb6ff" },
    { token: "attribute.value", foreground: "96d0ff" },
    { token: "type", foreground: "f69d50" },
    { token: "type.identifier", foreground: "f69d50" },
    { token: "delimiter", foreground: "d1d7e0" },
    { token: "delimiter.bracket", foreground: "d1d7e0" },
    { token: "operator", foreground: "f47067" },
    { token: "tag", foreground: "8ddb8c" },
    { token: "metatag", foreground: "d1d7e0" },
    { token: "annotation", foreground: "dcbdfb" },
    { token: "regexp", foreground: "8ddb8c" },
  ],
  colors: {
    "editor.background": "#212830",
    "editor.foreground": "#d1d7e0",
    "editor.lineHighlightBackground": "#262c36",
    "editor.selectionBackground": "#478be633",
    "editor.inactiveSelectionBackground": "#478be612",
    "editorCursor.foreground": "#478be6",
    "editorWhitespace.foreground": "#656c76",
    "editorIndentGuide.background": "#d1d7e01f",
    "editorIndentGuide.activeBackground": "#d1d7e03d",
    "editorLineNumber.foreground": "#576270",
    "editorLineNumber.activeForeground": "#d1d7e0",
    "editorBracketMatch.background": "#6bc46d40",
    "editorBracketMatch.border": "#6bc46d99",
    "scrollbarSlider.background": "#474e5733",
    "scrollbarSlider.hoverBackground": "#474e573d",
    "scrollbarSlider.activeBackground": "#474e5747",
    "editorOverviewRuler.border": "#010409",
    "editor.selectionHighlightBackground": "#6bc46d40",
    "editorWidget.background": "#2a313c",
    "editorWidget.border": "#3d444d",
  },
});

loader.config({ monaco });

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * What the dashboard composer (editor.js) and the notebook composer (notebook.js) show when a save succeeds but the service
 * attached warnings to the reply (#7565): the same text validate_custom_view returns, for a definition that saves and runs but
 * will not draw what its author meant (a perfmon counter ending in /sec charted as its running total). The rule lives in the
 * service's validator; this file only shows what it said, so the page never judges a definition a second time.
 */

import { el } from "./util.js";

/** The warning sentences a saved-view reply carries, or [] when it has none. */
export function savedViewWarnings(view) {
  const list = view && Array.isArray(view.warnings) ? view.warnings : [];
  return list.filter((w) => typeof w === "string" && w.trim() !== "");
}

/**
 * The strip shown in place of the redirect when a save came back with warnings: each sentence as text (never markup), then a
 * link to the saved view, so the person reads the advice before leaving the editor. `openHash` is the saved view's route.
 */
export function buildSaveWarningsStrip(warnings, openHash) {
  return el("div", { class: "strip notice", role: "status" }, [
    el("div", { text: "Saved. The service has a warning about this view:" }),
    ...warnings.map((w) => el("div", { text: w })),
    el("a", { href: openHash, text: "Open the view" }),
  ]);
}

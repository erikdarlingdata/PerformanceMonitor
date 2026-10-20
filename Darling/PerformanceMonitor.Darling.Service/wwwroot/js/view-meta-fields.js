/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The folder, personal and owner fields both editors show (#6214): the dashboard composer (editor.js) and the notebook
 * composer (notebook.js). Everything the user or the store typed reaches the page as text (el(), never markup). The rules for
 * what is allowed live in view-folders.js; the store enforces them, so a refusal still comes back as its own sentence in the
 * editor's error area.
 */

import { el } from "./util.js";
import { folderNames, MAX_FOLDER_LENGTH, ownerLine, personalChoice, viewAccess } from "./view-folders.js";

let listCounter = 0;

/**
 * Builds the fields. `meta` is the editor model ({ folder, personal }) and is updated as the user types. `view` is the stored
 * view being edited, or null for a new one. `session` is /api/session's answer. `views` is the list the sidebar read (for the
 * folder suggestions). `onChange` runs after any change so the editor can recompute its Save button.
 * Returns { node, readOnly, note }: `readOnly` is true for another person's personal view, which the store would refuse to
 * change, so the editor keeps Save disabled; `note` is the sentence saying why.
 */
export function buildViewMetaFields({ meta, view, session, views, onChange }) {
  const access = viewAccess(view, session);
  const choice = personalChoice(view, session);
  const listId = "view-folder-list-" + ++listCounter;

  const folderInput = el("input", {
    class: "editor-input",
    type: "text",
    placeholder: "Folder (optional)",
    "aria-label": "Folder",
    maxlength: String(MAX_FOLDER_LENGTH),
    list: listId,
  });
  folderInput.value = typeof meta.folder === "string" ? meta.folder : "";
  const datalist = el("datalist", { id: listId }, folderNames(views).map((name) => el("option", { value: name })));

  const personalBox = el("input", { class: "editor-check", type: "checkbox", "aria-label": "Personal (listed only for you)" });
  if (choice.disabled && !access.personal) meta.personal = false;
  personalBox.checked = meta.personal === true;
  personalBox.disabled = choice.disabled || access.readOnly;

  const folderNote = el("div", { class: "muted view-meta-note" });
  function syncFolder() {
    const personal = personalBox.checked;
    folderInput.disabled = access.readOnly || personal;
    folderNote.textContent = personal && !access.readOnly ? "A personal view is listed under My views, not in a folder." : "";
  }
  syncFolder();

  folderInput.addEventListener("input", () => {
    meta.folder = folderInput.value;
    if (onChange) onChange();
  });
  personalBox.addEventListener("change", () => {
    meta.personal = personalBox.checked;
    syncFolder();
    if (onChange) onChange();
  });

  const owner = ownerLine(view);
  const nodes = [
    el("div", { class: "editor-meta" }, [
      el("label", { class: "editor-field" }, [el("span", { class: "field-label", text: "Folder" }), folderInput, datalist]),
      el("div", { class: "editor-field field-enabled" }, [
        el("label", { class: "editor-check-row" }, [personalBox, el("span", { text: " Personal (listed only for you)" })]),
        choice.disabled && !access.readOnly ? el("div", { class: "muted view-meta-note", text: choice.reason }) : null,
      ]),
    ]),
    folderNote,
    owner ? el("div", { class: "muted view-owner-line", text: owner }) : null,
    access.readOnly ? el("div", { class: "strip empty view-readonly-note", text: access.note + ". It is read-only for you." }) : null,
  ];
  return { node: el("div", { class: "view-meta-fields" }, nodes), readOnly: access.readOnly, note: access.note };
}

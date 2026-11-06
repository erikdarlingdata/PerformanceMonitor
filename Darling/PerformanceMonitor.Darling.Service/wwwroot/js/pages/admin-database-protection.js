/* Admin > Settings: the Database Protection overdue ages (#7611). A full backup, a log backup and a CHECKDB each count as overdue
   once the last one is older than an age; the ages are settings, and a server or a database can override each in Threshold Overrides.

     GET  /api/read/get_alert_settings           the backup_overdue, log_backup_overdue and checkdb_overdue groups
     PUT  /api/alert-settings/database-protection  saves the age fields; update_alert_settings' own validation answers, and a
                                                   refusal's sentence is shown as the service wrote it

   The bounds below are shown and checked here only to save a round trip; the service checks them again and is the authority
   (they are the threshold override keys' bounds). A signed-in admin sees the form, anyone else sees the saved values as rows. */

import { el, apiWrite } from "../util.js";

/** The three ages: the group and field get_alert_settings carries, the words the box is labelled with, and the bounds. */
export const DATABASE_PROTECTION_AGES = [
  { group: "backup_overdue", field: "threshold_hours", label: "Full backup overdue when older than", unit: "hours", min: 1, max: 1440, fallback: 168 },
  { group: "log_backup_overdue", field: "threshold_minutes", label: "Log backup overdue when older than", unit: "minutes", min: 1, max: 86400, fallback: 60 },
  { group: "checkdb_overdue", field: "threshold_hours", label: "CHECKDB overdue when older than", unit: "hours", min: 1, max: 8760, fallback: 336 },
];

/** The sentence under a refused box, with the unit written out. */
export const ageRefusal = (age) => age.label.replace(/ when older than$/, "") + " age must be a whole number of " + age.unit + " between " + age.min + " and " + age.max + ".";

/** The PUT body from the boxes' text, or the first refusal: `{ body }` or `{ error }`. A box that is not a whole number inside its bounds is refused. */
export function databaseProtectionPayload(texts) {
  const body = {};
  for (const age of DATABASE_PROTECTION_AGES) {
    const text = String(texts[age.group] ?? "").trim();
    const n = /^\d+$/.test(text) ? Number(text) : NaN;
    if (!Number.isInteger(n) || n < age.min || n > age.max) return { error: ageRefusal(age) };
    body[age.group] = { [age.field]: n };
  }
  return { body };
}

/** The sentence a save answered with, and whether it is an error: the service's own `error` on a refusal, the saved words on a 200. */
export function databaseProtectionOutcome(res) {
  if (!res || res.status === 0) return { message: (res && res.message) || "Could not reach the service.", isError: true };
  if (res.expired) return { message: "Your session has expired. Sign in again.", isError: true };
  if (res.status >= 200 && res.status < 300) return { message: "Saved.", isError: false };
  const sentence = res.body && typeof res.body.error === "string" ? res.body.error : "";
  return { message: sentence || "The settings could not be saved (HTTP " + res.status + ").", isError: true };
}

function statusLine(out) {
  return el("div", { class: out.isError ? "strip error" : "strip notice", role: out.isError ? "alert" : "status", "data-field": "database-protection-status", text: out.message });
}

/* #7951: the boxes' text while a person types ({ [group]: text }), kept here so a redraw of the Settings tab (the 60 s poll, a save's
   repaint) draws the typed text and not the saved values. Null while nothing has been typed since the last read; a successful save
   clears it, so the repaint shows what was saved. */
let protectionDraft = null;

/** The editor, for a signed-in admin. `data` is the whole get_alert_settings read; each box starts at the saved value (the built-in value
    when the read does not carry the group). `repaint` redraws the Settings tab after a save so the saved values show. */
export function databaseProtectionEditor(data, repaint) {
  const d = data && typeof data === "object" ? data : {};
  const boxes = {};
  const read = () => {
    const texts = {};
    for (const age of DATABASE_PROTECTION_AGES) texts[age.group] = boxes[age.group].value;
    return texts;
  };
  const rows = DATABASE_PROTECTION_AGES.map((age) => {
    const saved = d[age.group] && typeof d[age.group] === "object" ? d[age.group][age.field] : undefined;
    const box = el("input", { type: "text", class: "tag-input", "data-field": "database-protection-" + age.group, inputmode: "numeric", size: "6" });
    box.value = protectionDraft && typeof protectionDraft[age.group] === "string" ? protectionDraft[age.group] : String(Number.isInteger(saved) ? saved : age.fallback);
    box.addEventListener("input", () => { protectionDraft = read(); });
    boxes[age.group] = box;
    return el("label", { class: "tag-field" }, [
      el("span", { class: "mute-label", text: age.label + " (" + age.unit + ", " + age.min + " to " + age.max + "):" }), box,
    ]);
  });

  const result = el("div", { "data-field": "database-protection-result" });
  let busy = false;
  const save = el("button", {
    class: "btn primary", type: "button", text: "Save", "data-action": "save-database-protection",
    onClick: async () => {
      if (busy) return;
      const checked = databaseProtectionPayload(read());
      if (checked.error) {
        result.replaceChildren(statusLine({ message: checked.error, isError: true }));
        return;
      }
      busy = true;
      let out;
      try {
        out = databaseProtectionOutcome(await apiWrite("PUT", "/api/alert-settings/database-protection", checked.body));
      } catch (e) {
        out = { message: "Could not save the settings: " + (e && e.message ? e.message : String(e)), isError: true };
      }
      busy = false;
      if (out.isError === false) protectionDraft = null;
      if (out.isError === false && repaint) return repaint();
      result.replaceChildren(statusLine(out));
    },
  });

  return el("div", { class: "tag-form", "data-database-protection-form": "1" }, [
    el("div", { class: "mute-label", text: "Database Protection overdue ages. A server or a database can override each one in Threshold Overrides." }),
    ...rows,
    el("div", { class: "tag-field" }, [save]),
    result,
  ]);
}

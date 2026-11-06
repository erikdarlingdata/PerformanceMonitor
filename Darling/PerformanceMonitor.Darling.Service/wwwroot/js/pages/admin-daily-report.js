/* Admin > Settings: the Daily Health Report (#7041). The report goes out once a day through the alert pipeline, as the
   "Daily Health Report" metric, and ships off.

     GET  /api/read/get_alert_settings      the daily_report group: enabled, send_at, time_zone, title
     PUT  /api/alert-settings/daily-report  saves them; update_alert_settings' own validation answers, and a refusal's
                                            sentence is shown as the service wrote it
     GET  /api/read/get_daily_health_report the Preview: the report for the last 24 hours, nothing sent

   A signed-in admin sees the form, anyone else sees the saved values as rows. Self-contained: admin.js hands this file its
   helpers, the way admin-agent-setup.js gets them. */

import { el, readTool, apiWrite, browserZoneOrBlank } from "../util.js";

/** The PUT body from the form's values. A blank time zone or title is sent as null, which clears the saved value (a blank zone
    means the service host's zone, a blank title means "Daily Health Report"). */
export function dailyReportPayload(values) {
  const blank = (s) => (String(s ?? "").trim() === "" ? null : String(s).trim());
  return {
    enabled: values.enabled === true,
    send_at: String(values.send_at ?? "").trim(),
    time_zone: blank(values.time_zone),
    title: blank(values.title),
  };
}

/** The sentence a save answered with, and whether it is an error: the service's own `error` on a refusal, the saved words on a 200. */
export function dailyReportOutcome(res) {
  if (!res || res.status === 0) return { message: (res && res.message) || "Could not reach the service.", isError: true };
  if (res.expired) return { message: "Your session has expired. Sign in again.", isError: true };
  if (res.status >= 200 && res.status < 300) return { message: "Saved.", isError: false };
  const sentence = res.body && typeof res.body.error === "string" ? res.body.error : "";
  return { message: sentence || "The settings could not be saved (HTTP " + res.status + ").", isError: true };
}

function statusLine(out) {
  return el("div", { class: out.isError ? "strip error" : "strip notice", role: out.isError ? "alert" : "status", "data-field": "daily-report-status", text: out.message });
}

/** The Preview result as nodes: the subject line and the short text when the read carries them, otherwise the answer as JSON. */
export function previewNodes(res) {
  if (!res || res.kind === "aborted" || res.kind === "auth") return [];
  if (res.kind === "error" || res.kind === "empty") return [statusLine({ message: res.message || "The preview came back empty.", isError: res.kind === "error" })];
  const d = res.data;
  const nodes = [];
  if (d && typeof d === "object" && !Array.isArray(d)) {
    if (typeof d.subject === "string") nodes.push(el("h4", { class: "section-title", text: d.subject }));
    if (typeof d.text === "string") nodes.push(el("pre", { class: "mp-pre", "data-field": "daily-report-preview", text: d.text }));
  }
  if (!nodes.length) nodes.push(el("pre", { class: "mp-pre", "data-field": "daily-report-preview", text: JSON.stringify(d, null, 2) }));
  return nodes;
}

/* #7951: the Preview, kept across a redraw of the Settings tab (the 60 s poll, a save's repaint): whether its read is in flight, the
   nodes its last answer drew, and the box on the page now. A redraw shows the result (or the building strip) in its new box, and a read
   that answers after a redraw lands in the box on the page, not in the one taken off it. */
const preview = { busy: false, nodes: null, box: null };
const buildingStrip = () => el("div", { class: "strip empty", text: "Building the preview..." });

/** The Preview button and the box it draws into. A read (get_daily_health_report), so it works with or without the right to save.
    `getBusy`/`setBusy` let the editor's Save and Preview share one in-flight flag. */
function previewControls(getBusy, setBusy) {
  const previewBox = el("div", { "data-field": "daily-report-preview-box" });
  preview.box = previewBox;
  if (preview.busy) previewBox.replaceChildren(buildingStrip());
  else if (preview.nodes) previewBox.replaceChildren(...preview.nodes);
  const button = el("button", {
    class: "btn", type: "button", text: "Preview", "data-action": "preview-daily-report",
    onClick: async () => {
      if (getBusy() || preview.busy) return;
      setBusy(true);
      preview.busy = true;
      preview.nodes = null;
      preview.box.replaceChildren(buildingStrip());
      let nodes;
      try {
        nodes = previewNodes(await readTool("get_daily_health_report", {}));
      } catch (e) {
        nodes = [statusLine({ message: "Could not build the preview: " + (e && e.message ? e.message : String(e)), isError: true })];
      }
      preview.busy = false;
      preview.nodes = nodes;
      preview.box.replaceChildren(...nodes);
      setBusy(false);
    },
  });
  return { preview: button, previewBox };
}

/** #7700: the preview alone, for a can-edit seat whose service refuses writes (web.allowAdminWrites off). Preview only reads, so it
    stays. Save and the inputs that only feed Save are not drawn. */
export function dailyReportPreviewOnly() {
  let busy = false;
  const { preview, previewBox } = previewControls(() => busy, (v) => { busy = v; });
  return el("div", { class: "tag-form", "data-daily-report-preview-only": "1" }, [
    el("div", { class: "tag-field" }, [preview]),
    previewBox,
  ]);
}

/* #7951: what the editor's boxes hold while a person types ({ enabled, send_at, time_zone, title }), kept here so a redraw of the
   Settings tab draws the typed values and not the saved ones. Null while nothing has been typed since the tab was drawn. A successful
   save clears it, so the repaint shows what was saved, and so do a switch to another Admin tab and leaving Admin (admin.js calls
   clearDailyReportDraft), so coming back shows the saved values and not unsaved text as if it had been saved. */
let reportDraft = null;

/** Drops the typed values, so the next draw of the editor shows the saved ones. */
export function clearDailyReportDraft() {
  reportDraft = null;
}

/** The editor, for a signed-in admin. `group` is the read's daily_report group (or undefined on a service that predates it);
    `repaint` redraws the Settings tab after a save so the saved values show. */
export function dailyReportEditor(group, repaint) {
  const g = group && typeof group === "object" ? group : {};
  const saved = {
    enabled: g.enabled === true,
    send_at: typeof g.send_at === "string" ? g.send_at : "07:00",
    /* A report that is off and has no zone yet starts at the browser's zone, since the person setting it up is most likely in the
       zone they want the report in. A saved zone, or a saved blank on a report that is already on, is left as it is. */
    time_zone: typeof g.time_zone === "string" ? g.time_zone : (g.enabled === true ? "" : browserZoneOrBlank()),
    title: typeof g.title === "string" ? g.title : "",
  };
  const values = reportDraft || saved;
  const enabled = el("input", { type: "checkbox", "data-field": "daily-report-enabled" });
  enabled.checked = values.enabled === true;
  const sendAt = el("input", { type: "time", class: "tag-input", "data-field": "daily-report-send-at", step: "60" });
  sendAt.value = values.send_at;
  const zone = el("input", {
    type: "text", class: "tag-input", "data-field": "daily-report-time-zone", maxlength: "64",
    placeholder: "Blank uses the service host's time zone",
  });
  zone.value = values.time_zone;
  const title = el("input", {
    type: "text", class: "tag-input", "data-field": "daily-report-title", maxlength: "100",
    placeholder: "Daily Health Report",
  });
  title.value = values.title;
  const read = () => ({ enabled: enabled.checked, send_at: sendAt.value, time_zone: zone.value, title: title.value });
  const sync = () => { reportDraft = read(); };
  for (const c of [enabled, sendAt, zone, title]) c.addEventListener("input", sync);

  const result = el("div", { "data-field": "daily-report-result" });
  let busy = false;

  const save = el("button", {
    class: "btn primary", type: "button", text: "Save", "data-action": "save-daily-report",
    onClick: async () => {
      if (busy || preview.busy) return;
      busy = true;
      let out;
      try {
        const res = await apiWrite("PUT", "/api/alert-settings/daily-report", dailyReportPayload(read()));
        out = dailyReportOutcome(res);
      } catch (e) {
        out = { message: "Could not save the settings: " + (e && e.message ? e.message : String(e)), isError: true };
      }
      busy = false;
      if (out.isError === false) reportDraft = null;
      if (out.isError === false && repaint) return repaint();
      result.replaceChildren(statusLine(out));
    },
  });

  const { preview, previewBox } = previewControls(() => busy, (v) => { busy = v; });

  return el("div", { class: "tag-form", "data-daily-report-form": "1" }, [
    el("label", { class: "tag-field" }, [enabled, el("span", { text: " Send a daily health report" })]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Send at (HH:mm, in the time zone below):" }), sendAt]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Time zone (an IANA id such as America/New_York; blank uses the service host's zone):" }), zone]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Title (optional):" }), title]),
    el("div", { class: "tag-field" }, [save, " ", preview]),
    result,
    previewBox,
  ]);
}

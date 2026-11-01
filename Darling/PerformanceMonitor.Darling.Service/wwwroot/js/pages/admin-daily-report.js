/* Admin > Settings: the Daily Health Report (#7041). The report goes out once a day through the alert pipeline, as the
   "Daily Health Report" metric, and ships off.

     GET  /api/read/get_alert_settings      the daily_report group: enabled, send_at, time_zone, title
     PUT  /api/alert-settings/daily-report  saves them; update_alert_settings' own validation answers, and a refusal's
                                            sentence is shown as the service wrote it
     GET  /api/read/get_daily_health_report the Preview: the report for the last 24 hours, nothing sent

   A signed-in admin sees the form, anyone else sees the saved values as rows. Self-contained: admin.js hands this file its
   helpers, the way admin-agent-setup.js gets them. */

import { el, readTool, apiWrite } from "../util.js";

/** The browser's IANA zone ("America/New_York"), or "" when the browser does not say. */
export function browserTimeZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "";
  } catch {
    return "";
  }
}

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

/** The editor, for a signed-in admin. `group` is the read's daily_report group (or undefined on a service that predates it);
    `repaint` redraws the Settings tab after a save so the saved values show. */
export function dailyReportEditor(group, repaint) {
  const g = group && typeof group === "object" ? group : {};
  const enabled = el("input", { type: "checkbox", "data-field": "daily-report-enabled" });
  enabled.checked = g.enabled === true;
  const sendAt = el("input", { type: "time", class: "tag-input", "data-field": "daily-report-send-at", step: "60" });
  sendAt.value = typeof g.send_at === "string" ? g.send_at : "07:00";
  const zone = el("input", {
    type: "text", class: "tag-input", "data-field": "daily-report-time-zone", maxlength: "64",
    placeholder: "Blank uses the service host's time zone",
  });
  /* A report that is off and has no zone yet starts at the browser's zone, since the person setting it up is most likely in the
     zone they want the report in. A saved zone, or a saved blank on a report that is already on, is left as it is. */
  zone.value = typeof g.time_zone === "string" ? g.time_zone : (g.enabled === true ? "" : browserTimeZone());
  const title = el("input", {
    type: "text", class: "tag-input", "data-field": "daily-report-title", maxlength: "100",
    placeholder: "Daily Health Report",
  });
  title.value = typeof g.title === "string" ? g.title : "";

  const result = el("div", { "data-field": "daily-report-result" });
  const previewBox = el("div", { "data-field": "daily-report-preview-box" });
  let busy = false;

  const save = el("button", {
    class: "btn primary", type: "button", text: "Save", "data-action": "save-daily-report",
    onClick: async () => {
      if (busy) return;
      busy = true;
      let out;
      try {
        const res = await apiWrite("PUT", "/api/alert-settings/daily-report",
          dailyReportPayload({ enabled: enabled.checked, send_at: sendAt.value, time_zone: zone.value, title: title.value }));
        out = dailyReportOutcome(res);
      } catch (e) {
        out = { message: "Could not save the settings: " + (e && e.message ? e.message : String(e)), isError: true };
      }
      busy = false;
      if (out.isError === false && repaint) return repaint();
      result.replaceChildren(statusLine(out));
    },
  });

  const preview = el("button", {
    class: "btn", type: "button", text: "Preview", "data-action": "preview-daily-report",
    onClick: async () => {
      if (busy) return;
      busy = true;
      previewBox.replaceChildren(el("div", { class: "strip empty", text: "Building the preview..." }));
      try {
        previewBox.replaceChildren(...previewNodes(await readTool("get_daily_health_report", {})));
      } catch (e) {
        previewBox.replaceChildren(statusLine({ message: "Could not build the preview: " + (e && e.message ? e.message : String(e)), isError: true }));
      }
      busy = false;
    },
  });

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

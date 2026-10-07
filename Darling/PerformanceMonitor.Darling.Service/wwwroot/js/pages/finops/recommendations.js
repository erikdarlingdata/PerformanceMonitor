/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Recommendations" tab: the cost-saving recommendations from get_finops_recommendations, in the order the
   read returns them (High severity first), with the desktop's six columns. Differences from the desktop: no filter
   buttons, no tooltips, no click-to-sort and no Refresh button (the page polls), a missing saving shows as an
   em dash, and the finding and detail text comes from the service in invariant format (a percent reads "20 %"). */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

const SEV = { High: "Critical", Medium: "Warning", Low: "Healthy" };

const COLUMNS = [
  { key: "category", label: "Category" },
  { key: "severity", label: "Severity", sevKey: "severity_sev" },
  { key: "confidence", label: "Confidence" },
  { key: "finding", label: "Finding", wrap: true },
  { key: "detail", label: "Detail", wrap: true },
  { key: "est_savings_usd_month", label: "Est. Savings ($/mo)", format: "int" },
];

function noticeText(n) {
  if (n === 0) return "No recommendations.";
  return n + (n === 1 ? " recommendation" : " recommendations") + ", High severity first.";
}

function displayRow(r) {
  return { ...r, severity_sev: SEV[r.severity] ?? null };
}

export const tab = {
  id: "recommendations",
  label: "Recommendations",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops_recommendations", { server }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        const rows = (data.recommendations || []).map(displayRow);
        const skipped = data.skipped_checks || [];
        const parts = [noticeStrip(noticeText(rows.length))];
        if (data.monthly_cost_usd == null) parts.push(noticeStrip("Monthly cost not set for this server, so savings estimates are blank."));
        if (skipped.length) parts.push(noticeStrip("Some checks could not run, so their findings are missing (not clean): " + skipped.join(", ") + "."));
        parts.push(VIZ.table({ rows }, { rowsKey: "rows", columns: COLUMNS, emptyText: "No recommendations: nothing needs attention, or not enough has been collected yet." }));
        mount(body, parts);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    })();
    return body;
  },
};

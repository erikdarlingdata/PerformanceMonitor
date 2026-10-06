/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * What "Mute this alert" pre-fills into the Mute Rules form from an Alert History row. Pure functions, no DOM
 * and no imports, so MuteRulesBehaviourTests runs them under Node.
 *
 * The rule has to match the alert it came from. The row's server_name is the registry DISPLAY name, which can
 * differ from the spelling the producer's mute context carries, so a by-name rule built from it can miss:
 *   - a row with a store id (server_id != 0) is keyed on that id, which the producer's context carries, and the
 *     stored spelling rides along as the label (the Viewer does the same);
 *   - a row with no id (the self-alert family, written under server_id 0) matches by name against its own stored
 *     spelling, so that is what is pre-filled (stored_server_name, falling back to server_name on an old payload).
 * The database, wait type, job and query dimensions come from the row's detail text, the same parse the Viewer's
 * AlertMuteContext.PopulateFromDetailText applies.
 */

const QUERY_PREFIXES = ["Query: ", "Blocked Query: ", "Blocking Query: ", "Victim SQL: "];
const QUERY_MAX = 200;
/* What a collector stores for a statement it withheld (SensitiveStatements.PlaceholderText, #4348). A mute rule on
 * it would match every withheld statement and nothing else, so no query pattern is seeded from it. */
const WITHHELD_MARKER = "-- statement text withheld (#4348)";

/** Parses an alert's detail text into the mute-context dimensions. Mirrors AlertMuteContext.PopulateFromDetailText:
 *  indented "Label: value" lines, a multi-line query value, and no parse at all for a custom alert. */
export function parseDetailContext(detailText, metricName) {
  const ctx = { database: null, waitType: null, jobName: null, queryText: null };
  if (!detailText) return ctx;
  if (typeof metricName === "string" && metricName.startsWith("Custom:")) return ctx;

  let query = null;
  const flush = () => {
    if (query !== null && ctx.queryText === null) ctx.queryText = query;
    query = null;
  };

  for (const line of String(detailText).split("\n")) {
    const trimmed = line.trimStart();
    const prefix = QUERY_PREFIXES.find((p) => trimmed.startsWith(p));
    if (ctx.database === null && trimmed.startsWith("Database: ")) {
      flush();
      ctx.database = trimmed.substring("Database: ".length).trim();
    } else if (ctx.waitType === null && trimmed.startsWith("Wait Type: ")) {
      flush();
      ctx.waitType = trimmed.substring("Wait Type: ".length).trim();
    } else if (ctx.jobName === null && trimmed.startsWith("Job Name: ")) {
      flush();
      ctx.jobName = trimmed.substring("Job Name: ".length).trim();
    } else if (ctx.queryText === null && query === null && prefix) {
      query = trimmed.substring(prefix.length).trim();
    } else if (query !== null) {
      if (trimmed.trim() === "" || line.startsWith("  ")) flush();
      else query += " " + trimmed.trim();
    }
  }
  flush();
  return ctx;
}

/** The query-string parameters "Mute this alert" opens the create form with. Empty values are dropped by buildQuery. */
export function mutePrefillParams(a) {
  const stored = a.stored_server_name || a.server_name;
  const id = Number(a.server_id);
  const d = parseDetailContext(a.detail_text, a.metric_name);
  const params = {
    server_id: Number.isInteger(id) && id !== 0 ? id : null,
    server_name: stored,
    metric_name: a.metric_name,
    database_pattern: d.database,
    wait_type_pattern: d.waitType,
    query_text_pattern: d.queryText === WITHHELD_MARKER ? null
      : d.queryText && d.queryText.length > QUERY_MAX ? d.queryText.substring(0, QUERY_MAX) : d.queryText,
    job_name_pattern: d.jobName,
  };
  return params;
}

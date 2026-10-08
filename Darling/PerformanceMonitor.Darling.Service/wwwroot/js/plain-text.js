/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Plain words for text the web page shows that the service wrote for an MCP client.

   The service's messages are one text for every reader. An MCP client can pass `hours_back`, read `hints.captures` and call
   `get_collection_health`; a person on the web page cannot, so the same sentence reads as internal names there. The service
   text stays as it is (the client needs those names and many tests pin it); the page runs what it shows through plainText()
   and shows the result.

   Only a short list of known names is rewritten, never "every snake_case word": a PostgreSQL object such as
   pg_stat_statements is a real name the reader may need. */

/** A tool name, in the words its page uses: get_collection_health -> Collection Health. */
function toolWords(name) {
  return name
    .split("_")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

/** The shown form of `label=value` counts: a run of pairs becomes "label words: value, label words: value". */
function pairsToText(run) {
  return run
    .split(" ")
    .map((pair) => {
      const eq = pair.indexOf("=");
      return pair.slice(0, eq).replace(/_/g, " ") + ": " + pair.slice(eq + 1);
    })
    .join(", ");
}

/* Fields of the Collection Health row that its own sentences point at, named by the column that shows them. */
const FIELD_WORDS = [
  [/\blast_error\b/g, "Last Error"],
  [/\blast_note\b/g, "Note"],
  [/\bnote_count\b/g, "the note count"],
  [/\btotal_runs\b/g, "Runs"],
  [/\bsession_missing\b/g, "session missing"],
];

/**
 * `text` with the internal names a web reader cannot use put into words. Text with none of them comes back unchanged.
 * Not a string (null, a number) comes back as it is.
 */
export function plainText(text) {
  if (typeof text !== "string" || text === "") return text;
  if (text.includes(ENGINE_GATE_MARK)) return NOT_COLLECTED_LINE;
  let t = text;

  /* A pointer into the MCP answer's own `hints`, which the page never shows. */
  t = t.replace(/\s*[—–-]+\s*see hints\.captures for which collectors ran and when/g, "");

  /* The health sentence's denial flag, said in words. The "true" form carries its own explanation after the dash. */
  t = t.replace(/, and denied_since_last_success is true - the newest denial postdates the newest success,/g, ", and the newest denial is newer than the newest success,");
  t = t.replace(/ \(denied_since_last_success is false\)/g, "");
  t = t.replace(/\bdenied_since_last_success is true\b/g, "a denial is newer than the last success");
  t = t.replace(/\bdenied_since_last_success is false\b/g, "no denial is newer than the last success");

  /* The window parameter an MCP client passes; the page calls it the time range. */
  t = t.replace(/\bwiden days_back\b/g, "widen the date range");
  t = t.replace(/\bmove as_of\b/g, "move the end date");
  t = t.replace(/\bdays_back\b/g, "the date range");
  t = t.replace(/\bwiden hours_back\b/g, "widen the time range");
  t = t.replace(/\bhours_back\b/g, "the time range");

  t = t.replace(/\bCheck list_servers\b/g, "Check the server list");
  t = t.replace(/\bpg_wraparound_stats\b/g, "the freeze-headroom collector");

  for (const [re, words] of FIELD_WORDS) t = t.replace(re, words);

  /* A collector's measurement counts: shred_gated=1 events_read=0 report_xml_empty=0. */
  t = t.replace(/\b[a-z][a-z0-9]*(?:_[a-z0-9]+)*=\d+(?: [a-z][a-z0-9]*(?:_[a-z0-9]+)*=\d+)*/g, pairsToText);

  /* A tool name: use get_collection_health -> use Collection Health. */
  t = t.replace(/\bget_([a-z0-9]+(?:_[a-z0-9]+)*)\b/g, (_m, rest) => toolWords(rest));

  return t;
}

/** The one short line the page shows where the server says a collector can never run on this kind of server. The
    server's own sentence names the collector, the engine gate and the way it is decided, none of which helps a reader. */
export const NOT_COLLECTED_LINE = "This server does not collect this data (it does not apply to this kind of server).";

/** The words every engine-gate sentence ends on (CollectorEngineCapability's permanent-gap epilogue). A "not collected"
    answer that is something else, such as a plan that was never stored, does not carry them and keeps its own sentence. */
const ENGINE_GATE_MARK = "permanent engine capability gap";

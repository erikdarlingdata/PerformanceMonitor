/* Runs the shipped Recommendations tab (wwwroot/js/pages/analysis-findings.js) against a recording fetch and a
   node-tree DOM, and prints the result of a scenario as one line of JSON.
       node analysis-findings-harness.mjs <path to wwwroot/js> <scenario> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.handlers = {};
    this.text = text == null ? null : String(text);
    this.open = false;
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { this.children.push(c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  setAttribute(n, v) { this.attrs[n] = String(v); if (n === "open") this.open = true; }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  addEventListener(t, fn) { this.handlers[t] = fn; }
  set textContent(v) { this.children = []; this.text = String(v); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  body: new FakeNode("body"),
};

const all = (n, pred, out = []) => { if (pred(n)) out.push(n); n.children.forEach((c) => all(c, pred, out)); return out; };
const byTag = (n, tag) => all(n, (x) => x.tag === tag);
const clipboard = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clipboard.push(t); } } }, configurable: true });

const fetches = [];
let body = "{}";
const signals = [];
globalThis.fetch = async (url, opts) => { fetches.push(String(url)); signals.push(opts && opts.signal ? opts.signal : null); return { status: 200, ok: true, text: async () => body }; };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "analysis-findings-"));
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const mod = await import(pathToFileURL(path.join(scratch, "pages", "analysis-findings.js")).href);

  const finding = (over) => ({
    severity: 2, category: "cpu", story_path: "path", incident_id: "inc-1", occurrences: 3,
    last_seen: "2026-01-01T00:00:00Z", time_range: { start: "2026-01-01T00:00:00Z", end: "2026-01-01T01:00:00Z" },
    advice: { headline: "Head", investigation: "Look here", remediation: "Do this" }, ...over,
  });
  const FIX = "ALTER DATABASE <b>x</b> SET AUTO_SHRINK OFF;";
  const sample = [
    finding({ finding_id: 1, severity: 0.9, advice: { headline: "Minor", investigation: "i", remediation: "r" } }),
    finding({ finding_id: 2, severity: 2.1, advice: { headline: "Major", investigation: "i", remediation: "r" } }),
    finding({ finding_id: 3, severity: 0.2, incident_id: "", advice: { headline: "Solo info", investigation: "i", remediation: "r" } }),
    finding({ finding_id: 4, severity: 1.6, incident_id: "inc-2", remediation_command: FIX, structured_remediation: null, is_config_fix: true, advice: { headline: "Config", investigation: "i", remediation: "r" } }),
  ];
  const settle = () => new Promise((r) => setTimeout(r, 20));
  const run = async (server, payload, ctx) => {
    fetches.length = 0;
    body = JSON.stringify(payload || { findings: sample });
    const root = mod.analysisFindingsTab.build(server, ctx || { hours: 24, label: "last 24 hours" });
    await settle();
    return root[0];
  };
  const groupsOf = (root) => byTag(root, "details");
  const sum = (d) => d.children[0].textContent;

  const scenarios = {
    read: async () => {
      await run("srv-a");
      const u = new URL(fetches[0], "http://x.test");
      return { path: u.pathname, params: Object.fromEntries(u.searchParams) };
    },
    grouping: async () => {
      const root = await run("srv-a");
      const groups = groupsOf(root);
      return {
        headers: groups.map(sum),
        cardsPerGroup: groups.map((g) => byTag(g, "h4").length),
        headlineCounts: groups.map((g, i) => g.textContent.split(["Major", "Config", "Solo info"][i]).length - 1),
        open: groups.map((g) => g.open),
        order: ["Critical", "Warning", "Info"].map((l) => root.textContent.indexOf(l)),
        badgeClasses: byTag(root, "span").filter((s) => /^badge /.test(s.className)).map((s) => s.className),
      };
    },
    fix: async () => {
      const root = await run("srv-a");
      const pre = byTag(root, "pre");
      const copy = byTag(root, "button").find((b) => b.textContent === "Copy fix");
      copy.handlers.click();
      await settle();
      return { pre: pre.map((p) => p.textContent), preChildren: pre.map((p) => p.children.length), clipboard, label: copy.textContent, buttons: byTag(root, "button").length };
    },
    askAi: async () => {
      const root = await run("srv-a");
      body = JSON.stringify({ prompt: "THE PROMPT" });
      fetches.length = 0;
      clipboard.length = 0;
      const ask = byTag(root, "button").find((b) => b.textContent === "Ask AI");
      ask.handlers.click();
      await settle();
      const u = new URL(fetches[0], "http://x.test");
      return { path: u.pathname, params: Object.fromEntries(u.searchParams), clipboard, label: ask.textContent, buttons: byTag(root, "button").filter((b) => b.textContent === "Ask AI" || b.textContent === "AI prompt copied").length };
    },
    link: async () => {
      const root = await run("srv-a");
      return byTag(root, "a").map((a) => a.attrs.href);
    },
    linkKinds: async () => {
      // A force-plan incident carries structured_remediation but is not a config fix; a config fix carries is_config_fix.
      const forcePlan = finding({ finding_id: 1, incident_id: "a", structured_remediation: { eligible: true }, is_config_fix: false, advice: { headline: "ForcePlan", investigation: "i", remediation: "r" } });
      const config = finding({ finding_id: 2, incident_id: "b", structured_remediation: null, is_config_fix: true, advice: { headline: "ConfigFix", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [forcePlan, config] });
      return groupsOf(root).map((g) => ({ head: sum(g), links: byTag(g, "a").length }));
    },
    standingLink: async () => {
      /* #7416: a setting or a volume's free space is a standing state, so no running query explains it and the Queries link is not offered. */
      const volume = finding({ finding_id: 1, incident_id: "a", standing_state: true, is_config_fix: false, advice: { headline: "VolumeLow", investigation: "i", remediation: "r" } });
      const wait = finding({ finding_id: 2, incident_id: "b", standing_state: false, is_config_fix: false, advice: { headline: "WaitSpike", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [volume, wait] });
      return groupsOf(root).map((g) => ({ head: sum(g), links: byTag(g, "a").map((a) => a.textContent) }));
    },
    sameRoot: async () => {
      /* #7416: two chains on one root in one incident are one card, with the newest chain's figures. */
      const latch = (over) => finding({ incident_id: "inc-1", root_fact: { key: "LATCH_EX", value: 1 }, advice: { headline: "Latch contention", investigation: "i", remediation: "r" }, ...over });
      const newest = latch({ finding_id: 1, severity: 0.8, story_path: "LATCH_EX → CXPACKET", last_seen: "2026-01-01T10:00:00Z", occurrences: 5 });
      const older = latch({ finding_id: 2, severity: 0.3, story_path: "LATCH_EX → TEMPDB_USAGE", last_seen: "2026-01-01T08:30:00Z", occurrences: 2 });
      const other = finding({ finding_id: 3, incident_id: "inc-1", severity: 0.6, root_fact: { key: "CPU_SPIKE", value: 1 }, advice: { headline: "CPU", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [newest, older, other] });
      const groups = groupsOf(root);
      return { headers: groups.map(sum), cards: byTag(root, "h4").map((h) => h.textContent), text: root.textContent };
    },
    sameRootOtherSentence: async () => {
      /* #7416, #7696: two chains on one root with two sentences are one card, whatever the headlines say: the chains are listed in words under it. */
      const latch = (over) => finding({ incident_id: "inc-1", root_fact: { key: "LATCH_EX", value: 1 }, ...over });
      const newest = latch({ finding_id: 1, severity: 0.8, story_path: "LATCH_EX → CXPACKET", last_seen: "2026-01-01T10:00:00Z", occurrences: 5, advice: { headline: "Latch contention", investigation: "i", remediation: "r" } });
      const older = latch({ finding_id: 2, severity: 0.3, story_path: "LATCH_EX → TEMPDB_USAGE", last_seen: "2026-01-01T08:30:00Z", occurrences: 2, advice: { headline: "Tempdb latch contention", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [newest, older] });
      return { cards: all(root, (x) => /\breco-card\b/.test(x.className)).length, text: root.textContent };
    },
    pileupTwoStatements: async () => {
      /* #7582: two pileups on different statements print one headline, but each is its own card with its own remediation. */
      const head = "4 sessions ran one statement for 30 s";
      const pile = (over) => finding({ root_fact: { key: "SAME_STATEMENT_PILEUP", value: 1 }, ...over });
      const a = pile({ finding_id: 1, incident_id: "inc-a", story_path: "SAME_STATEMENT_PILEUP → 0x1A2B3C4D5E6F7A8B", occurrences: 3, advice: { headline: head, investigation: "i", remediation: "Fix statement A" } });
      const b = pile({ finding_id: 2, incident_id: "inc-b", story_path: "SAME_STATEMENT_PILEUP → 0x99887766AABBCCDD", occurrences: 5, advice: { headline: head, investigation: "i", remediation: "Fix statement B" } });
      const root = await run("srv-a", { findings: [a, b] });
      return { cards: all(root, (x) => /\breco-card\b/.test(x.className)).length, text: root.textContent };
    },
    pileupSameStatement: async () => {
      /* #7582: one statement's pileup on two incidents is still one card. */
      const pile = (over) => finding({ root_fact: { key: "SAME_STATEMENT_PILEUP", value: 1 }, story_path: "SAME_STATEMENT_PILEUP → 0x1A2B3C4D5E6F7A8B", advice: { headline: "4 sessions ran one statement for 30 s", investigation: "i", remediation: "r" }, ...over });
      const root = await run("srv-a", { findings: [pile({ finding_id: 1, incident_id: "inc-a", occurrences: 3 }), pile({ finding_id: 2, incident_id: "inc-b", occurrences: 5 })] });
      return { cards: all(root, (x) => /\breco-card\b/.test(x.className)).length, text: root.textContent };
    },
    pileupAlsoSeen: async () => {
      /* #7582, #7696: one incident's pileup read back under two headlines that differ only in the figures is one card at the newest headline, with no hash. */
      const pile = (over) => finding({ incident_id: "inc-a", root_fact: { key: "SAME_STATEMENT_PILEUP", value: 1 }, story_path: "SAME_STATEMENT_PILEUP → 0x1A2B3C4D5E6F7A8B", ...over });
      const newer = pile({ finding_id: 1, last_seen: "2026-01-01T10:00:00Z", advice: { headline: "4 sessions ran one statement for 30 s", investigation: "i", remediation: "r" } });
      const older = pile({ finding_id: 2, last_seen: "2026-01-01T08:00:00Z", advice: { headline: "3 sessions ran one statement for 20 s", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [newer, older] });
      return { cards: all(root, (x) => /\breco-card\b/.test(x.className)).length, text: root.textContent };
    },
    pileupTwoStatementsOneIncident: async () => {
      /* #7696: two pileups on different statements in ONE incident print one headline and stay two cards, each with its own remediation. */
      const head = "4 sessions ran one statement for 30 s";
      const pile = (over) => finding({ incident_id: "inc-a", root_fact: { key: "SAME_STATEMENT_PILEUP", value: 1 }, ...over });
      const a = pile({ finding_id: 1, story_path: "SAME_STATEMENT_PILEUP → 0x1A2B3C4D5E6F7A8B", advice: { headline: head, investigation: "i", remediation: "Fix statement A" } });
      const b = pile({ finding_id: 2, story_path: "SAME_STATEMENT_PILEUP → 0x99887766AABBCCDD", advice: { headline: head, investigation: "i", remediation: "Fix statement B" } });
      const root = await run("srv-a", { findings: [a, b] });
      return { cards: all(root, (x) => /\breco-card\b/.test(x.className)).length, text: root.textContent };
    },
    objectAnomalyOneIncident: async () => {
      /* #7696: two tables (growth) and two objects (contention) under one root fact each, in ONE incident: every one keeps its own card, so no subject is lost. */
      const row = (key, id, headline) => finding({ finding_id: id, incident_id: "inc-a", root_fact: { key, value: 1 }, story_path: key, advice: { headline, investigation: "i", remediation: "r" } });
      const findings = [
        row("ANOMALY_OBJECT_GROWTH", 1, "dbo.Orders grew 900 MB day-over-day"),
        row("ANOMALY_OBJECT_GROWTH", 2, "dbo.Events grew 700 MB day-over-day"),
        row("ANOMALY_OBJECT_CONTENTION", 3, "dbo.Queue accrued 40 s of new lock-wait time"),
        row("ANOMALY_OBJECT_CONTENTION", 4, "dbo.Jobs accrued 30 s of new lock-wait time"),
      ];
      const root = await run("srv-a", { findings });
      const merged = mod.mergeBySentence(findings);
      return { cards: all(root, (x) => /\breco-card\b/.test(x.className)).length, titles: merged.map((m) => m.finding_title), text: root.textContent };
    },
    sameSentence: async () => {
      /* #7582: one sentence on rows of two incidents is one card, Critical, with the occurrences added up, and the chains it covers named in words. */
      const read = (over) => finding({ advice: { headline: "Read latency spiked to 155 ms", investigation: "i", remediation: "r" }, ...over });
      const warn = read({ finding_id: 1, incident_id: "inc-1", severity: 0.9, peak_severity: 0.9, occurrences: 17, story_path: "ANOMALY_READ_LATENCY", last_seen: "2026-01-01T07:00:00Z" });
      const crit = read({ finding_id: 2, incident_id: "inc-2", severity: 1.7, peak_severity: 1.7, occurrences: 4, story_path: "ANOMALY_READ_LATENCY → LCK_M_S", last_seen: "2026-01-01T04:00:00Z" });
      const other = finding({ finding_id: 3, incident_id: "inc-3", severity: 0.2, peak_severity: 0.2, advice: { headline: "Something else", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [warn, crit, other] });
      return { headers: groupsOf(root).map(sum), text: root.textContent };
    },
    sameRootOtherFigure: async () => {
      /* #7696: one root fact on two incidents whose headlines differ only in the figure is one card: Critical, the newest figure, the occurrences added up. */
      const wl = (over) => finding({ root_fact: { key: "ANOMALY_WRITE_LATENCY", value: 1 }, story_path: "ANOMALY_WRITE_LATENCY", ...over });
      const older = wl({ finding_id: 1, incident_id: "inc-1", severity: 1.6, peak_severity: 1.6, occurrences: 25, first_seen: "2026-01-01T01:00:00Z", last_seen: "2026-01-01T08:50:00Z", time_range: { start: "2026-01-01T01:41:00Z", end: "2026-01-01T08:50:00Z" }, advice: { headline: "Write latency spiked to 2300 ms", investigation: "i", remediation: "r" } });
      const newer = wl({ finding_id: 2, incident_id: "inc-2", severity: 1.3, peak_severity: 1.3, occurrences: 5, first_seen: "2026-01-01T00:11:00Z", last_seen: "2026-01-01T09:20:00Z", time_range: { start: "2026-01-01T00:11:00Z", end: "2026-01-01T09:20:00Z" }, advice: { headline: "Write latency spiked to 4935.5 ms", investigation: "i", remediation: "r" } });
      const root = await run("srv-a", { findings: [older, newer] });
      const merged = mod.mergeBySentence([older, newer]);
      return { headers: groupsOf(root).map(sum), text: root.textContent, rows: merged.length, title: merged[0].finding_title, start: merged[0].time_range.start, end: merged[0].time_range.end, label: mod.severityLabel(merged[0].severity) };
    },
    noRootStillMergesByTitle: async () => {
      /* #7696: a row with no root fact keeps the old key, its headline. */
      const row = (over) => finding({ root_fact: null, story_path: "", advice: { headline: "Same words", investigation: "i", remediation: "r" }, ...over });
      const other = row({ finding_id: 3, incident_id: "inc-3", advice: { headline: "Other words", investigation: "i", remediation: "r" } });
      const merged = mod.mergeBySentence([row({ finding_id: 1, incident_id: "inc-1", occurrences: 2 }), row({ finding_id: 2, incident_id: "inc-2", occurrences: 4 }), other]);
      return { rows: merged.length, occurrences: merged.map((m) => m.occurrences) };
    },
    objectAnomalyTwoTables: async () => {
      /* #7696: ANOMALY_OBJECT_GROWTH names its table only in the headline, so two tables stay two cards. */
      const grow = (over) => finding({ root_fact: { key: "ANOMALY_OBJECT_GROWTH", value: 1 }, story_path: "ANOMALY_OBJECT_GROWTH", ...over });
      const merged = mod.mergeBySentence([
        grow({ finding_id: 1, incident_id: "inc-1", advice: { headline: "dbo.Orders grew 900 MB day-over-day", investigation: "i", remediation: "r" } }),
        grow({ finding_id: 2, incident_id: "inc-2", advice: { headline: "dbo.Events grew 700 MB day-over-day", investigation: "i", remediation: "r" } }),
      ]);
      return { titles: merged.map((m) => m.finding_title) };
    },
    peakSeverity: async () => {
      /* #7582: the newest occurrence scored 0.62 but the peak was 1.3, so the card reads Warning. */
      const root = await run("srv-a", { findings: [finding({ finding_id: 1, severity: 0.62, peak_severity: 1.3, incident_id: "inc-1", advice: { headline: "Wait profile shifted", investigation: "i", remediation: "r" } })] });
      return { headers: groupsOf(root).map(sum) };
    },
    nullSeverity: async () => {
      const root = await run("srv-a", { findings: [finding({ finding_id: 1, severity: null })] });
      return { text: root.textContent };
    },
    scoreWording: async () => {
      /* The severity score is labelled as a score and the category follows a dot: "0.80 waits" read as a count of waits. */
      const root = await run("srv-a", { findings: [finding({ finding_id: 1, severity: 0.8, category: "waits", category_name: "Waits" })] });
      return { text: root.textContent };
    },
    categoryNames: async () => {
      /* #7486: the card shows the name the reply carries, and a row with no name shows its category as it is. */
      const root = await run("srv-a", { findings: [
        finding({ finding_id: 1, category: "bad_actor", category_name: "Expensive query", incident_id: "inc-1", advice: { headline: "First sentence" } }),
        finding({ finding_id: 2, category: "weird_new_source", incident_id: "inc-2", advice: { headline: "Second sentence" } }),
      ] });
      return { text: root.textContent };
    },
    truncation: async () => {
      const root = await run("srv-a", { findings: sample, truncated: true, truncation_note: "READ-CAP-NOTE", findings_truncated: true, findings_truncated_note: "PAGE-NOTE" });
      return { text: root.textContent };
    },
    replicaNote: async () => {
      /* #5558: the secondary-copy note shows with a list and with an empty chain list, and is absent when the read carries none. */
      const note = "REPLICA-NOTE";
      const withFindings = await run("srv-a", { findings: sample, secondary_replica_note: note });
      const noFindings = await run("srv-a", { findings: [], secondary_replica_note: note });
      const without = await run("srv-a", { findings: sample });
      const nullNote = await run("srv-a", { findings: sample, secondary_replica_note: null });
      return { withFindings: withFindings.textContent, noFindings: noFindings.textContent, without: without.textContent, nullNote: nullNote.textContent };
    },
    signal: async () => {
      const ac = new AbortController();
      await run("srv-a", null, { hours: 24, label: "x", signal: ac.signal });
      return { passed: signals[0] != null };
    },
    rebuild: async () => {
      let root = await run("srv-a");
      let g = groupsOf(root);
      // Reader closes the Critical group and opens the Info group, then the 60 s poll rebuilds the tab.
      g[0].open = false; g[0].handlers.toggle();
      g[2].open = true; g[2].handlers.toggle();
      root = await run("srv-a");
      const again = groupsOf(root).map((x) => x.open);
      root = await run("srv-b");
      const other = groupsOf(root).map((x) => x.open);
      return { again, other };
    },
    empty: async () => {
      body = JSON.stringify({ status: "empty", message: "No findings in the requested time range." });
      const root = mod.analysisFindingsTab.build("srv-e", { hours: 24, label: "x" });
      await settle();
      return { text: root[0].textContent };
    },
  };
  console.log(JSON.stringify(await scenarios[scenario]()));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

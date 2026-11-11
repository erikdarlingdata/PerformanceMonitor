/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * markdown.js — a minimal, air-gapped, XSS-safe Markdown -> DOM renderer for notebook markdown cells (#1563 D7).
 *
 * SECURITY (the load-bearing property): EVERY node is built through util.el() / textContent / text nodes — there is
 * NO innerHTML anywhere, and nothing is ever assigned as markup. Raw HTML in the input (a <script> tag, an
 * onerror= attribute, a stray angle bracket) is therefore inert BY CONSTRUCTION: it can only ever become the TEXT
 * of a node, never a parsed element. Links are the one attribute sink; ONLY an http / https href becomes an <a>
 * (with rel="noopener noreferrer"), and any other scheme (javascript:, data:, vbscript:, …) or a malformed link
 * renders as its literal source text. No external library, no network, no off-origin literal — the CI
 * self-containment scan forbids a literal scheme+// URL anywhere in wwwroot, so the one scheme test below is
 * written so its regex SOURCE never contains that substring (the two slashes stay non-adjacent as \/\/).
 *
 * Supported (a deliberately small subset — enough to document an investigation, no more): ATX headings
 * (# … ######), paragraphs, **bold** / *italic* (and __ / _), `inline code`, ``` fenced code blocks ```, unordered
 * (- * +) and ordered (1. / 1)) lists, pipe tables (a header row, a |---| separator row, body rows), "> " block quotes, and
 * [text](url) links. Everything else renders as its literal text.
 */

import { el } from "./util.js";

/**
 * Render Markdown source into a DocumentFragment of block nodes (headings / paragraphs / lists / code / …). A
 * fragment (not a wrapper element) so the caller mounts it wherever it likes; a fresh fragment each call, so it is
 * safe to re-render into a live-preview box. Never throws on odd input — anything unrecognized becomes text.
 */
export function renderMarkdown(text) {
  const frag = document.createDocumentFragment();
  for (const node of renderBlocks(normalize(text).split("\n"))) {
    frag.appendChild(node);
  }
  return frag;
}

/** The block nodes for an array of source lines. A block quote renders its own lines through this again. */
function renderBlocks(lines) {
  const out = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    /* Blank line: a block separator — skip it. */
    if (line.trim() === "") {
      i++;
      continue;
    }

    /* Fenced code block: ``` or ~~~ (>=3), closed by a fence of the same char (>= the opening length). The body is
       verbatim text (no inline parsing), so anything inside — including markup — is literal by definition. */
    const fence = fenceOpen(line);
    if (fence) {
      const fenceChar = fence[1][0];
      const fenceLen = fence[1].length;
      const body = [];
      i++;
      while (i < lines.length) {
        const close = lines[i].match(/^\s*(`{3,}|~{3,})\s*$/);
        if (close && close[1][0] === fenceChar && close[1].length >= fenceLen) {
          i++;
          break;
        }
        body.push(lines[i]);
        i++;
      }
      /* An info string of "text" marks prose that must still show as typed (the alert notebook's finding story): the
         block is still verbatim, but it wraps and uses the body font instead of scrolling sideways in monospace. */
      const isProse = fence[2].trim() === "text";
      out.push(el("pre", { class: isProse ? "md-pre md-pre-text" : "md-pre" }, [el("code", { text: body.join("\n") })]));
      continue;
    }

    /* ATX heading: 1–6 leading #, a required space, optional trailing #'s stripped. */
    const heading = line.match(/^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$/);
    if (heading) {
      out.push(el("h" + heading[1].length, { class: "md-h" }, parseInline(heading[2])));
      i++;
      continue;
    }

    /* Pipe table: a header row, then a separator row ("|---|:---:|") with the same number of cells, then body rows. Every cell
       is parsed as inline text, so a cell holds no markup beyond what a paragraph holds. */
    if (isTableStart(lines, i)) {
      const header = splitTableRow(line);
      const aligns = splitTableRow(lines[i + 1]).map(cellAlign);
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].trim() !== "" && lines[i].includes("|")) {
        rows.push(splitTableRow(lines[i]));
        i++;
      }
      const cell = (tag, text, c) => el(tag, { class: aligns[c] ? "md-al-" + aligns[c] : "" }, parseInline(text));
      const head = el("thead", {}, [el("tr", {}, header.map((h, c) => cell("th", h, c)))]);
      const body = el(
        "tbody",
        {},
        rows.map((r) => el("tr", {}, header.map((_, c) => cell("td", r[c] ?? "", c))))
      );
      out.push(el("div", { class: "md-table-wrap" }, [el("table", { class: "md-table" }, [head, body])]));
      continue;
    }

    /* Block quote: consecutive lines that start with ">" (an optional space after it). The lines with the marker removed are
       rendered as blocks of their own, so a quote may hold a list, a code fence or another quote. */
    if (QUOTE_LINE.test(line)) {
      const inner = [];
      while (i < lines.length && QUOTE_LINE.test(lines[i])) {
        inner.push(lines[i].replace(QUOTE_LINE, ""));
        i++;
      }
      out.push(el("blockquote", { class: "md-quote" }, renderBlocks(inner)));
      continue;
    }

    /* Unordered list: consecutive - / * / + item lines. */
    if (/^\s*[-*+]\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*[-*+]\s+/.test(lines[i])) {
        items.push(el("li", {}, parseInline(lines[i].replace(/^\s*[-*+]\s+/, ""))));
        i++;
      }
      out.push(el("ul", { class: "md-ul" }, items));
      continue;
    }

    /* Ordered list: consecutive "N." / "N)" item lines. */
    if (/^\s*\d+[.)]\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*\d+[.)]\s+/.test(lines[i])) {
        items.push(el("li", {}, parseInline(lines[i].replace(/^\s*\d+[.)]\s+/, ""))));
        i++;
      }
      out.push(el("ol", { class: "md-ol" }, items));
      continue;
    }

    /* Paragraph: gather consecutive non-blank lines until the next block start; soft-wrap joins with a space. */
    const para = [];
    while (i < lines.length && lines[i].trim() !== "" && !isBlockStart(lines[i]) && !isTableStart(lines, i)) {
      para.push(lines[i].trim());
      i++;
    }
    out.push(el("p", { class: "md-p" }, parseInline(para.join(" "))));
  }

  return out;
}

/** Normalize source: CRLF/CR -> LF, tabs -> spaces, null-safe to "". */
function normalize(text) {
  return String(text == null ? "" : text).replace(/\r\n?/g, "\n").replace(/\t/g, "    ");
}

/**
 * The fence a line opens, as [whole, fence, info string], or null. #7944: a backtick fence's info string holds no backtick
 * (CommonMark), so a line that opens and closes inline code with three backticks ("```code``` and more") is a paragraph line, not a
 * fence that runs to the end of the cell. A tilde fence's info string may hold anything.
 */
function fenceOpen(line) {
  return line.match(/^\s*(`{3,})([^`]*)$/) || line.match(/^\s*(~{3,})(.*)$/);
}

/** Whether a line opens a non-paragraph block (so a paragraph run stops before it). Mirrors the block detectors. */
function isBlockStart(line) {
  return (
    fenceOpen(line) !== null ||
    /^\s{0,3}#{1,6}\s+/.test(line) ||
    /^\s*[-*+]\s+/.test(line) ||
    /^\s*\d+[.)]\s+/.test(line) ||
    QUOTE_LINE.test(line)
  );
}

/** A block quote line: up to three spaces, ">", and one optional space (removed by the replace in the quote block). */
const QUOTE_LINE = /^\s{0,3}>[ ]?/;

/** A separator row cell: dashes with an optional colon at either end. */
const TABLE_SEPARATOR_CELL = /^:?-+:?$/;

/**
 * The cells of a table row: a leading and a trailing pipe are dropped, "\|" is a literal pipe, and each cell is trimmed. A row
 * with no pipe at all is one cell.
 */
function splitTableRow(line) {
  let row = line.trim();
  if (row.startsWith("|")) row = row.slice(1);
  if (row.endsWith("|") && !row.endsWith("\\|")) row = row.slice(0, -1);
  const cells = [];
  let buf = "";
  for (let j = 0; j < row.length; j++) {
    if (row[j] === "\\" && row[j + 1] === "|") {
      buf += "|";
      j++;
    } else if (row[j] === "|") {
      cells.push(buf.trim());
      buf = "";
    } else {
      buf += row[j];
    }
  }
  cells.push(buf.trim());
  return cells;
}

/** "right" for "---:", "center" for ":---:", "" otherwise: a separator cell's alignment. */
function cellAlign(cell) {
  const right = cell.endsWith(":");
  const left = cell.startsWith(":");
  return right && left ? "center" : right ? "right" : "";
}

/** Whether line `at` opens a table: it holds a pipe, and the next line is a separator row with the same number of cells. */
function isTableStart(lines, at) {
  if (at + 1 >= lines.length || !lines[at].includes("|") || !lines[at + 1].includes("|")) {
    return false;
  }
  const sep = splitTableRow(lines[at + 1]);
  return sep.every((c) => TABLE_SEPARATOR_CELL.test(c)) && sep.length === splitTableRow(lines[at]).length;
}

/* ─────────────────────────── inline parsing (the XSS-critical path) ─────────────────────────── */

/** Parse inline markdown into an array of DOM nodes (text nodes + <strong>/<em>/<code>/<a>). */
function parseInline(text) {
  return parseInlineImpl(String(text == null ? "" : text), true);
}

/**
 * The inline tokenizer: a single left-to-right pass. Precedence: inline code (backtick) first (its content is
 * literal, so a * or [ inside a code span never reaches the emphasis/link handlers), then links, then bold, then
 * italic. `allowLinks` is false inside a link label so links never nest. Every branch that fails to find its close
 * falls back to emitting the marker as literal text — the parser never throws and never drops input.
 */
function parseInlineImpl(text, allowLinks) {
  const nodes = [];
  let buf = "";
  const flush = () => {
    if (buf) {
      nodes.push(document.createTextNode(buf));
      buf = "";
    }
  };

  let i = 0;
  const n = text.length;
  while (i < n) {
    const c = text[i];

    /* `inline code` — verbatim content, no nested formatting. #7944: a run of backticks opens a span that the next run of exactly
       the same length closes (CommonMark), so "```code```" is one span and not three; a run with no match is literal text. */
    if (c === "`") {
      const open = backtickRun(text, i);
      const close = closingRun(text, i + open, open);
      if (close > i) {
        flush();
        nodes.push(el("code", { class: "md-code", text: text.slice(i + open, close) }));
        i = close + open;
        continue;
      }
      buf += text.slice(i, i + open);
      i += open;
      continue;
    }

    /* [label](url) — only an http / https url becomes an <a>; anything else renders as its literal source. */
    if (c === "[" && allowLinks) {
      const link = matchLink(text, i);
      if (link) {
        flush();
        const href = safeUrl(link.url);
        if (href) {
          nodes.push(
            el("a", { class: "md-link", href, rel: "noopener noreferrer", target: "_blank" }, parseInlineImpl(link.label, false))
          );
        } else {
          nodes.push(document.createTextNode(link.raw));
        }
        i = link.end;
        continue;
      }
      buf += c;
      i++;
      continue;
    }

    /* An underscore inside a word (SOS_SCHEDULER_YIELD, sys_dm_exec_requests) is literal, as in CommonMark (#7172). */
    if (c === "_" && isWordChar(text[i - 1])) {
      buf += c;
      i++;
      continue;
    }

    /* **bold** / __bold__ */
    if ((c === "*" || c === "_") && text[i + 1] === c) {
      const close = indexOfDouble(text, c, i + 2);
      if (close > i + 1) {
        flush();
        nodes.push(el("strong", {}, parseInlineImpl(text.slice(i + 2, close), allowLinks)));
        i = close + 2;
        continue;
      }
      buf += c;
      i++;
      continue;
    }

    /* *italic* / _italic_ */
    if (c === "*" || c === "_") {
      const close = indexOfSingle(text, c, i + 1);
      if (close > i) {
        flush();
        nodes.push(el("em", {}, parseInlineImpl(text.slice(i + 1, close), allowLinks)));
        i = close + 1;
        continue;
      }
      buf += c;
      i++;
      continue;
    }

    buf += c;
    i++;
  }

  flush();
  return nodes;
}

/** The length of the run of backticks starting at `at`. */
function backtickRun(text, at) {
  let j = at;
  while (text[j] === "`") j++;
  return j - at;
}

/** The start of the next run of exactly `len` backticks at or after `from`, or -1. A longer or shorter run is skipped whole. */
function closingRun(text, from, len) {
  for (let j = from; j < text.length; j++) {
    if (text[j] !== "`") continue;
    const run = backtickRun(text, j);
    if (run === len) return j;
    j += run - 1;
  }
  return -1;
}

/** True for a letter or digit: an underscore touching one on the wrong side is part of a word, not emphasis (#7172). */
function isWordChar(ch) {
  return ch !== undefined && /[\p{L}\p{N}]/u.test(ch);
}

/** The next LONE emphasis marker `c` at or after `from` (one whose next char isn't also `c`, so it's not a `**`). An `_` closes only when no letter or digit follows it. */
function indexOfSingle(text, c, from) {
  for (let j = from; j < text.length; j++) {
    if (text[j] === c && text[j + 1] !== c && !(c === "_" && isWordChar(text[j + 1]))) {
      return j;
    }
  }
  return -1;
}

/** The next doubled marker `cc` at or after `from`. A `__` closes only when no letter or digit follows it. */
function indexOfDouble(text, c, from) {
  for (let j = text.indexOf(c + c, from); j >= 0; j = text.indexOf(c + c, j + 1)) {
    if (!(c === "_" && isWordChar(text[j + 2]))) {
      return j;
    }
  }
  return -1;
}

/**
 * Match a link starting at `start` (a '['): the bracket-balanced label, a required '(' immediately after ']', and
 * the URL up to the first ')'. Returns {label, url, raw, end} or null when the shape doesn't hold (the caller then
 * treats '[' as literal). Kept intentionally simple — a ')' inside a URL truncates it, which safeUrl then rejects.
 */
function matchLink(text, start) {
  let depth = 0;
  let labelEnd = -1;
  for (let j = start; j < text.length; j++) {
    const ch = text[j];
    if (ch === "[") {
      depth++;
    } else if (ch === "]") {
      depth--;
      if (depth === 0) {
        labelEnd = j;
        break;
      }
    }
  }
  if (labelEnd < 0 || text[labelEnd + 1] !== "(") {
    return null;
  }
  const urlStart = labelEnd + 2;
  const urlEnd = text.indexOf(")", urlStart);
  if (urlEnd < 0) {
    return null;
  }
  return {
    label: text.slice(start + 1, labelEnd),
    url: text.slice(urlStart, urlEnd),
    raw: text.slice(start, urlEnd + 1),
    end: urlEnd + 1,
  };
}

/**
 * The ONLY safe href: an absolute http / https URL with no whitespace / quotes / angle brackets. Any other scheme
 * (javascript:, data:, vbscript:, mailto:, …), a protocol-relative //host, or a relative path returns null and the
 * link renders as literal text. NOTE the regex source deliberately keeps the two slashes non-adjacent (\/\/) and
 * uses `https?` so the file never contains a literal scheme+slashslash substring — the air-gap CI scan forbids one.
 */
function safeUrl(url) {
  const u = String(url == null ? "" : url).trim();
  if (!u || /[\s"'<>]/.test(u)) {
    return null;
  }
  return /^https?:\/\//i.test(u) ? u : null;
}

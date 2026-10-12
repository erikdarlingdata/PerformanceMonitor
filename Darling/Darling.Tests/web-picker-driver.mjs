/* Drives the shared time range picker (wwwroot/js/time-range-picker.js) inside the page harnesses' node-tree DOM (#5562).
   A harness's FakeNode keeps one handler per event type and sets `className` through el(). The helpers here open the popup, type
   a range into its text box and press Enter, exactly as a reader does, so a page test exercises the real control and not a
   stand-in. Import it from a harness; it does not copy the js tree and reads no page file itself. */

/** Raises an event on a harness node, whichever FakeNode shape it is (`handlers[type]` or `listeners[type]` with `fire`). */
export function raise(node, type, event = {}) {
  if (node.handlers && node.handlers[type]) return node.handlers[type](event);
  if (typeof node.fire === "function") return node.fire(type, event);
  throw new Error("no " + type + " handler on " + node.tag);
}

export function allNodes(node, out = []) {
  out.push(node);
  for (const c of node.children || []) allNodes(c, out);
  return out;
}

/** The picker's button, found by the accessible name the page gave it ("Window", "Time range", ...). */
export function pickerButton(root, label) {
  return allNodes(root).find(
    (n) => n.tag === "button" && String(n.className || "").includes("trp-button") && String(n.attrs["aria-label"] || "").startsWith(label + ":")
  );
}

/** The button's text: the range name the picker shows ("Past 1 day", "Custom (45m)"). */
export function pickerText(root, label) {
  const b = pickerButton(root, label);
  return b ? b.textContent : null;
}

/** The popup's items by their name, with whether each is greyed out and the reason it gives. Opens the popup. */
export function pickerItems(root, label) {
  const b = pickerButton(root, label);
  raise(b, "click");
  const items = allNodes(root)
    .filter((n) => n.tag === "button" && String(n.className || "").includes("trp-item"))
    .map((n) => ({
      name: n.children[0].textContent,
      disabled: String(n.className).includes("trp-disabled"),
      why: (n.children.find((c) => String(c.className || "").includes("trp-item-why")) || { textContent: null }).textContent,
    }));
  raise(b, "click");
  return items;
}

/** Whether the popup carries the calendar periods and the start-and-end boxes. */
export function popupShape(root, label) {
  const b = pickerButton(root, label);
  raise(b, "click");
  const nodes = allNodes(root);
  const headings = nodes.filter((n) => String(n.className || "").includes("trp-heading")).map((n) => n.textContent);
  const boxes = nodes.filter((n) => n.tag === "input" && n.attrs.type === "datetime-local").length;
  raise(b, "click");
  return { headings, dateBoxes: boxes };
}

/** Types `text` into the popup's box and presses Enter. Returns the preview line the picker showed before Enter. */
export function pickRange(root, label, text) {
  const b = pickerButton(root, label);
  raise(b, "click");
  const box = allNodes(root).find((n) => n.tag === "input" && n.attrs["aria-label"] === "Type a time range");
  box.value = text;
  raise(box, "input");
  const preview = allNodes(root).find((n) => String(n.className || "").includes("trp-preview"));
  const shown = preview ? preview.textContent : "";
  raise(box, "keydown", { key: "Enter", preventDefault() {} });
  /* A refused range leaves the popup open; close it so the next pick starts from the closed state. */
  if (b.attrs["aria-expanded"] === "true") raise(b, "click");
  return shown;
}

/** The document stub every harness needs once the picker's popup opens (it listens for an outside click only while open). */
export function withDocumentListeners(doc) {
  doc.addEventListener = doc.addEventListener || (() => {});
  doc.removeEventListener = doc.removeEventListener || (() => {});
  return doc;
}

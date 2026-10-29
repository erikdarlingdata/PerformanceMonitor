/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The front door's store picker (#6840). A front door fronts several Darling stores, and the page works inside one of
 * them at a time: util.js holds the picked store and sends it as X-Darling-Store on every request, and the hash carries
 * it as #/s/{store}/ so a link or a bookmark opens in the same store. This file loads the store list from
 * GET /api/front-door, picks the first store to work in, draws the picker and asks GET /api/front-door/locate which store
 * a bare #/server/<name> link belongs to. Nothing here runs without a front door: app.js calls initFrontDoor only when
 * /api/session said front_door: true.
 *
 * Every store name and server name reaches the page through el() (text and attributes, never markup).
 */

import { el, mount, apiGet, currentStore, setStore, setFrontDoor, frontDoor, storeHash, splitStoreHash } from "./util.js";
import { setStoreScope } from "./viewer-local.js";
import { peekSession } from "./views-api.js";

const REMEMBER_KEY = "darling.frontDoor.store";

/* [{ name, health, server_count, last_ok_utc, admin_writes_off, managed_store }] from the last /api/front-door read. */
let stores = [];
let pickerHost = null;

/** The stores the front door knows, as last read. */
export function frontDoorStores() {
  return stores;
}

export function isKnownStore(name) {
  return stores.some((s) => s.name === name);
}

/** The wording for a store's health word. An unknown word is said as it came. */
export function healthLabel(health) {
  if (health === "ok") return "ok";
  if (health === "stale") return "stale";
  if (health === "unreachable") return "unreachable";
  if (health === "auth_failed") return "sign-in failed";
  return String(health == null ? "" : health);
}

/** The band class a health word dots with (the same dots the sidebar's servers use). */
export function healthBandClass(health) {
  if (health === "ok") return "band-Healthy";
  if (health === "stale") return "band-Warning";
  return "band-Critical";
}

/**
 * The stores (or any list of things that each belong to one store) with the picked store first and the rest in their existing
 * order (#6917). The Fleet page and the sidebar both list stores in the front door's order and both call this, so the picked
 * store leads in both. `nameOf` reads an item's store name. Returns a new array and leaves `items` alone.
 */
export function currentFirst(items, nameOf, current) {
  return [...items.filter((x) => nameOf(x) === current), ...items.filter((x) => nameOf(x) !== current)];
}

/** The small muted word that follows the picked store's name on its Fleet section and sidebar header (#6917). */
export function currentMark() {
  return el("span", { class: "store-current-mark", text: "current" });
}

/** The store this browser used last, or null (nothing stored, or storage is not available). */
function remembered() {
  try {
    return localStorage.getItem(REMEMBER_KEY);
  } catch {
    return null;
  }
}

/** Remembers `name` as the store to start in next time. The one place that key is written, and a storage that refuses the write
 *  (private mode) only means the choice lasts until the page closes. */
export function rememberStore(name) {
  try {
    localStorage.setItem(REMEMBER_KEY, name);
  } catch {
    /* private mode: the choice lasts until the page closes */
  }
}

/** Reads GET /api/front-door into the store list. Returns true when the front door answered with a list. */
export async function refreshStores() {
  const res = await apiGet("/api/front-door");
  if (res.kind !== "data" || !res.data || !Array.isArray(res.data.stores)) return false;
  stores = res.data.stores
    .filter((s) => s && typeof s.name === "string" && s.name !== "")
    .map((s) => ({
      name: s.name,
      health: typeof s.health === "string" ? s.health : "ok",
      server_count: Number.isFinite(s.server_count) ? s.server_count : null,
      last_ok_utc: typeof s.last_ok_utc === "string" ? s.last_ok_utc : null,
      admin_writes_off: s.admin_writes_off === true,
      managed_store: s.managed_store === true,
    }));
  applyStoreSession(peekSession());
  return true;
}

/**
 * The front door has no admin_writes_off or managed_store of its own: each store reports them in the store list. The page draws
 * Admin write controls and the notes that name the switch from the session's flags, so it takes the flags of the store in use, which
 * is what the same page reads from the store's own /api/session when it is not behind a front door. Does nothing outside a front door.
 */
export function applyStoreSession(session) {
  if (!session || !frontDoor()) return;
  const mine = stores.find((s) => s.name === currentStore());
  if (!mine) return;
  session.admin_writes_off = mine.admin_writes_off === true;
  session.managed_store = mine.managed_store === true;
}

/**
 * Turns the front door on and picks the store to start in: the one the address names, else the one this browser used
 * last, else the first listed. `hash` is location.hash at start. `host` is the element the picker is drawn into.
 */
export async function initFrontDoor(host, hash) {
  setFrontDoor(true);
  pickerHost = host || null;
  await refreshStores();
  const named = splitStoreHash(hash).store;
  const last = remembered();
  const pick = [named, last].find((n) => n && isKnownStore(n)) || (stores.length ? stores[0].name : null);
  setStore(pick);
  setStoreScope(pick);
  if (pick) rememberStore(pick);
  applyStoreSession(peekSession());
  paintPicker();
}

/** Polls the store list (health changes) and repaints the picker. */
export async function refreshPicker() {
  if (await refreshStores()) paintPicker();
}

/** Makes `name` the store to work in from now on, and remembers it for the next visit. */
export function chooseStore(name) {
  setStore(name);
  setStoreScope(name);
  rememberStore(name);
}

function paintPicker() {
  if (!pickerHost) return;
  if (!stores.length) {
    mount(pickerHost, []);
    pickerHost.hidden = true;
    return;
  }
  pickerHost.hidden = false;
  const select = el(
    "select",
    { id: "store-picker-select", class: "store-picker-select", "aria-label": "Store", "aria-describedby": "store-picker-note" },
    stores.map((s) => {
      const opt = el("option", { value: s.name, text: s.health === "ok" ? s.name : s.name + " (" + healthLabel(s.health) + ")" });
      if (s.name === currentStore()) opt.selected = true;
      return opt;
    })
  );
  if (currentStore()) select.value = currentStore();
  select.addEventListener("change", () => switchStore(select.value));
  mount(pickerHost, [
    el("label", { class: "store-picker-label", for: "store-picker-select", text: "Store" }),
    select,
    el("div", { id: "store-picker-note", class: "store-picker-note", text: "Alerts, views and settings come from this store." }),
  ]);
}

/** Picks another store and reloads the page on the same route inside it. A full reload, because the views list, the alert
    counts and the other things the shell holds for the page all belong to the store that was left. */
export function switchStore(name) {
  if (!name || name === currentStore()) return;
  const route = splitStoreHash(location.hash).hash || "#/fleet";
  chooseStore(name);
  location.hash = storeHash(route, name);
  location.reload();
}

/**
 * Asks which store holds a server. Answers one of:
 *   { kind: "found", store, server }            200: exactly one store has it
 *   { kind: "ambiguous", message, stores }      409: several stores have it
 *   { kind: "notfound", message }               404: none does
 *   { kind: "error", message }                  anything else, a transport failure included
 * Raw fetch rather than apiGet: apiGet drops the body of a 409 and the list of stores is in it.
 */
export async function locateServer(name) {
  let resp;
  try {
    resp = await fetch("/api/front-door/locate?server=" + encodeURIComponent(name), { headers: { Accept: "application/json" } });
  } catch (e) {
    return { kind: "error", message: "Network error: " + (e && e.message ? e.message : String(e)) };
  }
  let body = null;
  try {
    body = JSON.parse(await resp.text());
  } catch {
    body = null;
  }
  const message = body && typeof body.message === "string" ? body.message : "";
  if (resp.status === 200 && body && typeof body.store === "string" && body.store !== "") {
    return { kind: "found", store: body.store, server: typeof body.server_name === "string" ? body.server_name : name };
  }
  if (resp.status === 409) {
    return { kind: "ambiguous", message: message || "More than one store has that server.", stores: Array.isArray(body && body.stores) ? body.stores.map(String) : [] };
  }
  if (resp.status === 404) return { kind: "notfound", message: message || "No store has that server." };
  return { kind: "error", message: message || "Could not look the server up (HTTP " + resp.status + ")." };
}

/** The page's answer to an ambiguous server name: the front door's sentence, then one link per store that opens the same
    route in that store. `hash` is the route without a store (#/server/x/tab). */
export function ambiguousStrip(answer, hash) {
  return el("div", { class: "strip error store-ambiguous", role: "alert" }, [
    el("div", { text: answer.message }),
    el("div", { class: "store-ambiguous-choices" }, [
      el("span", { text: "Open it in: " }),
      ...answer.stores.map((s) => el("a", { class: "btn small", href: storeHash(hash, s), text: s })),
    ]),
  ]);
}

/** The page's answer to a server no store has, or a lookup that failed. */
export function locateFailureStrip(answer) {
  return el("div", { class: "strip error", role: "alert", text: answer.message });
}

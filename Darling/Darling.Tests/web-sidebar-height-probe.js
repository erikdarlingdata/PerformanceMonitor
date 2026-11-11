/* #8631: runs inside the shipped index.html shell (its scripts removed), in a real browser. It fills the sidebar with what the app
   paints (sixteen page links, forty servers, a few views, the settings disclosure), then reports which entries sit fully inside the
   viewport without any scrolling. The page sits in an iframe of the exact size under test (a headless window's inner size is not its
   --window-size), so the result goes to the wrapper page by postMessage, which writes it into <pre id="pm-probe-result">. */
(function () {
  function add(parent, tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text) n.textContent = text;
    parent.appendChild(n);
    return n;
  }

  // The app shows the Availability Groups link once the store has group data: with it every one of the sixteen entries is drawn.
  var ag = document.querySelector('#nav-links a[data-route="ag"]');
  if (ag) ag.hidden = false;
  document.getElementById("auto-refresh-toggle").textContent = "Pause auto-refresh";

  var search = document.getElementById("server-search");
  var input = add(search, "input", "search-input sidebar-search-input");
  input.type = "search";
  input.placeholder = "Search servers";
  var label = add(search, "label", "group-control sidebar-group-control");
  add(label, "input").type = "checkbox";
  add(label, "span", "", "Group by tag");

  var list = document.getElementById("server-list");
  for (var i = 1; i <= 40; i++) {
    var row = add(list, "div", "server-item");
    row.dataset.probe = "server";
    add(row, "span", "dot band-Healthy");
    add(row, "span", "name", "server-" + i);
    add(row, "button", "fav-star", "*").type = "button";
  }

  var views = document.getElementById("view-list");
  for (var v = 1; v <= 4; v++) add(views, "a", "view-item", "View " + v).href = "#/view/" + v;
  add(views, "a", "view-item new-view", "+ New view").href = "#/view/new";
  add(views, "a", "view-item new-view", "+ New notebook").href = "#/notebook/new";

  var details = add(document.getElementById("viewer-settings"), "details", "viewer-settings");
  add(details, "summary", "", "Alert severity colors");

  var sidebar = document.getElementById("sidebar");

  // Fully inside the viewport and inside every ancestor that clips (the sidebar and the inner scroll regions).
  function visible(el) {
    var r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) return false;
    var top = 0, bottom = window.innerHeight;
    for (var p = el.parentElement; p && p !== document.documentElement; p = p.parentElement) {
      if (getComputedStyle(p).overflowY !== "visible") {
        var pr = p.getBoundingClientRect();
        top = Math.max(top, pr.top);
        bottom = Math.min(bottom, pr.bottom);
      }
    }
    return r.top >= top - 0.5 && r.bottom <= bottom + 0.5;
  }

  function report() {
    var links = Array.prototype.filter.call(document.querySelectorAll("#nav-links a"), function (a) { return !a.hidden; });
    var out = {
      size: location.search.slice(1),
      innerWidth: window.innerWidth,
      innerHeight: window.innerHeight,
      pageLinks: links.length,
      pageLinksVisible: links.filter(visible).length,
      alertHistoryVisible: visible(document.querySelector('#nav-links a[data-route="alerts"]')),
      searchVisible: visible(input),
      serversVisible: Array.prototype.filter.call(document.querySelectorAll('[data-probe="server"]'), visible).length,
      viewsHeadingVisible: visible(document.querySelectorAll(".server-list-header")[1]),
      viewListHeight: Math.round(views.getBoundingClientRect().height),
      navHeight: Math.round(document.getElementById("nav-links").getBoundingClientRect().height),
      serverListHeight: Math.round(list.getBoundingClientRect().height),
      sidebarOverflows: sidebar.scrollHeight > sidebar.clientHeight + 1
    };
    window.parent.postMessage(JSON.stringify(out), "*");
  }

  report();
})();

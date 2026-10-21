/* Runs the re-navigation page's inline script (#6286) against stand-ins for the browser and prints what it did as one
   line of JSON. ReNavigationScriptBehaviourTests starts it as: node renavigation-script-harness.mjs <script text> <scenario>
   The script text is DarlingWebHostService.ReNavigationScriptBlock without its <script> tags. */
import vm from "node:vm";

const [script, scenario] = process.argv.slice(-2);

const run = (performanceStub) => {
  const calls = { reload: 0, message: null };
  const context = vm.createContext({
    document: { getElementById: (id) => (id === "m" ? { set textContent(v) { calls.message = v; } } : null) },
    location: { reload: () => { calls.reload++; } },
  });
  if (performanceStub !== undefined) context.performance = performanceStub;
  vm.runInContext(script, context);
  return calls;
};

const entry = (type) => ({ getEntriesByType: (k) => (k === "navigation" ? [{ type }] : []) });
const scenarios = {
  navigate: () => run(entry("navigate")),
  reload: () => run(entry("reload")),
  backForward: () => run(entry("back_forward")),
  prerender: () => run(entry("prerender")),
  noEntry: () => run({ getEntriesByType: () => [] }),
  noPerformance: () => run(undefined),
  throwing: () => run({ getEntriesByType: () => { throw new Error("blocked"); } }),
};
if (!(scenario in scenarios)) throw new Error("unknown scenario " + scenario);
console.log(JSON.stringify(scenarios[scenario]()));

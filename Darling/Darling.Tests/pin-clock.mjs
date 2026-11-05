/* Pins the page harnesses' clock for the calendar-edge sweep (#7062). Load it into every harness with
   NODE_OPTIONS="--import file:///<repo>/Darling/Darling.Tests/pin-clock.mjs" and PM_TEST_NOW=2026-11-01T00:20:00Z.
   The pinned instant is where the clock starts: it keeps running from there, so waits and debounces still finish.
   Date.now() and new Date() with no argument follow the pinned clock. new Date(x), Date.UTC and Date.parse are
   untouched. With PM_TEST_NOW unset this file does nothing. */
const pin = process.env.PM_TEST_NOW;
if (pin) {
  const start = Date.parse(pin);
  if (Number.isNaN(start)) throw new Error(`PM_TEST_NOW is not a date: ${pin}`);
  const realNow = Date.now.bind(Date);
  const offset = start - realNow();
  const shifted = () => realNow() + offset;
  const PinnedDate = new Proxy(Date, {
    construct(target, args, newTarget) {
      return Reflect.construct(target, args.length === 0 ? [shifted()] : args, newTarget === PinnedDate ? target : newTarget);
    },
  });
  Date.now = shifted;
  globalThis.Date = PinnedDate;
}

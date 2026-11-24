#!/usr/bin/env python3
"""Checks for ci-select.py's pinned-classes selection (#8753). Run: python -I .github/scripts/tests/ci-select-pinned-test.py"""
import importlib.util
import os
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("cs", os.path.join(here, "..", "ci-select.py"))
cs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cs)

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


LIVE = '''using Xunit;
public class {name}
{{
    [Fact]
    public void Reads() {{ var c = System.Environment.GetEnvironmentVariable("DARLING_TEST_PG"); }}
}}
{extra}
'''
UNIT = '''using Xunit;
public class {name}
{{
    [Fact]
    public void Adds() {{ Assert.Equal(2, 1 + 1); }}
}}
'''

with tempfile.TemporaryDirectory() as root:
    tests = os.path.join(root, "Darling", "Darling.Tests")
    os.makedirs(tests)
    os.makedirs(os.path.join(root, "Lite.Tests"))
    files = {
        "AlphaLiveTests.cs": LIVE.format(name="AlphaLiveTests", extra="public class AlphaFixtureRow { public int Id; }\n"
                                                                       "public class AlphaSecondLiveTests\n{\n    [Fact]\n    public void Two() { var s = \"DARLING_TEST_PG\"; }\n}"),
        "BetaLiveTests.cs": LIVE.format(name="BetaLiveTests", extra=""),
        "ClockOnlyTests.cs": UNIT.format(name="ClockOnlyTests").replace("Assert.Equal", "var t = LiveClock.Now(); Assert.Equal"),
        "PlainUnitTests.cs": UNIT.format(name="PlainUnitTests"),
        "PrivateNestedTests.cs": LIVE.format(name="PrivateNestedTests", extra="").replace(
            "public class PrivateNestedTests", "internal static class Holder\n{\n    private class Nested\n    {\n        [Fact]\n        public void X() { }\n    }\n}\npublic class PrivateNestedTests"),
    }
    for name, text in files.items():
        with open(os.path.join(tests, name), "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)

    def pick(*changed, limit=cs.PINNED_CLASS_LIMIT):
        return cs.pinned_classes(root, [f"Darling/Darling.Tests/{c}" if c.endswith(".cs") and "/" not in c else c for c in changed], limit)

    names, notes = pick("AlphaLiveTests.cs")
    check("every class of a changed live file runs, the helper row does not", names, ["AlphaLiveTests", "AlphaSecondLiveTests"])
    check("no note when every class fits", notes, [])

    names, _ = pick("AlphaLiveTests.cs", "BetaLiveTests.cs")
    check("two live files", names, ["AlphaLiveTests", "AlphaSecondLiveTests", "BetaLiveTests"])

    names, notes = pick("PlainUnitTests.cs")
    check("a file that is not live picks nothing", names, [])
    check("and says why", len(notes), 1)

    names, _ = pick("ClockOnlyTests.cs")
    check("a file that reads LiveClock is live", names, ["ClockOnlyTests"])

    names, _ = pick("PrivateNestedTests.cs")
    check("a private nested class is not a class the host lists", names, ["PrivateNestedTests"])

    names, notes = pick("Gone.cs")
    check("a deleted file picks nothing", (names, len(notes)), ([], 1))

    names, notes = pick("Lite/Services/Foo.cs", "README.md")
    check("a diff with no Darling test file picks nothing", (names, notes), ([], ["the pull request adds or changes no Darling.Tests file"]))

    names, notes = pick("AlphaLiveTests.cs", "BetaLiveTests.cs", limit=2)
    check("over the limit the first by name run", names, ["AlphaLiveTests", "AlphaSecondLiveTests"])
    check("and a note counts the rest", len(notes), 1)

    out = os.path.join(root, "out.txt")
    listing = os.path.join(root, "files.txt")
    with open(listing, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("Darling/Darling.Tests/BetaLiveTests.cs\n")
    rc = cs.main(["pinned-classes", "--files-from", listing, "--github-output", out, "--root", root])
    with open(out, encoding="utf-8") as fh:
        check("the output lines", (rc, fh.read()), (0, "pinned=true\npinned_classes=BetaLiveTests\n"))
    with open(listing, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("Lite/Services/Foo.cs\n")
    os.remove(out)
    rc = cs.main(["pinned-classes", "--files-from", listing, "--github-output", out, "--root", root])
    with open(out, encoding="utf-8") as fh:
        check("no file, no pass", (rc, fh.read()), (0, "pinned=false\npinned_classes=\n"))

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-select pinned-classes: all checks passed")

# Darling docs on erikdarling.com

Audience: the person who cuts a release.

`publish_darling_docs.py` copies the Darling operator pages in `docs/darling/` to erikdarling.com. It makes one WordPress page for `docs/darling/README.md` and one child page for each page that the README lists. The pages are generated from the repo, so the site and the repo cannot drift. The script uses only the Python standard library. It needs `git` and the GitHub CLI (`gh`, signed in).

## What the script does

1. Reads `docs/darling/README.md` and every page it links to, at the git ref that you give. It reads with `git show`, so your working tree does not matter.
2. Renders each page with GitHub's own renderer (`gh api markdown`). Tables, code blocks and headings look as they do on GitHub.
3. Fixes the links:
   - A link to another page in `docs/darling` goes to that page on the site, with its `#anchor` if it has one. Each heading on the site has the same id that GitHub uses, so these anchors work.
   - A link to any other file in the repo goes to that file on GitHub, at the same ref.
   - The script prints a `WARNING` for a link whose `#anchor` is not a heading on the target page.
4. Adds one line at the top of each page. For a tag ref it names the release, for example `v3.12.0`. For any other ref it says "the development build at" and a short commit id. The line also links to the page's source file on GitHub at that ref.
5. Creates or updates the pages. It finds each page by slug and parent. A second run updates the same pages and never makes a copy. It only writes to the `darling-docs` page and its children.
6. Sets the excerpt and the Rank Math description of each page. Both are its first paragraph, cut at a word boundary to about 155 characters. The "Audience:" line is skipped.

The parent page has the slug `darling-docs` and is a child of page 13037 (slug `free-sql-server-performance-monitoring`). Each child has the slug of its file name without `.md`. The parent page also holds a list of every page with its second-level headings. It has no filter box, because no page on the site keeps inline script.

## Arguments

| Argument | Meaning |
|---|---|
| `--ref <git ref>` | The ref to read. Default `origin/dev`. Use the tag for a release. |
| `--credentials <path>` | The WordPress credentials JSON. Required, with no default, unless you use `--dry-run`. |
| `--status draft\|publish` | Default `draft`. A draft run does not unpublish a page that is already public. |
| `--dry-run` | Writes the HTML to the `--out` folder (default `darling-docs-html`) and calls no WordPress endpoint. |
| `--out <folder>` | The folder for the `--dry-run` HTML. |

The credentials file has the keys `site_url`, `username`, `application_password` and `author_id`. The script never prints a value from it.

## Release step

The site pages describe the latest release, not dev. After the tag exists, run:

    python tools/docs-site/publish_darling_docs.py --ref v3.12.0 --credentials <credentials file> --status publish

Then open two pages, add the section to the Feature Docs strip on page 13037, and submit the new URLs to IndexNow. The full list is step 12 of `.claude/skills/release-checklist/SKILL.md`.

To check the output first, run with `--dry-run` or with the default `--status draft`, and read the pages in the WordPress editor.

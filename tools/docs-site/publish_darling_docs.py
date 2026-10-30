#!/usr/bin/env python
"""Publish the Darling docs (docs/darling/*.md) to erikdarling.com as WordPress pages.

Standard library only. Renders each page with GitHub's own markdown renderer
(`gh api markdown`), rewrites the links, and creates or updates one parent page
(slug `darling-docs`, a child of the Feature Docs landing page) and one child page
per file that docs/darling/README.md lists. See tools/docs-site/README.md.
"""
import argparse
import base64
import html
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
GITHUB = 'https://github.com/erikdarlingdata/PerformanceMonitor'
DOCS_DIR = 'docs/darling'
README = DOCS_DIR + '/README.md'
LANDING_ID = 13037                                    # the Feature Docs landing page
LANDING_SLUG = 'free-sql-server-performance-monitoring'
ROOT_SLUG = 'darling-docs'
ROOT_TITLE = 'Darling documentation'
DEFAULT_SITE = 'https://erikdarling.com'
EXCERPT_LEN = 155


def git(*args):
    r = subprocess.run(['git', '-C', REPO_ROOT, *args], capture_output=True)
    if r.returncode != 0:
        raise SystemExit('git %s failed: %s' % (' '.join(args), r.stderr.decode('utf-8', 'replace').strip()))
    return r.stdout.decode('utf-8')


def git_ok(*args):
    return subprocess.run(['git', '-C', REPO_ROOT, *args], capture_output=True).returncode == 0


def show(ref, path):
    return git('show', '%s:%s' % (ref, path))


def path_kind(ref, path):
    r = subprocess.run(['git', '-C', REPO_ROOT, 'cat-file', '-t', '%s:%s' % (ref, path)], capture_output=True)
    return r.stdout.decode().strip() if r.returncode == 0 else None


def render_markdown(md_text, scratch):
    """GitHub's renderer. mode=markdown matches what a .md file looks like on github.com."""
    tmp = os.path.join(scratch, '_render.md')
    with open(tmp, 'w', encoding='utf-8', newline='\n') as f:
        f.write(md_text)
    gh = shutil.which('gh') or 'gh'
    r = subprocess.run([gh, 'api', 'markdown', '-f', 'mode=markdown', '-F', 'text=@' + tmp], capture_output=True)
    os.remove(tmp)
    if r.returncode != 0:
        raise SystemExit('gh api markdown failed: ' + r.stderr.decode('utf-8', 'replace').strip()[:300])
    return r.stdout.decode('utf-8')


def list_pages(readme_md):
    """The files README.md links to inside docs/darling, in reading order, README itself excluded."""
    out = []
    for m in re.finditer(r'\]\(([^)#\s]+\.md)(?:#[^)]*)?\)', readme_md):
        p = posixpath.normpath(posixpath.join(DOCS_DIR, m.group(1)))
        if posixpath.dirname(p) == DOCS_DIR and p != README and p not in out:
            out.append(p)
    return out


def slug_of(path):
    return posixpath.basename(path)[:-3]


def strip_title(md_text):
    m = re.match(r'\s*#\s+(.+?)\s*\n', md_text)
    if not m:
        raise SystemExit('page has no # title')
    return m.group(1).strip(), md_text[m.end():]


def tidy_html(h):
    """Turn GitHub's markup into plain HTML that the site's pages already use."""
    # Heading: <div class="markdown-heading"><hN class=..>X</hN><a id="user-content-S" ..></a></div>
    # becomes <hN id="S">X</hN>. The id has no user-content- prefix, so a link to #S works here.
    def heading(m):
        return '<h%s id="%s">%s</h%s>' % (m.group(1), m.group(3), m.group(2), m.group(1))
    h = re.sub(r'<div class="markdown-heading"><h([1-6])[^>]*>(.*?)</h\1>\s*<a id="user-content-([^"]+)"[^>]*>.*?</a></div>',
               heading, h, flags=re.S)

    # Code blocks: no highlight spans (the site has no styles for them).
    def pre(m):
        inner = re.sub(r'</?span[^>]*>', '', m.group(1))
        inner = re.sub(r'^\s*<code[^>]*>|</code>\s*$', '', inner)
        return '<pre><code>%s</code></pre>' % inner
    h = re.sub(r'<div class="highlight[^"]*">\s*(<pre.*?</pre>)\s*</div>', r'\1', h, flags=re.S)
    h = re.sub(r'<pre[^>]*>(.*?)</pre>', pre, h, flags=re.S)
    h = re.sub(r' (?:dir|class)="(?:auto|notranslate)"', '', h)
    h = re.sub(r'</?markdown-accessiblity-table>', '', h)

    # WordPress turns a single newline inside text into <br />. Keep newlines only inside <pre>.
    parts = re.split(r'(<pre>.*?</pre>)', h, flags=re.S)
    for i in range(0, len(parts), 2):
        parts[i] = re.sub(r'\s*\n\s*', ' ', parts[i])
        parts[i] = re.sub(r'(</(?:p|h[1-6]|ul|ol|table|blockquote|details)>)\s*', r'\1\n\n', parts[i])
    for i in range(1, len(parts), 2):
        parts[i] = parts[i] + '\n\n'
    return ''.join(parts).strip() + '\n'


class Site:
    def __init__(self, site_url, ref, link_ref, pages):
        self.base = '%s/%s/%s/' % (site_url.rstrip('/'), LANDING_SLUG, ROOT_SLUG)
        self.ref = ref
        self.link_ref = link_ref
        self.pages = pages                      # repo path -> slug, README included
        self.warnings = []
        self.title_ids = {}

    def page_url(self, path):
        return self.base if path == README else self.base + self.pages[path] + '/'

    def github_url(self, path, frag=''):
        kind = path_kind(self.ref, path)
        if kind is None:
            self.warnings.append('link to a file that is not in the repo at this ref: ' + path)
            kind = 'blob'
        return '%s/%s/%s/%s%s' % (GITHUB, 'tree' if kind == 'tree' else 'blob', self.link_ref,
                                  urllib.parse.quote(path), ('#' + frag) if frag else '')

    def rewrite(self, h, src_path, found):
        """Rewrite every href. `found` collects (page path, target page path, fragment) for the anchor check."""
        def sub(m):
            href = html.unescape(m.group(1))
            if not href or href.startswith('//') or re.match(r'[a-zA-Z][a-zA-Z0-9+.-]*:', href):
                return m.group(0)
            path, _, frag = href.partition('#')
            if not path:
                found.append((src_path, src_path, frag))
                return m.group(0)
            target = posixpath.normpath(posixpath.join(posixpath.dirname(src_path), urllib.parse.unquote(path)))
            if target.startswith('..'):
                self.warnings.append('%s: link leaves the repo: %s' % (src_path, href))
                return m.group(0)
            if target in self.pages:
                if frag == self.title_ids.get(target):
                    frag = ''
                found.append((src_path, target, frag))
                url = self.page_url(target) + (('#' + frag) if frag else '')
            else:
                url = self.github_url(target, frag)
            return 'href="%s"' % html.escape(url, quote=True)
        return re.sub(r'href="([^"]*)"', sub, h)


def first_paragraph_text(h):
    for m in re.finditer(r'<p>(.*?)</p>', h, flags=re.S):
        t = html.unescape(re.sub(r'<[^>]+>', '', m.group(1)))
        t = re.sub(r'\s+', ' ', t).strip()
        if t and not t.startswith('Audience:'):         # every page opens with an "Audience:" line
            return t
    return ''


def cut_at_word(text, n=EXCERPT_LEN):
    if len(text) <= n:
        return text
    cut = text[:n + 1]
    cut = cut[:cut.rfind(' ')] if ' ' in cut else cut[:n]
    return cut.rstrip(' ,;:-')


def version_line(is_tag, ref_name, short_sha, link_ref, path):
    what = 'Darling %s' % ref_name if is_tag else 'the development build at %s, not a release' % short_sha
    src = '%s/blob/%s/%s' % (GITHUB, link_ref, path)
    return ('<p><em>This page describes %s. <a href="%s">The source of this page is on GitHub</a> at that version.</em></p>\n\n'
            % (what, src))


def h2_list(h):
    return [(m.group(1), m.group(2)) for m in re.finditer(r'<h2 id="([^"]+)">(.*?)</h2>', h, flags=re.S)]


def build_index(site, rendered):
    """Every page with its second-level headings, as nested lists of links."""
    out = ['<h2>Every page and its sections</h2>\n\n<ul>\n']
    for path, info in rendered.items():
        if path == README:
            continue
        url = site.page_url(path)
        out.append('<li><a href="%s"><strong>%s</strong></a>' % (url, html.escape(info['title'])))
        subs = h2_list(info['html'])
        if subs:
            out.append('\n<ul>\n')
            for hid, inner in subs:
                out.append('<li><a href="%s#%s">%s</a></li>\n' % (url, hid, inner))
            out.append('</ul>\n')
        out.append('</li>\n')
    out.append('</ul>\n')
    return ''.join(out)


class Wp:
    def __init__(self, creds_path):
        with open(creds_path, encoding='utf-8') as f:
            c = json.load(f)
        self.site = c.get('site_url', DEFAULT_SITE).rstrip('/')
        self.author = c.get('author_id')
        tok = base64.b64encode(('%s:%s' % (c['username'], c['application_password'])).encode()).decode()
        self.headers = {'Authorization': 'Basic ' + tok, 'User-Agent': 'darling-docs-publisher',
                        'Content-Type': 'application/json'}

    def call(self, method, path, body=None):
        data = json.dumps(body).encode('utf-8') if body is not None else None
        req = urllib.request.Request(self.site + path, data=data, method=method, headers=self.headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            raise SystemExit('%s %s -> HTTP %s: %s' % (method, path.split('?')[0], e.code, e.read().decode('utf-8', 'replace')[:300]))

    def find(self, slug, parent):
        r = self.call('GET', '/wp-json/wp/v2/pages?slug=%s&parent=%d&status=any&context=edit&_fields=id,status,slug,parent'
                      % (urllib.parse.quote(slug), parent))
        if len(r) > 1:
            raise SystemExit('more than one page has slug %s under parent %d' % (slug, parent))
        return r[0] if r else None

    def upsert(self, slug, parent, title, content, excerpt, order, status):
        body = {'title': title, 'content': content, 'excerpt': excerpt, 'menu_order': order, 'parent': parent}
        existing = self.find(slug, parent)
        if existing:
            if existing['status'] == 'publish' and status == 'draft':
                print('  %s is published; a draft run leaves it published' % slug)
            else:
                body['status'] = status
            page = self.call('POST', '/wp-json/wp/v2/pages/%d' % existing['id'], body)
            verb = 'updated'
        else:
            body.update({'slug': slug, 'status': status})
            if self.author:
                body['author'] = self.author
            page = self.call('POST', '/wp-json/wp/v2/pages', body)
            verb = 'created'
        self.call('POST', '/wp-json/rankmath/v1/updateMeta',
                  {'objectID': page['id'], 'objectType': 'post', 'meta': {'rank_math_description': excerpt}})
        print('  %s %s: id %d (%s)' % (verb, slug, page['id'], page['status']))
        return page['id']


def main():
    ap = argparse.ArgumentParser(description='Publish docs/darling to erikdarling.com as WordPress pages.')
    ap.add_argument('--ref', default='origin/dev', help='git ref to read the docs from (default origin/dev)')
    ap.add_argument('--credentials', help='path to the WordPress credentials JSON (required unless --dry-run)')
    ap.add_argument('--status', choices=['draft', 'publish'], default='draft')
    ap.add_argument('--dry-run', action='store_true', help='write the HTML to --out and call nothing')
    ap.add_argument('--out', default='darling-docs-html', help='folder for the --dry-run HTML')
    a = ap.parse_args()
    if not a.dry_run and not a.credentials:
        ap.error('--credentials is required unless --dry-run')

    sha = git('rev-parse', '--verify', a.ref + '^{commit}').strip()
    is_tag = git_ok('show-ref', '--verify', '--quiet', 'refs/tags/' + a.ref)
    link_ref = a.ref if is_tag else sha
    short = sha[:9]

    wp = None if a.dry_run else Wp(a.credentials)
    site_url = wp.site if wp else DEFAULT_SITE
    if wp:
        landing = wp.call('GET', '/wp-json/wp/v2/pages/%d?_fields=id,slug' % LANDING_ID)
        if landing['slug'] != LANDING_SLUG:
            raise SystemExit('page %d is not the Feature Docs landing page' % LANDING_ID)

    readme_md = show(a.ref, README)
    paths = list_pages(readme_md)
    pages = {README: ROOT_SLUG}
    pages.update({p: slug_of(p) for p in paths})
    site = Site(site_url, a.ref, link_ref, pages)

    os.makedirs(a.out, exist_ok=True)
    rendered = {}
    anchors_found = []
    for path in [README] + paths:
        title, _ = strip_title(show(a.ref, path))
        h = tidy_html(render_markdown(show(a.ref, path), a.out))
        # The site shows the title as the page title, so the body drops the # heading. GitHub links to
        # that heading by its id; a link to it means the top of the page.
        m = re.match(r'<h1 id="([^"]+)">.*?</h1>\s*', h, flags=re.S)
        if not m:
            raise SystemExit(path + ': no # heading in the rendered page')
        site.title_ids[path] = m.group(1)
        rendered[path] = {'title': ROOT_TITLE if path == README else title, 'html': h[m.end():]}
        print('rendered %s' % path)
    for path, info in rendered.items():
        info['html'] = site.rewrite(info['html'], path, anchors_found)

    # Every link that carries a #fragment must land on a heading that exists on the target page.
    ids = {p: set(re.findall(r'<h[1-6] id="([^"]+)"', info['html'])) for p, info in rendered.items()}
    for src, dst, frag in anchors_found:
        if frag and frag not in ids[dst]:
            site.warnings.append('%s links to %s#%s, which is not a heading there' % (src, dst, frag))

    full = {}
    for path, info in rendered.items():
        body = version_line(is_tag, a.ref, short, link_ref, path) + info['html']
        if path == README:
            body += '\n' + build_index(site, rendered)
        excerpt = cut_at_word(first_paragraph_text(info['html']))
        full[path] = (info['title'], body, excerpt)
        with open(os.path.join(a.out, pages[path] + '.html'), 'w', encoding='utf-8', newline='\n') as f:
            f.write(body)

    for w in site.warnings:
        print('WARNING: ' + w)
    print('%d pages, %d warnings, version line: %s' % (len(full), len(site.warnings),
          'tag ' + a.ref if is_tag else 'development build ' + short))
    if a.dry_run:
        print('dry run: HTML is in ' + os.path.abspath(a.out))
        return

    print('writing as %s' % a.status)
    root_id = wp.upsert(ROOT_SLUG, LANDING_ID, full[README][0], full[README][1], full[README][2], 0, a.status)
    for order, path in enumerate(paths, start=1):
        title, body, excerpt = full[path]
        wp.upsert(pages[path], root_id, title, body, excerpt, order, a.status)


if __name__ == '__main__':
    sys.exit(main())

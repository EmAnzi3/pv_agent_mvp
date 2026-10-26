#!/usr/bin/env python3
"""Collect one explicit public project scope. Keep originals until review.
No AI service, publication, login, or deletion. Source errors remain visible.
"""
from __future__ import annotations
import argparse, hashlib, ipaddress, json, os, re, socket, tempfile, time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urldefrag


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def fingerprint(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, body):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def safe_path(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError('Local path outside project directory')
    return path


def validate(url, hosts, resolve=True):
    p = urlsplit(url); host = (p.hostname or '').lower()
    if p.scheme != 'https' or p.username or p.password or p.port not in (None, 443):
        raise ValueError('Only public HTTPS without credentials is allowed')
    if not any(host == h or host.endswith('.' + h) for h in hosts):
        raise ValueError('Host outside configured scope: ' + host)
    if resolve:
        addresses = socket.getaddrinfo(host, 443)
        if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
            raise ValueError('Non-public network destination')
    return urldefrag(url)[0]


class HTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.links, self.active, self.label, self.hidden = [], [], None, [], 0
    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style', 'noscript'): self.hidden += 1
        if tag == 'a': self.active, self.label = dict(attrs).get('href'), []
    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'noscript'): self.hidden = max(0, self.hidden-1)
        if tag == 'a' and self.active:
            self.links.append((self.active, ' '.join(self.label))); self.active = None
        if tag in ('p', 'div', 'tr', 'li', 'h1', 'h2', 'h3'): self.parts.append('\n')
    def handle_data(self, text):
        if not self.hidden:
            self.parts.append(text)
            if self.active: self.label.append(text)


def extract(path, kind):
    if kind == 'pdf':
        from pypdf import PdfReader
        reader = PdfReader(path, strict=False)
        if reader.is_encrypted and not reader.decrypt(''): raise ValueError('Encrypted PDF')
        if len(reader.pages) > 2500: raise ValueError('Oversize page count; dedicated processing required')
        pages = []
        for n, page in enumerate(reader.pages, 1):
            try: text, error = page.extract_text() or '', None
            except Exception as exc: text, error = '', str(exc)[:200]
            pages.append({'page': n, 'text': text, 'extraction_error': error,
                          'visual_review_needed': len(text.strip()) < 80})
        return {'kind': kind, 'page_count': len(pages), 'pages': pages, 'links': [],
                'analysis_status': 'not_analyzed', 'whole_document_read': False}
    parser = HTML(); parser.feed(path.read_text(encoding='utf-8', errors='replace'))
    text = re.sub(r'[ \t]+', ' ', ''.join(parser.parts))
    if re.search(r'access denied|verify you are human|request rejected|just a moment', text[:1500], re.I):
        raise ValueError('Access challenge; not accepted as source content')
    return {'kind': kind, 'text': text, 'links': parser.links,
            'dynamic_content_review_needed': len(re.sub(r'\s+', ' ', text).strip()) < 100,
            'analysis_status': 'not_analyzed', 'whole_document_read': False}


def fetch(source, old, root, hosts, force=False, offline=False):
    import requests
    url = source['url']; result = {'url': url, 'title': source['title'], 'scope': source['scope'],
        'checked_at': now(), 'network_requests': 0, 'bytes_transferred': 0, 'extractions': 0}
    valid_old = bool(old and old.get('object') and safe_path(root, old['object']).is_file()
                     and fingerprint(safe_path(root, old['object'])) == old.get('sha256'))
    text_old = bool(valid_old and old.get('text') and safe_path(root, old['text']).is_file())
    if not force and text_old and (offline or old.get('next_check', '') > now()):
        return {**old, **result, 'status': 'reused_no_access'}
    if offline:
        return {**(old or {}), **result, 'status': 'offline_missing_source', 'error': 'No usable local copy'}
    headers = {'User-Agent': 'WindProjectHarvester/1.0', 'Accept-Encoding': 'identity'}
    if valid_old:
        if old.get('etag') and not old['etag'].startswith('W/'): headers['If-None-Match'] = old['etag']
        elif old.get('last_modified'): headers['If-Modified-Since'] = old['last_modified']
    try:
        with requests.Session() as client, tempfile.TemporaryDirectory(dir=root) as temp:
            final = url; started = time.monotonic()
            for _ in range(6):
                validate(final, hosts)
                result['network_requests'] += 1
                response = client.get(final, headers=headers, timeout=(8, 30), stream=True, allow_redirects=False)
                if response.status_code not in (301, 302, 303, 307, 308): break
                location = response.headers.get('Location'); response.close()
                if not location: raise ValueError('Redirect without destination')
                final = urljoin(final, location)
                headers.pop('If-None-Match', None); headers.pop('If-Modified-Since', None)
            else: raise ValueError('Too many redirects')
            with response:
                result['http_status'] = response.status_code
                if response.status_code == 304:
                    if not valid_old or not any(k in headers for k in ('If-None-Match', 'If-Modified-Since')):
                        raise ValueError('Unsolicited 304')
                    if 'If-None-Match' in headers and response.headers.get('ETag') not in (None, headers['If-None-Match']):
                        raise ValueError('Mismatched 304 validator')
                    return {**old, **result, 'status': 'unchanged_304', 'next_check': (datetime.now(timezone.utc)+timedelta(days=7)).isoformat()}
                response.raise_for_status()
                if response.status_code != 200: raise ValueError('Incomplete response status')
                if response.headers.get('Content-Encoding', 'identity') not in ('identity', ''): raise ValueError('Encoded response unsupported')
                length = int(response.headers.get('Content-Length') or 0)
                if length > 128*1024**2: raise ValueError('File too large for pilot; retained as pending')
                raw = Path(temp)/'download'
                with raw.open('wb') as stream:
                    for block in response.iter_content(131072):
                        result['bytes_transferred'] += len(block)
                        if result['bytes_transferred'] > 128*1024**2 or time.monotonic()-started > 180:
                            raise ValueError('Transfer budget exceeded; not a complete file')
                        stream.write(block)
                if not raw.stat().st_size or (length and length != raw.stat().st_size): raise ValueError('Truncated download')
                sha = fingerprint(raw)
                with raw.open('rb') as stream: prefix = stream.read(1024)
                kind = 'pdf' if b'%PDF-' in prefix else 'html'
                if kind == 'html' and b'<html' not in prefix.lower() and b'<!doctype html' not in prefix.lower():
                    raise ValueError('Unsupported format; not marked read')
                if kind != 'pdf' and source.get('expected') == 'pdf': raise ValueError('Expected PDF but received HTML')
                obj = 'originali/' + sha + ('.pdf' if kind == 'pdf' else '.html-source')
                text = 'testo/' + sha + '.json'
                target = safe_path(root, obj); target.parent.mkdir(exist_ok=True)
                if not target.exists(): os.replace(raw, target)
                if not safe_path(root, text).exists():
                    packet = extract(target, kind); save(safe_path(root, text), packet); result['extractions'] = 1
                result.update(status='downloaded' if not old or old.get('sha256') != sha else 'same_bytes_no_reanalysis',
                    object=obj, text=text, sha256=sha, bytes=target.stat().st_size, kind=kind, final_url=final,
                    etag=response.headers.get('ETag', ''), last_modified=response.headers.get('Last-Modified', ''),
                    next_check=(datetime.now(timezone.utc)+timedelta(days=7)).isoformat())
        return result
    except Exception as exc:
        return {**(old or {}), **result, 'status': 'error', 'error': str(exc)[:400],
                'previous_good_copy_retained': valid_old, 'last_access_error': str(exc)[:400]}


def links_from(source, row, root, hosts):
    if row.get('status') in ('error', 'offline_missing_source') or row.get('kind') != 'html': return []
    if source['scope'] == 'corporate_contact': return []
    body = json.loads(safe_path(root, row['text']).read_text(encoding='utf-8')); found = []
    for href, title in body.get('links', []):
        url = urljoin(row.get('final_url') or source['url'], href)
        try: url = validate(url, hosts, resolve=False)
        except ValueError: continue
        asset = bool(re.search(r'\.pdf(?:$|[?#])', url, re.I))
        relevant = bool(re.search(r'tarsia', url + ' ' + title, re.I))
        unrelated_footer = bool(re.search(r'bilancio|sostenibil|slavery|modello.?231|policy|codice.etico|privacy|cookie', url + ' ' + title, re.I))
        if unrelated_footer: continue
        if relevant or (source['scope'] == 'project_page' and asset and not re.search(r'logo|privacy|cookie|codice.etico', url + ' ' + title, re.I)):
            found.append({'url': url, 'title': title.strip() or urlsplit(url).path.split('/')[-1],
                'scope': 'project_document' if asset else 'project_page', 'expected': 'pdf' if asset else 'auto',
                'discovered_from': source['url']})
    return found


@contextmanager
def lock(root):
    path = root / '.harvest.lock'; fd = os.open(path, os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, 'w') as f: f.write(str(os.getpid()))
        yield
    finally: path.unlink(missing_ok=True)


def run(config, root, offline=False, force=False, workers=2):
    root = Path(root).resolve(); root.mkdir(parents=True, exist_ok=True); started = time.monotonic()
    with lock(root):
        manifest_path = root/'manifest.json'
        old = json.loads(manifest_path.read_text(encoding='utf-8')) if manifest_path.exists() else {}
        if old and old.get('project_id') != config['project_id']: raise ValueError('Different project directory')
        records = old.get('resources', {}); sources = {s['url']: s for s in config['sources']}
        for url, row in records.items(): sources.setdefault(url, {k: row[k] for k in ('url', 'title', 'scope')})
        results, visited, depths = [], set(), config.get('link_depth', 2)
        with ThreadPoolExecutor(max_workers=min(2, max(1, workers))) as pool:
            for depth in range(depths+1):
                batch = [s for u,s in sources.items() if u not in visited]
                if not batch: break
                if len(visited)+len(batch) > 80: raise ValueError('Scope grew beyond pilot; no silent truncation')
                for source, row in zip(batch, pool.map(lambda s: fetch(s, records.get(s['url']), root, config['allowed_hosts'], force, offline), batch)):
                    visited.add(source['url']); records[source['url']] = row; results.append(row)
                    save(manifest_path, {'project_id': config['project_id'], 'generated_at': now(),
                        'scope': config['scope_note'], 'resources': records, 'onedrive_upload_verified': False,
                        'semantic_analysis': 'separate_chat_review', 'paid_model_calls': 0})
                    if depth < depths:
                        for child in links_from(source, row, root, config['allowed_hosts']): sources.setdefault(child['url'], child)
        errors = [r['url'] for r in results if r['status'] in ('error','offline_missing_source')]
        summary = {'project_id': config['project_id'], 'resource_count': len(records),
            'successful': len(results)-len(errors), 'errors': errors,
            'pdf_resources': sum(r.get('kind') == 'pdf' and r['status'] not in ('error','offline_missing_source') for r in results),
            'unique_originals': len({r.get('sha256') for r in records.values() if r.get('sha256')}),
            'network_requests': sum(r['network_requests'] for r in results),
            'bytes_transferred': sum(r['bytes_transferred'] for r in results),
            'text_extractions': sum(r['extractions'] for r in results),
            'elapsed_seconds': round(time.monotonic()-started,3), 'paid_model_calls': 0,
            'all_project_documentation_certified': False, 'onedrive_upload_verified': False,
            'semantic_facts_created': 0, 'offline': offline}
        save(root/('harvest-offline.json' if offline else 'harvest-first.json'), summary)
        return summary


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', required=True); p.add_argument('--output', required=True)
    p.add_argument('--offline', action='store_true'); p.add_argument('--check-changes', action='store_true')
    a = p.parse_args(); print(json.dumps(run(json.loads(Path(a.config).read_text(encoding='utf-8')), a.output, a.offline, a.check_changes), ensure_ascii=False, indent=2))

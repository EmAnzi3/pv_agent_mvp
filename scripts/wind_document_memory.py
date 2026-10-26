#!/usr/bin/env python3
"""Hash-pinned semantic memory. Extraction is never counted as review.

The committed memory is durable; raw originals remain in the harvest workspace.
New/changed bytes produce a targeted queue. Reviewed unchanged bytes do not open
extracted text, even after the raw cache has been lost and downloaded again.
"""
import argparse
import copy
import json
from pathlib import Path
from harvest_wind_project import fingerprint, safe_path, save


def plan(manifest, memory, root):
    if manifest['project_id'] != memory['project_id']:
        raise ValueError('Wrong project memory')
    queue, skipped, failed = [], [], []
    queued_by_sha={}
    for url, row in manifest['resources'].items():
        if row.get('status') in ('error', 'offline_missing_source'):
            failed.append({'url':url, 'error':row.get('error'),
                           'previous_sha256':row.get('sha256')})
            continue
        sha=row.get('sha256')
        if not sha or not row.get('object') or fingerprint(safe_path(Path(root),row['object'])) != sha:
            raise ValueError('Missing/corrupt original: '+url)
        old=memory.get('documents',{}).get(sha)
        if old and old.get('analysis_status')=='targeted_reviewed':
            skipped.append({'url':url,'sha256':sha,'reason':'same_bytes_reviewed'})
            continue
        if sha in queued_by_sha:
            queued_by_sha[sha].setdefault('alias_urls',[]).append(url)
            continue
        text=json.loads(safe_path(Path(root),row['text']).read_text(encoding='utf-8'))
        # No auto facts: reviewers inspect the project-specific text and drawings.
        queue.append({'url':url,'sha256':sha,'title':row['title'],
                      'page_count':text.get('page_count'),
                      'pages':text.get('pages',[]),'analysis_status':'pending_targeted_review'})
        queued_by_sha[sha]=queue[-1]
    return {'project_id':manifest['project_id'],'pending':queue,
            'skipped':skipped,'failures':failed,'analyzed_new':0,'paid_model_calls':0}


def record(memory, review, packet, original):
    """Accept only an explicit, page-pinned human/assistant review of these bytes."""
    if review['project_id'] != memory['project_id']:
        raise ValueError('Wrong project')
    sha=fingerprint(original)
    if review['sha256']!=sha:raise ValueError('Review is for different bytes')
    result=copy.deepcopy(memory)
    existing=result.setdefault('documents',{}).get(sha)
    if existing and existing.get('analysis_status')=='targeted_reviewed':
        # A URL alias is provenance, not a new semantic review. Keep the
        # hash-pinned analysis immutable and do not access extracted pages.
        semantic=lambda r:{k:v for k,v in r.items() if k not in ('url','alias_urls')}
        if semantic(existing)!=semantic(review):
            raise ValueError('Existing review immutable; explicit revision required')
        if not review.get('url'):raise ValueError('Alias URL required')
        versions=result.setdefault('versions_by_url',{}).setdefault(review['url'],[])
        if sha not in versions:versions.append(sha)
        return result
    pages={p['page']:p['text'] for p in packet['pages']}
    inspected=review.get('pages_reviewed',[])
    if not inspected or any(p not in pages for p in inspected):
        raise ValueError('Explicit reviewed pages required')
    if not review.get('outcome_note') or not review.get('reviewed_on') or not review.get('url'):
        raise ValueError('Review provenance required')
    for fact in review.get('findings',[]):
        if fact['page'] not in inspected or not fact.get('statement'):
            raise ValueError('Unreviewed finding page')
        # Text excerpts are normalized only for whitespace, never fuzzy-matched.
        quote=' '.join(fact.get('excerpt','').split())
        if not quote or quote not in ' '.join(pages[fact['page']].split()):
            raise ValueError('Finding excerpt not in pinned page')
    if review.get('analysis_status')!='targeted_reviewed':raise ValueError('Not a completed targeted review')
    result['documents'][sha]=review
    result.setdefault('versions_by_url',{}).setdefault(review['url'],[])
    if sha not in result['versions_by_url'][review['url']]:result['versions_by_url'][review['url']].append(sha)
    return result


if __name__=='__main__':
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--harvest',required=True);ap.add_argument('--memory',required=True)
    ap.add_argument('--output',required=True)
    a=ap.parse_args()
    root=Path(a.harvest)
    result=plan(json.loads((root/'manifest.json').read_text()),json.loads(Path(a.memory).read_text()),root)
    save(a.output,result)
    print(json.dumps({'pending':len(result['pending']),'skipped':len(result['skipped']),
                      'failures':len(result['failures']),'analyzed_new':0}))

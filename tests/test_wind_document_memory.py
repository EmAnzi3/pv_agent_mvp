import copy,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import wind_document_memory as m

class MemoryTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.root=Path(self.tmp.name);(self.root/'a.pdf').write_bytes(b'%PDF-test')
  self.sha=m.fingerprint(self.root/'a.pdf')
  self.packet={'pages':[{'page':1,'text':'RWE progetto Serra 42 MW'},{'page':2,'text':'nessun contatto'}]}
  (self.root/'text.json').write_text(json.dumps(self.packet))
  self.mem={'project_id':'serra-giannina','documents':{}}
  self.url='https://va.mite.gov.it/File/Documento/123'
  self.man={'project_id':'serra-giannina','resources':{self.url:{'status':'downloaded','sha256':self.sha,'object':'a.pdf','text':'text.json','title':'test'}}}
  self.review={'project_id':'serra-giannina','sha256':self.sha,'url':self.url,'reviewed_on':'2026-10-15','pages_reviewed':[1], 'analysis_status':'targeted_reviewed','outcome_note':'identità e potenza','findings':[{'page':1,'excerpt':'Serra 42 MW','statement':'42 MW dichiarati'}]}
 def test_extraction_never_counts_as_analysis(self):
  q=m.plan(self.man,self.mem,self.root)
  self.assertEqual(len(q['pending']),1);self.assertEqual(q['analyzed_new'],0)
 def test_new_duplicate_has_one_queue_entry_and_no_second_text_access(self):
  alias='https://va.mite.gov.it/File/Documento/456'
  row=copy.deepcopy(self.man['resources'][self.url]);row['text']='must-not-be-read.json'
  self.man['resources'][alias]=row
  q=m.plan(self.man,self.mem,self.root)
  self.assertEqual(len(q['pending']),1)
  self.assertEqual(q['pending'][0]['alias_urls'],[alias])
 def test_unchanged_skips_text_access(self):
  mem=m.record(self.mem,self.review,self.packet,self.root/'a.pdf')
  (self.root/'text.json').unlink()
  q=m.plan(self.man,mem,self.root)
  self.assertEqual(len(q['skipped']),1);self.assertFalse(q['pending'])
 def test_changed_hash_requires_new_review(self):
  mem=m.record(self.mem,self.review,self.packet,self.root/'a.pdf')
  (self.root/'a.pdf').write_bytes(b'%PDF-changed')
  self.man['resources'][self.url]['sha256']=m.fingerprint(self.root/'a.pdf')
  q=m.plan(self.man,mem,self.root);self.assertEqual(len(q['pending']),1)
  self.assertIn(self.sha,mem['documents'])
 def test_failures_do_not_poison_previous_review(self):
  mem=m.record(self.mem,self.review,self.packet,self.root/'a.pdf')
  self.man['resources'][self.url]['status']='error'
  q=m.plan(self.man,mem,self.root)
  self.assertEqual(len(q['failures']),1);self.assertFalse(q['pending'])
  self.assertIn(self.sha,mem['documents'])
 def test_wrong_hash_page_quote_project_rejected(self):
  for key,value in [('sha256','0'*64),('pages_reviewed',[3]),('project_id','other')]:
   r=copy.deepcopy(self.review);r[key]=value
   with self.assertRaises(ValueError):m.record(self.mem,r,self.packet,self.root/'a.pdf')
  r=copy.deepcopy(self.review);r['findings'][0]['excerpt']='invented EPC'
  with self.assertRaises(ValueError):m.record(self.mem,r,self.packet,self.root/'a.pdf')
 def test_corrupt_original_not_skipped(self):
  self.mem['documents'][self.sha]=self.review
  (self.root/'a.pdf').write_bytes(b'corrupt')
  with self.assertRaises(ValueError):m.plan(self.man,self.mem,self.root)
 def test_existing_review_not_overwritten(self):
  mem=m.record(self.mem,self.review,self.packet,self.root/'a.pdf')
  r=copy.deepcopy(self.review);r['outcome_note']='replacement'
  with self.assertRaises(ValueError):m.record(mem,r,self.packet,self.root/'a.pdf')
 def test_alias_registration_preserves_analysis_without_text(self):
  mem=m.record(self.mem,self.review,self.packet,self.root/'a.pdf')
  alias=copy.deepcopy(self.review);alias['url']='https://va.mite.gov.it/File/Documento/456'
  result=m.record(mem,alias,None,self.root/'a.pdf')
  self.assertEqual(result['documents'],mem['documents'])
  self.assertEqual(result['versions_by_url'][alias['url']],[self.sha])
  self.assertEqual(result['versions_by_url'][self.url],[self.sha])
  self.assertEqual(m.record(result,alias,None,self.root/'a.pdf'),result)
  self.assertNotIn(alias['url'],mem['versions_by_url'])
if __name__=='__main__':unittest.main()

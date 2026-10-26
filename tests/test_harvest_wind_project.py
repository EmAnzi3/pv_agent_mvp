import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import harvest_wind_project as h
class HarvestTests(unittest.TestCase):
 def test_https(self):
  for u in ('http://eni.com/a','https://u:p@eni.com/a','https://eni.com:123/a','https://eni.com.attacker.test/a'):
   with self.assertRaises(ValueError):h.validate(u,['eni.com'],False)
 def test_valid_url(self):self.assertEqual(h.validate('https://www.eni.com/a#x',['eni.com'],False),'https://www.eni.com/a')
 def test_private_dns(self):
  with patch.object(h.socket,'getaddrinfo',return_value=[(2,1,6,'',('127.0.0.1',443))]):
   with self.assertRaises(ValueError):h.validate('https://eni.com/a',['eni.com'])
 def test_paths(self):
  with tempfile.TemporaryDirectory() as tmp:
   with self.assertRaises(ValueError):h.safe_path(Path(tmp),'../../secrets')
 def test_extract_is_not_analysis(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'raw';p.write_text('<html><body>Proponente Esempio 13 MW<script>secret</script></body></html>')
   r=h.extract(p,'html');self.assertFalse(r['whole_document_read']);self.assertEqual(r['analysis_status'],'not_analyzed');self.assertNotIn('secret',r['text'])
 def test_warm_and_offline(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);(root/'originali').mkdir();(root/'testo').mkdir()
   p=root/'originali/a';p.write_text('<html>test</html>');(root/'testo/a.json').write_text('{}')
   old={'object':'originali/a','text':'testo/a.json','sha256':h.fingerprint(p),'next_check':'2999','kind':'html'}
   source={'url':'https://eni.com/a','title':'test','scope':'project_page'}
   with patch('requests.Session',side_effect=AssertionError('network forbidden')):
    for offline in (False,True):
     r=h.fetch(source,old,root,['eni.com'],offline=offline);self.assertEqual(r['network_requests'],0);self.assertEqual(r['status'],'reused_no_access')
 def test_corrupt_not_reused(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'a';p.write_text('bad');old={'object':'a','sha256':'0'*64,'next_check':'2999'}
   r=h.fetch({'url':'https://eni.com/a','title':'t','scope':'project_page'},old,Path(tmp),['eni.com'],offline=True)
   self.assertEqual(r['status'],'offline_missing_source')
 def test_lock(self):
  with tempfile.TemporaryDirectory() as tmp:
   with h.lock(Path(tmp)):
    with self.assertRaises(FileExistsError):
     with h.lock(Path(tmp)):pass
   self.assertFalse((Path(tmp)/'.harvest.lock').exists())
 def test_no_delete_existing_original(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);p=root/'keep';p.write_text('good')
   h.run({'project_id':'t','scope_note':'test','allowed_hosts':[],'sources':[]},root,offline=True)
   self.assertEqual(p.read_text(),'good')
if __name__=='__main__':unittest.main()

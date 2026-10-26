import importlib.util,unittest,json,tempfile,copy
from pathlib import Path
p=Path(__file__).resolve().parents[1]/"scripts/wind_mase_inventory.py"
spec=importlib.util.spec_from_file_location("wind_mase_inventory",p)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def html(page=1,ident=111):
 return ("""<h2>(n.2) Documenti</h2><div>Pagina %s di 2</div>
 <a href="?pagina=2">2</a><table><tr>
 <td>Cronoprogramma Serra Giannina</td><td>a.pdf</td><td>Documentazione</td>
 <td>A1</td><td>17/06/2026</td><td>-</td><td>329 kB</td>
 <td><a href="/File/Documento/%d">Scarica</a></td></tr></table>"""%(page,ident)).encode()
class TestIndex(unittest.TestCase):
 def test_parse(self):
  r=m.parse(html(),"https://va.mite.gov.it/it-IT/Oggetti/Documentazione/7112/18807")
  self.assertEqual((r["expected"],r["page"],r["pages"]),(2,1,2))
  self.assertEqual(r["rows"][0]["url"],"https://va.mite.gov.it/File/Documento/111")
 def test_invalid_html(self):
  with self.assertRaises(ValueError):m.parse(b"<html>Forbidden</html>","https://va.mite.gov.it/it-IT/Oggetti/Documentazione/7112/18807")
 def test_exhaustive_counts(self):
  class Response:
   def __init__(self,content):self.content=content
  class Session:
   def get(self,url,**kw):return Response(html(2,222) if "pagina=2" in url else html())
  resp=Session()
  old=m.get;m.get=lambda session,url:resp.get(url).content
  try:
   r=m.inventory(None,{"id":"18807","url":"https://va.mite.gov.it/it-IT/Oggetti/Documentazione/7112/18807"})
   self.assertTrue(r["complete"]);self.assertEqual(len(r["rows"]),2)
  finally:m.get=old
 def test_reject_non_mase(self):
  with self.assertRaises(ValueError):m.validate("https://localhost/")
 def test_offline_rejects_changed_procedure_identity(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);config=root/'config.json';out=root/'out';out.mkdir()
   spec={'id':'19356','url':'https://va.mite.gov.it/it-IT/Oggetti/Documentazione/7112/19356'}
   c={'project_id':'serra','mase_object_id':7112,'procedures':[spec],'extra_procedures_uninventoried':[]}
   report={'project_id':'serra','object_id':7112,'procedures':[{**spec,'complete':True,'rows':[]}],'extra_procedures_uninventoried':[]}
   (out/'inventory.json').write_text(json.dumps(report));config.write_text(json.dumps(c))
   self.assertEqual(m.run(config,out,offline=True)['indexed_procedures'],1)
   for key,value in [('id','other'),('url','https://va.mite.gov.it/it-IT/Oggetti/Documentazione/7112/18807')]:
    wrong=copy.deepcopy(c);wrong['procedures'][0][key]=value;config.write_text(json.dumps(wrong))
    with self.assertRaisesRegex(ValueError,'IDs/URLs'):m.run(config,out,offline=True)
if __name__=="__main__":unittest.main()

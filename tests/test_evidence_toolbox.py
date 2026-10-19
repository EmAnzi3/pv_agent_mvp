import unittest
from experiments.evidence_toolbox import Citation, valid_vat_it, location, schedule, company_role, closeout

class ToolboxTests(unittest.TestCase):
    def test_company_vat(self):
        self.assertTrue(valid_vat_it("10148261216"))
        self.assertFalse(valid_vat_it("10148261217"))
    def test_no_inferred_execution(self):
        with self.assertRaises(ValueError):
            company_role("Engineering Srl","EPC",None)
    def test_no_guessed_coordinates(self):
        with self.assertRaises(ValueError):
            location("41.394, 15.389","coordinates",None)
    def test_duration_not_cod(self):
        result=schedule(24,None,None)
        self.assertIsNone(result["end_date"])
        self.assertEqual(result["start_status"],"unknown")
    def test_closeout_rejects_unvalidated(self):
        result=closeout(112,112,113,113,0,["roles not verified"],False)
        self.assertFalse(result["complete"])
    def test_closeout_acceptance(self):
        self.assertTrue(closeout(112,112,113,113,0,[],True)["complete"])

if __name__ == "__main__":
    unittest.main()

from fdds.utils import moveTablets as mv

site1 = {}

site1["host"] = "localhost"
site1["port"] = "5551"
site1["database"] = "postgres"
site1["username"] = "varshith"
site1["password"] = ""

site2 = {}

site2["host"] = "localhost"
site2["port"] = "5552"
site2["database"] = "postgres"
site2["username"] = "varshith"
site2["password"] = ""


tab1 = "tab"
tab2 = "tabler"

mv(site1, site2, tab1, tab2)
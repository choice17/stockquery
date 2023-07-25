import requests

HEADER = {'USER-AGENT': "Mozilla/5.0"}
FMT = "https://tw.stock.yahoo.com/class-quote?sectorId=%d&exchange=TAI"

sector={1:"cement",
        2:"food",
        3:"plastic",
        4:"textile",
        6:"electric",
        7:"electricalcable",
        37:"chemistry",
        38:"biotech",
        9:"glass",
        10:"paper",
        11:"steel",
        12:"rubber",
        13:"motor",
        40:"semiconductor",
        41:"computer",
        42:"photoelectric",
        43:"communication",
        44:"electronicparts",
        45:"electricappliance",
        46:"itservice",
        47:"otherelectric",
        19:"construction",
        20:"shipping",
        21:"sightseeing",
        22:"finance",
        24:"departmentstore",
        39:"gasoline",
        }
q1 = FMT % 1
r = requests.get(q1, headers={'USER-AGENT': "Mozilla/5.0"})
print(r.status_code)
if (r.status_code == requests.codes.ok):
	print("OK")
	f= open("sector1.txt", "w",encoding='UTF-8')
	f.write(r.text)
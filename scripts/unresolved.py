import pandas as pd
import unicodedata
import re
import os

# ----------------------------
# CONFIGURATION
# ----------------------------
GTFS_NIGHT_FOLDERS = [
    "data/raw/night/Switzerland/",
    "data/raw/night/long_distance/",
    "data/raw/night/sncf-data/",
    "data/raw/night/open_data/",
    "data/raw/night/long_distance_rail/"
]

OUTPUT_FILE = "data/extracted/cleaned_night_routes.csv"

FOLDER_COUNTRY_MAP = {
    "data/raw/night/Switzerland/": "CH",
    "data/raw/night/long_distance/": None,
    "data/raw/night/sncf-data/": "FR",
    "data/raw/night/open_data/": None,
    "data/raw/night/long_distance_rail/": None
}

# Full station-to-country map covering all 862 previously unresolved stations
STATION_COUNTRY_MAP = {
    # GERMANY
    "aachen hbf": "DE", "aalen hbf": "DE", "alfeld leine": "DE", "altenbeken": "DE",
    "andernach": "DE", "angermunde": "DE", "anklam": "DE", "ansbach": "DE",
    "aschaffenburg hbf": "DE", "augsburg hauptbahnhof": "DE", "augsburg hbf": "DE",
    "augustfehn": "DE", "bad bentheim": "DE", "bad bentheim gr": "DE",
    "bad bevensen": "DE", "bad endorf": "DE", "bad hersfeld": "DE",
    "bad oeynhausen": "DE", "bad schandau": "DE", "baden boyd": "DE",
    "bamberg": "DE", "bebra": "DE", "bensheim": "DE", "berchtesgaden hbf": "DE",
    "bergen auf rugen": "DE", "bielefeld hbf": "DE", "bingen rhein hbf": "DE",
    "bischofshofen": "DE", "bitterfeld": "DE", "boblingen": "DE",
    "bochum hbf": "DE", "bonn bad godesberg": "DE", "bonn beuel": "DE",
    "bonn hbf": "DE", "brandenburg hbf": "DE", "braunschweig hbf": "DE",
    "bremen hbf": "DE", "bruchsal": "DE", "bullay db": "DE", "bunde westf": "DE",
    "butzow": "DE", "celle": "DE", "chemnitz hbf": "DE", "cottbus hbf": "DE",
    "crailsheim": "DE", "darmstadt hbf": "DE", "delmenhorst": "DE",
    "dessau hbf": "DE", "donauworth": "DE", "dortmund hauptbahnhof": "DE",
    "dortmund hbf": "DE", "dresden hbf": "DE", "dresden neustadt": "DE",
    "duisburg hbf": "DE", "duren": "DE", "dusseldorf flughafen": "DE",
    "dusseldorf hbf": "DE", "eberswalde hbf": "DE", "eisenach": "DE",
    "eisenach hbf": "DE", "elsterwerda": "DE", "elze han": "DE",
    "emden auenhafen": "DE", "emden hbf": "DE", "emmerich gr": "DE",
    "erfurt hbf": "DE", "erlangen": "DE", "essen hbf": "DE",
    "fehmarn burg": "DE", "flensburg": "DE", "freilassing bahnhof": "DE",
    "freiburg breisgau hbf": "DE", "freiburg hauptbahnhof": "DE",
    "friedberg hess": "DE", "fulda": "DE", "gaufelden": "DE",
    "gelsenkirchen hbf": "DE", "gera hbf": "DE", "gieen": "DE",
    "glandorf bahnhst": "DE", "gottingen": "DE", "greifswald": "DE",
    "gunzburg": "DE", "gutersloh hbf": "DE", "hagen hbf": "DE",
    "haffkrug": "DE", "halle saale hbf": "DE", "hamm westf": "DE",
    "hamm westf hbf": "DE", "hanau hbf": "DE", "hannover hbf": "DE",
    "hannover messe laatzen": "DE", "hausach": "DE", "heidelberg hbf": "DE",
    "helmstedt": "DE", "hengelo": "DE", "herford": "DE",
    "herne wanne eickel hbf": "DE", "herrenberg": "DE", "herzogenrath": "DE",
    "hildesheim gbf": "DE", "hildesheim hbf": "DE", "homburg saar hbf": "DE",
    "horb": "DE", "horka gr": "DE", "hude": "DE", "hunfeld": "DE",
    "ingolstadt hbf": "DE", "iserlohn letmathe": "DE", "itzehoe": "DE",
    "jena goschwitz": "DE", "jena paradies": "DE", "karlsruhe hbf": "DE",
    "kassel wilhelmshohe": "DE", "kiel hbf": "DE", "koblenz hbf": "DE",
    "konstanz": "DE", "kothen": "DE", "krefeld hbf": "DE",
    "kreiensen": "DE", "kreuztal": "DE", "kronach": "DE",
    "lahr schwarzw": "DE", "lichtenfels": "DE", "limburg sud": "DE",
    "lindau hbf": "DE", "lindau insel": "DE", "lindau reutin": "DE",
    "lingen ems": "DE", "lippstadt": "DE", "lubeck hbf": "DE",
    "ludwigslust": "DE", "luneburg": "DE", "lutherstadt wittenberg hbf": "DE",
    "magdeburg hbf": "DE", "mainz hbf": "DE", "mannheim hbf": "DE",
    "marburg lahn": "DE", "marienhafe": "DE", "meppen": "DE",
    "minden kaiserstrae": "DE", "minden westf": "DE",
    "monchengladbach hbf": "DE", "montabaur": "DE", "muhlacker": "DE",
    "munster westf hbf": "DE", "munster westf": "DE", "naumburg saale hbf": "DE",
    "neumunster": "DE", "neustadt weinstr hbf": "DE", "neustrelitz hbf": "DE",
    "nienburg weser": "DE", "norddeich": "DE", "norddeich mole": "DE",
    "northeim han": "DE", "nurnberg hauptbahnhof": "DE", "nurnberg hbf": "DE",
    "oberhausen hbf": "DE", "oberndorf neckar": "DE", "oberstdorf": "DE",
    "offenburg": "DE", "oldenburg holst": "DE", "oldenburg oldb": "DE",
    "oldenburg oldb hbf": "DE", "oranienburg": "DE", "osnabruck hbf": "DE",
    "ostseebad binz": "DE", "papenburg ems": "DE", "pasewalk": "DE",
    "passau hauptbahnhof": "DE", "passau hbf": "DE", "pforzheim hbf": "DE",
    "plattling": "DE", "plochingen": "DE", "prien a chiemsee": "DE",
    "puttgarden ms": "DE", "ravensburg": "DE", "recklinghausen hbf": "DE",
    "regensburg hbf": "DE", "remagen": "DE", "rheine": "DE",
    "ribnitz damgarten ost": "DE", "ribnitz damgarten west": "DE",
    "riesa": "DE", "rosenheim": "DE", "rosenheim bahnhof": "DE",
    "rostock hbf": "DE", "rottweil": "DE", "saalfeld saale": "DE",
    "saarbrucken hbf": "DE", "schwabisch gmund": "DE", "schwerin hbf": "DE",
    "siegburg bonn": "DE", "siegen hbf": "DE", "singen hohentwiel": "DE",
    "soest": "DE", "solingen hbf": "DE", "solling bahnhof": "DE",
    "spaichingen": "DE", "stendal": "DE", "stendal hbf": "DE",
    "stralsund hbf": "DE", "stuttgart hauptbahnhof 1": "DE",
    "stuttgart hauptbahnhof 16": "DE", "stuttgart hauptbahnhof 5": "DE",
    "stuttgart hbf": "DE", "traunstein": "DE", "treuchtlingen": "DE",
    "treysa": "DE", "tubingen hbf": "DE", "tuttlingen": "DE",
    "uelzen": "DE", "ulm hbf": "DE", "vaihingen enz": "DE",
    "verden aller": "DE", "wabern bz kassel": "DE", "wanne eickel hbf": "DE",
    "warburg westf": "DE", "waren muritz": "DE", "warnemunde": "DE",
    "weimar": "DE", "weinheim bergstr hbf": "DE", "weienfels": "DE",
    "westerland sylt": "DE", "wiesbaden hbf": "DE", "wiesloch walldorf": "DE",
    "wittenberge": "DE", "wolfsburg hbf": "DE", "wuppertal hbf": "DE",
    "wurzburg hbf": "DE", "coswig b dresden": "DE", "hauptbahnhof": "DE",
    "hauptbahnhof a1": "DE", "hermsdorf klosterlausnitz": "DE",
    "berlin gesundbrunnen": "DE", "berlin hbf": "DE", "berlin ostbahnhof": "DE",
    "berlin spandau": "DE", "berlin sudkreuz": "DE",
    "frankfurt m flughafen fernbf": "DE", "munchen hbf": "DE",
    "munchen pasing": "DE", "garmisch partenkirchen": "DE",
    "zussow": "DE", "gotha": "DE", "leer ostfriesl": "DE",
    "lennestadt grevenbruck": "DE",

    # AUSTRIA
    "absdorf hippersdorf bahnhof": "AT", "achenlohe bahnhst": "AT",
    "admont bahnhof": "AT", "aisthofen bahnhst": "AT",
    "altenmarkt bahnhof": "AT", "altmunster traunsee bahnhof": "AT",
    "amstetten bahnhof": "AT", "angern": "AT",
    "arnoldstein bahnhof": "AT", "aspang markt bahnhof": "AT",
    "attnang puchheim bahnhof": "AT", "attnang puchheim bahnhof obergeschoss b1": "AT",
    "ausschlag zobern bahnhof": "AT", "bad aussee bahnhof": "AT",
    "bad erlach bahnhof": "AT", "bad fischau brunn bahnhof": "AT",
    "bad gastein bahnhof": "AT", "bad goisern bahnhof": "AT",
    "bad ischl bahnhof": "AT", "bad mitterndorf bahnhof": "AT",
    "bad mitterndorf heilbrunn bahnhof": "AT", "bad radkersburg bahnhof": "AT",
    "bad ried bahnhst": "AT", "bad schallerbach wallern bahnhof": "AT",
    "baumgartenberg bahnhst": "AT", "bernhardsthal bahnhof": "AT",
    "bisamberg bahnhof": "AT", "bischofshofen bahnhof": "AT",
    "bleiburg bahnhof": "AT", "bleiburg stadt bahnhst": "AT",
    "bludenz": "AT", "bludenz bahnhof": "AT", "bludenz moos bahnhof": "AT",
    "bockstein bahnhof": "AT", "bohlerwerk an der ybbs bahnhof": "AT",
    "bondorf b herrenberg": "AT", "braunau inn bahnhof": "AT",
    "breitenbrunn bahnhof": "AT", "bruck leitha bahnhof": "AT",
    "bruck mur bahnhof 1": "AT", "bruck mur bahnhof 2": "AT",
    "bruck mur bahnhof 26": "AT", "bruck mur bahnhof 3": "AT",
    "bruck mur bahnhof 4": "AT", "bruck mur bahnhof 5": "AT",
    "bruck mur bahnhof 6": "AT", "brunn an der pitten bahnhof": "AT",
    "brunn maria enzersdorf bahnhof": "AT", "buchs bahnhof": "AT",
    "dechantskirchen bahnhof": "AT", "deutsch wagram bahnhof": "AT",
    "deutschkreutz bahnhof": "AT", "dellach im drautal bahnhof": "AT",
    "dornbirn": "AT", "dornbirn bahnhof": "AT",
    "dornbirn haselstauden bahnhof": "AT", "dornbirn hatlerdorf bahnhof": "AT",
    "dornbirn schoren bahnhof": "AT", "dorfgastein": "AT",
    "durnbach enns bahnhst": "AT", "durnberg b ottensheim bahnhof": "AT",
    "durnkrut bahnhof": "AT", "ebensee landungsplatz bahnhst": "AT",
    "eberschwang bahnhof": "AT", "ebreichsdorf bahnhof": "AT",
    "edlitz grimmenstein bahnhof": "AT", "eichgraben altlengbach bahnhof": "AT",
    "eisenstadt bahnhof": "AT", "eisenstadt schule": "AT",
    "emmersdorf im gailtal bahnhst": "AT", "ennsdorf bahnhof": "AT",
    "ernsthofen bahnhof": "AT", "etsdorf stra bahnhof": "AT",
    "eugendorf s bahn": "AT", "fehring": "AT", "feldbach 2": "AT",
    "feldkirch": "AT", "feldkirch bahnhof": "AT",
    "feldkirchen in ktn bahnhof": "AT", "feldkirchen seiersberg bahnhof": "AT",
    "felixdorf bahnhof": "AT", "fels am wagram bahnhof": "AT",
    "fieberbrunn": "AT", "fischamend bahnhof": "AT",
    "foderlach bahnhof": "AT", "frankenmarkt bahnhof": "AT",
    "friedberg bahnhof": "AT", "friedburg bahnhof": "AT",
    "friesach bahnhof": "AT", "frohnleiten bahnhof 2": "AT",
    "frohnleiten bahnhof 3": "AT", "furstenfeld bahnhof": "AT",
    "gaisbach wartberg bahnhof": "AT", "ganserndorf bahnhof": "AT",
    "gars thunau bahnhof": "AT", "garsten bahnhof": "AT",
    "gedersdorf bahnhof": "AT", "gemeinlebarn bahnhof": "AT",
    "gerasdorf bahnhof": "AT", "gisingen bahnhof": "AT",
    "gleienfeld bahnhof": "AT", "gleisdorf 2": "AT", "gleisdorf 4": "AT",
    "gmd bahnhof": "AT", "gobelsburg bahnhof": "AT",
    "gollersdorf bahnhof": "AT", "golling abtenau bahnhof": "AT",
    "gopfritz": "AT", "gotzendorf bahnhof": "AT", "gotzis bahnhof": "AT",
    "gramatneusiedl bahnhof": "AT", "greifenburg weiensee bahnhof": "AT",
    "grein bad kreuzen bahnhof": "AT", "grein stadt bahnhof": "AT",
    "groendorf bahnhst": "AT", "grunau im almtal bahnhof": "AT",
    "grunbach schneeberg schule": "AT", "gutenstein bahnhof": "AT",
    "hadersdorf kamp bahnhof": "AT", "hainburg donau kulturfabrik bhf": "AT",
    "hainfeld bahnhof": "AT", "hall in tirol bahnhof": "AT",
    "hall in tirol bahnhof b": "AT", "hall thaur bahnhof": "AT",
    "hard fussach bahnhof": "AT", "hartberg bahnhof": "AT",
    "haslau an der donau bahnhof": "AT", "hatzendorf bahnhof": "AT",
    "hausleiten bahnhof": "AT", "helmahof bahnhof": "AT",
    "hermagor bahnhof": "AT", "herzogenburg bahnhof": "AT",
    "hieflau bahnhof": "AT", "hinterstoder bahnhof": "AT",
    "hirtenberg bahnhof": "AT", "hochfilzen bahnhof": "AT",
    "hoflein donau bahnhof": "AT", "hohenau": "AT",
    "hohenems bahnhof": "AT", "hollabrunn bahnhof": "AT",
    "hopfgarten i br bahnhof": "AT", "hopfgarten i br berglift bahnhof": "AT",
    "hopfgarten im brixental": "AT", "horn bahnhof": "AT",
    "hotzelsdorf geras bahnhof": "AT", "imst pitztal": "AT",
    "imst pitztal bahnhof": "AT", "jenbach": "AT", "jenbach bahnhof": "AT",
    "jenbach bahnhof a": "AT", "jenbach bahnhof sev": "AT",
    "jois bahnhof": "AT", "judenburg bahnhof": "AT",
    "kaindorf bahnhof": "AT", "kainisch bahnhof": "AT",
    "kalwang bahnhof": "AT", "kammer schorfling bahnhst": "AT",
    "kammern bahnhof": "AT", "kapfenberg bahnhof": "AT",
    "kapfenberg fachhochschule": "AT", "kastenreith bahnhst": "AT",
    "katzelsdorf bahnhof": "AT", "kimpling bahnhst": "AT",
    "kindberg bahnhof": "AT", "kirchberg i t bahnhof": "AT",
    "kirchdorf krems bahnhof": "AT", "kitzbuhel schwarzsee bahnhof": "AT",
    "klagenfurt hauptbahnhof": "AT", "klagenfurt hbf": "AT",
    "klagenfurt lend bahnhst": "AT", "klagenfurt west bahnhst": "AT",
    "klaus an der pyhrnbahn bahnhof": "AT", "kledering bahnhof": "AT",
    "kleinreifling bahnhof": "AT", "knittelfeld bahnhof": "AT",
    "kottingbrunn bahnhof": "AT", "kraubath an der mur bahnhof": "AT",
    "krems bahnhof": "AT", "kremsmunster bahnhof": "AT",
    "krieglach bahnhof": "AT", "kritzendorf bahnhof": "AT",
    "krumpendorf worthersee bahnhof": "AT", "kub bahnhof": "AT",
    "kuchl s bahn": "AT", "kufstein": "AT", "kufstein bahnhof": "AT",
    "kuhnsdorf klopeiner see bahnhof": "AT", "kumpfmuhl bahnhof": "AT",
    "kupfern bahnhst": "AT", "laa thaya bahnhof": "AT",
    "lacken im muhlkreis bahnhst": "AT", "ladendorf bahnhof": "AT",
    "lahrndorf b garsten bahnhof": "AT", "lambach oo bahnhof": "AT",
    "landeck zams bahnhof": "AT", "langen a a bahnhof": "AT",
    "langen am arlberg": "AT", "langenzersdorf bahnhof": "AT",
    "lanitzhohe 1": "AT", "lanitzhohe 2": "AT",
    "lanitzthal 1": "AT", "lanitzthal 2": "AT",
    "lauffen b bad ischl bahnhst": "AT", "lebring bahnhof": "AT",
    "ledenitzen bahnhst": "AT", "lehen altensam bahnhst": "AT",
    "leibnitz bahnhof": "AT", "lend bahnhof": "AT",
    "lendorf im drautal bahnhst": "AT", "leoben hauptbahnhof 1": "AT",
    "leoben hauptbahnhof 2": "AT", "leoben hauptbahnhof 3": "AT",
    "leobendorf burg kreuzenstein bhf": "AT", "leobersdorf bahnhof": "AT",
    "lienz bahnhof": "AT", "liezen bahnhof 1": "AT",
    "liezen bahnhof 2": "AT", "lind rosegg bahnhst": "AT",
    "angangs lind rosegg bahnhst": "AT",
    "linz donau franckstrae bahnhof": "AT",
    "linz donau hauptbahnhof 1": "AT", "linz donau hauptbahnhof 10": "AT",
    "linz donau hauptbahnhof 11": "AT", "linz donau hauptbahnhof 12": "AT",
    "linz donau hauptbahnhof 2": "AT", "linz donau hauptbahnhof 21": "AT",
    "linz donau hauptbahnhof 22": "AT", "linz donau hauptbahnhof 3": "AT",
    "linz donau hauptbahnhof 4": "AT", "linz donau hauptbahnhof 5": "AT",
    "linz donau hauptbahnhof 6": "AT", "linz donau hauptbahnhof 7": "AT",
    "linz donau hauptbahnhof 8": "AT", "linz donau hauptbahnhof 9": "AT",
    "linz donau urfahr bahnhof": "AT", "linz hbf": "AT",
    "lochau horbranz bahnhof": "AT", "loipersbach schattendorf bhf": "AT",
    "loosdorf melk bahnhof": "AT", "lungitz gusen bahnhof": "AT",
    "lustenau bahnhof": "AT", "maishofen saalbach bahnhof": "AT",
    "mallnitz obervellach bahnhof": "AT", "marchegg bahnhof": "AT",
    "marchegg tarifpunkt": "AT", "marchtrenk bahnhof": "AT",
    "maria anzbach bahnhof": "AT", "maria ellend bahnhof": "AT",
    "markt piesting bahnhof": "AT", "mattersburg bahnhof": "AT",
    "mauerkirchen bahnhof": "AT", "melk bahnhof": "AT",
    "micheldorf in oo bahnhof": "AT", "micheldorf in oo bahnhof a": "AT",
    "mistelbach bahnhof": "AT", "mittenwald bahnhof": "AT",
    "mittewald bahnhof": "AT", "modling bahnhof": "AT",
    "motz bahnhof": "AT", "munderfing bahnhof": "AT",
    "munster wiesing bahnhof": "AT", "mureck bahnhof": "AT",
    "murzzuschlag": "AT", "murzzuschlag bahnhof": "AT",
    "nettingsdorf bahnhof": "AT", "neufelden bahnhof": "AT",
    "neuhaus niederwaldkirchen bahnhst": "AT", "neuhofen krems bahnhof": "AT",
    "neulengbach bahnhof": "AT", "neumarkt bahnhof": "AT",
    "neumarkt kallham bahnhof": "AT", "neuratting bahnhst": "AT",
    "neusiedl am see bahnhof": "AT", "nickelsdorf bahnhof": "AT",
    "niederkreuzstetten bahnhof": "AT", "niklasdorf bahnhof 1": "AT",
    "nostlbach st marien bahnhst": "AT", "oberbrunn im innkreis bahnhst": "AT",
    "oberhofen i i bahnhof": "AT", "oberhofen zell am moos bahnhst": "AT",
    "obernberg altheim bahnhof": "AT", "oberndorf in tirol": "AT",
    "obersdorf bahnhof": "AT", "obertrattnach markt hofkirchen bahnhst": "AT",
    "obertraun dachsteinhohlen bahnhof": "AT", "otztal bahnhof": "AT",
    "pamhagen bahnhof": "AT", "parndorf bahnhof": "AT",
    "passering bahnhst": "AT", "paudorf bahnhof": "AT",
    "payerbach reichenau bahnhof": "AT", "perchtoldsdorf bahnhof": "AT",
    "perg bahnhof": "AT", "petzenkirchen bahnhof": "AT",
    "pichl bei schladming bahnhof": "AT", "pill vomperbach bahnhof": "AT",
    "pinsdorf bahnhst 1": "AT", "pitten bahnhof": "AT",
    "plank am kamp bahnhof": "AT", "pochlarn bahnhof": "AT",
    "pockau bahnhst": "AT", "poham bahnhof": "AT",
    "pondorf bahnhof": "AT", "portschach worthersee bahnhof": "AT",
    "pottendorf landegg bahnhof": "AT", "pottschach bahnhof": "AT",
    "pram haag bahnhof": "AT", "pregarten bahnhof": "AT",
    "pressbaum bahnhof": "AT", "pruggern bahnhof": "AT",
    "puch s bahn": "AT", "puch urstein fh": "AT",
    "puchberg am schneeberg bahnhof": "AT", "puchenau west bahnhst": "AT",
    "purkersdorf sanatorium bhf": "AT", "purkersdorf zentrum bahnhof": "AT",
    "pusarnitz bahnhof": "AT", "raasdorf bahnhof": "AT",
    "radstadt bahnhof": "AT", "rankweil bahnhof": "AT",
    "retz bahnhof": "AT", "ried im innkreis bahnhof": "AT",
    "riedau bahnhof": "AT", "rohr bad hall bahnhof": "AT",
    "rohrbach an der golsen bahnhof": "AT", "rohrbach vorau bahnhof": "AT",
    "rosenau am sonntagberg bahnhof": "AT", "rosenbach bahnhof": "AT",
    "rottenegg bahnhof": "AT", "rum bahnhof": "AT",
    "saalfelden": "AT", "saalfelden bahnhof": "AT",
    "sarasdorf bahnhof": "AT", "sattledt bahnhof": "AT",
    "sausenstein bahnhof": "AT", "scharding bahnhof 1": "AT",
    "scharnitz bahnhof": "AT", "scheibbs bahnhof": "AT",
    "scheiblingkirchen warth bhf": "AT", "schladming bahnhof": "AT",
    "schonborn mallebarn": "AT", "schrambach bahnhof": "AT",
    "schruns bahnhof": "AT", "schutzen am gebirge ruster strae bahnhof": "AT",
    "schwanenstadt bahnhof": "AT", "schwarzach bahnhof": "AT",
    "schwarzach st veit": "AT", "schwarzach st veit bahnhof": "AT",
    "schwaz bahnhof": "AT", "schwechat bahnhof": "AT",
    "schwertberg bahnhof": "AT", "sebersdorf bahnhof": "AT",
    "seefeld i t bahnhof": "AT", "seekirchen bahnhof": "AT",
    "seekirchen stadt s bahn": "AT", "selzthal bahnhof": "AT",
    "seyring bahnhof": "AT", "siebenbrunn leopoldsdorf bahnhof": "AT",
    "sigmundsherberg bahnhof": "AT", "sillian bahnhof": "AT",
    "simbach inn": "AT", "sonntagberg bahnhof": "AT",
    "spielfeld stra bahnhof": "AT", "spielfeld tarifpunkt grenze bahn": "AT",
    "spillern bahnhof": "AT", "spital am semmering bahnhof": "AT",
    "spittal millstatter see": "AT", "spittal millstatter see bahnhof": "AT",
    "st andra am zicksee bahnhof": "AT", "st andra wordern bahnhof": "AT",
    "st anton a a bahnhof": "AT", "st georgen am steinfelde bahnhof": "AT",
    "st georgen gusen ort bahnhst": "AT", "st georgen ob judenburg bahnhof": "AT",
    "st johann i t bahnhof": "AT", "st johann i t grieswirt bahnhof": "AT",
    "st johann im pongau": "AT", "st marein st lorenzen bahnhof": "AT",
    "st michael bahnhof": "AT", "st michael ob bleiburg bahnhst": "AT",
    "st nikola struden bahnhst": "AT", "st valentin bahnhof": "AT",
    "st veit an der glan bahnhof": "AT", "stainach irdning bahnhof": "AT",
    "stans bahnhof": "AT", "stein an der enns bahnhof": "AT",
    "steinach a br bahnhof": "AT", "steinkogel bahnhst": "AT",
    "steyregg bahnhof": "AT", "stiefern bahnhof": "AT",
    "stockerau bahnhof": "AT", "strawalchen bahnhof": "AT",
    "summerau bahnhof": "AT", "tauchendorf haidensee bahnhst": "AT",
    "telfs pfaffenhofen bahnhof": "AT", "ternberg bahnhof": "AT",
    "tiffen bahnhst": "AT", "tp bleiburg staatsgrenze": "AT",
    "tp gmund lainsitzbrucke schiene": "AT", "tp summerau gr": "AT",
    "traisen bahnhof": "AT", "traisen markt bahnhof": "AT",
    "traiskirchen aspangbahn": "AT", "traismauer bahnhof": "AT",
    "traun oo bahnhof": "AT", "treibach althofen": "AT",
    "trieben bahnhof": "AT", "trumau bahnhof": "AT",
    "tulln bahnhof": "AT", "tulln stadt bahnhof": "AT",
    "tullnerbach pressbaum bahnhof": "AT", "tullnerfeld bahnhof": "AT",
    "ulmerfeld hausmening bahnhof": "AT", "ulrichskirchen bahnhof": "AT",
    "unterhoflein bahnhof": "AT", "unterkritzendorf bahnhof": "AT",
    "unterpurkersdorf bahnhof": "AT", "unterretzbach bahnhof": "AT",
    "untertullnerbach bahnhof": "AT", "unzmarkt bahnhof 1": "AT",
    "unzmarkt bahnhof 3": "AT", "urschendorf bahnhof": "AT",
    "velden am worther see": "AT", "velden worthersee bahnhof": "AT",
    "villach hauptbahnhof": "AT", "villach hbf": "AT",
    "villach landskron bahnhst": "AT", "villach seebach bahnhst": "AT",
    "villach westbahnhof": "AT", "vocklabruck bahnhof": "AT",
    "vocklabruck bahnhof a": "AT", "voitsdorf bahnhst": "AT",
    "vols bahnhof": "AT", "waidhofen ybbs bahnhof": "AT",
    "waldegg bahnhof": "AT", "wampersdorf bahnhof": "AT",
    "wartberg krems bahnhof": "AT", "weienbach st gallen bahnhof": "AT",
    "weissenbach neuhaus bahnhof": "AT", "weitlanbrunn bahnhof": "AT",
    "weizelsdorf bahnhof": "AT", "wels hauptbahnhof": "AT",
    "wels hbf": "AT", "werndorf bahnhof": "AT",
    "wernstein inn bahnhof": "AT", "weyer enns bahnhof": "AT",
    "wiederndorf aich bahnhst": "AT", "wieselburg bahnhof": "AT",
    "wiesenfeld schwarzenbach bf": "AT", "wildon bahnhof": "AT",
    "wildungsmauer bahnhof": "AT", "windischgarsten bahnhof": "AT",
    "winzendorf bahnhof": "AT", "wolfsberg bahnhof": "AT",
    "wolfsthal": "AT", "wolfurt bahnhof": "AT",
    "wolkersdorf bahnhof": "AT", "worgl hauptbahnhof": "AT",
    "worgl hbf": "AT", "worschach schwefelbad bahnhof": "AT",
    "wulkaprodersdorf bahnhof": "AT", "zell am see bahnhof": "AT",
    "zeltweg bahnhof": "AT",

    # SWITZERLAND
    "arth goldau": "CH", "bern": "CH", "chur": "CH",
    "interlaken ost": "CH", "lenzburg": "CH", "sargans": "CH",
    "schaffhausen": "CH", "st margrethen": "CH",
    "st margrethen bahnhof": "CH", "st margrethen sg": "CH",
    "basel sbb": "CH", "basel bad bf": "CH",

    # ITALY
    "arezzo": "IT", "bologna centrale": "IT",
    "bologna stazione di bologna centrale": "IT", "bolzano bozen": "IT",
    "bolzano stazione di bolzano": "IT", "brennero brenner": "IT",
    "brennero stazione di brennero": "IT", "bressanone brixen": "IT",
    "brixen i th bahnhof": "IT", "brunico stazione di brunico": "IT",
    "chiusi chianciano terme": "IT", "desenzano del garda sirmione": "IT",
    "fortezza franzensfeste": "IT", "fortezza stazione di fortezza": "IT",
    "milano centrale": "IT", "peschiera del garda": "IT",
    "roma stazione di roma tiburtina": "IT", "roma termini": "IT",
    "san candido stazione di san candido": "IT",
    "san lorenzo di sebato stazione di san lorenzo": "IT",
    "tarvisio citta boscoverde": "IT", "treviso centrale": "IT",
    "trieste centrale 1": "IT", "udine stazione": "IT",
    "valdaora stazione di valdaora anterselva": "IT",
    "venezia mestre": "IT", "venezia santa lucia": "IT",
    "verona porta nuova": "IT", "orvieto": "IT", "rimini": "IT",
    "como": "IT", "chiasso": "IT", "lugano": "IT",
    "bellinzona": "IT", "domodossola": "IT", "simplontunnel": "IT",

    # FRANCE
    "brest central": "FR", "brest gr": "FR", "calais ville": "FR",
    "paris nord": "FR", "paris gare du nord": "FR", "paris est": "FR",
    "lyon": "FR", "marseille": "FR", "nice": "FR",
    "strasbourg": "FR", "lille europe": "FR",

    # BELGIUM
    "bruxelles nord": "BE", "bruxelles midi": "BE", "brussels midi": "BE",

    # NETHERLANDS
    "amersfoort": "NL", "amersfoort centraal": "NL",
    "arnhem centraal": "NL", "deventer": "NL", "eindhoven centraal": "NL",
    "groningen": "NL", "hilversum": "NL", "s hertogenbosch": "NL",
    "utrecht centraal": "NL", "venlo": "NL",
    "amsterdam centraal": "NL", "rotterdam centraal": "NL",

    # DENMARK
    "aarhus": "DK", "fredericia st": "DK", "koebenhavn h": "DK",
    "koebenhavns lufthavn st": "DK", "lunderskov st": "DK",
    "nykoebing f st": "DK", "oesterport st": "DK", "padborg st": "DK",
    "ringsted st": "DK", "roedby": "DK", "roedekro st": "DK",
    "roskilde st": "DK", "slagelse st": "DK", "soroe st": "DK",
    "tinglev st": "DK", "vamdrup st": "DK",

    # CZECH REPUBLIC
    "breclav": "CZ", "brno hl n": "CZ", "decin hl n": "CZ",
    "decin hlavni nadrazi": "CZ", "ostrava hl n": "CZ",
    "praha hl n": "CZ", "praha hlavni nadrazi": "CZ",
    "praha holesovice": "CZ", "praha smichov": "CZ",
    "bohumin": "CZ", "bohumin gr": "CZ",

    # POLAND
    "bochnia": "PL", "bydgoszcz glowna": "PL", "chelm": "PL",
    "chop": "PL", "dobiegniew": "PL", "gdansk wrzeszcz": "PL",
    "gdynia glowna": "PL", "gliwice": "PL", "glogow": "PL",
    "inowroclaw": "PL", "inowroclaw rabinek": "PL",
    "katowice stacja kolejowa": "PL", "kedzierzyn kozle": "PL",
    "kostrzyn": "PL", "krakow glowny": "PL", "krakow plaszow": "PL",
    "kutno": "PL", "mieszkowice": "PL", "opole glowne": "PL",
    "pilawa": "PL", "poznan gl": "PL", "poznan glowny": "PL",
    "przemysl gl": "PL", "przemysl glowny": "PL", "raciborz": "PL",
    "rawicz": "PL", "rzepin": "PL", "rybnik pl": "PL",
    "sosnowiec glowny": "PL", "swinoujscie": "PL", "trzebinia": "PL",
    "warszawa wschodnia": "PL", "warszawa zachodnia": "PL",
    "wroclaw glowny": "PL", "zabrze": "PL", "debica": "PL",
    "terespol": "PL",

    # HUNGARY
    "budapest nyugati": "HU", "fertoszentmiklos palyaudvar": "HU",
    "gyor palyaudvar": "HU", "hegyeshalom": "HU",
    "hegyeshalom palyaudvar": "HU", "mosonmagyarovar": "HU",
    "nyiregyhaza": "HU", "sopron palyaudvar": "HU",
    "szentgotthard palyaudvar": "HU", "tatabanya": "HU",
    "tp zahony grenze": "HU",

    # SLOVAKIA
    "bratislava hl st": "SK", "bratislava hlavna stanica": "SK",
    "bratislava petrzalka stanica": "SK", "kosice": "SK",
    "sturovo": "SK", "devinska nova ves": "SK",

    # SLOVENIA
    "borovnica": "SI", "kranj": "SI", "Ljubljana": "SI",
    "Ljubljana zelezniska postaja": "SI",
    "maribor zelezniska postaja": "SI",
    "sentilj zelezniska postaja": "SI", "sevnica": "SI",
    "zidani most": "SI",

    # CROATIA
    "rijeka": "HR", "split zeljeznicki kolodvor": "HR",
    "zagreb glavni kolodvor": "HR",

    # ROMANIA
    "bucuresti nord gara a": "RO", "dobova gr": "RO",

    # UKRAINE
    "kyjiw passaschyrskyj": "UA",

    # BELARUS
    "minsk passajirskii": "BY",

    # RUSSIA
    "moskva belorusskaja": "RU",

    # LUXEMBOURG
    "luxembourg": "LU",

    # GB
    "st pancras international": "GB", "london st pancras": "GB",
}

# ----------------------------
# FUNCTION: Normalize station names
# ----------------------------
def normalize(name):
    if not isinstance(name, str) or name.strip() == "":
        return ""
    name = name.lower().strip()
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

# ----------------------------
# FUNCTION: Simplify station for dashboard
# ----------------------------
def simplify_station_advanced(name):
    if not isinstance(name, str) or name.strip() == "":
        return ""
    name = name.strip().lower()
    remove_terms = ["bf", "hbf", "tief", "bad", "gare", "routiere"]
    pattern = r'\b(' + '|'.join(remove_terms) + r')\b'
    name = re.sub(pattern, '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    small_words = ["de","du","des","la","le","les","d'","l'","à"]
    words = []
    for w in name.split():
        if w in small_words:
            words.append(w)
        else:
            words.append(w.capitalize())
    return ' '.join(words)

# ----------------------------
# FUNCTION: Fix route name if empty or same as origin
# ----------------------------
def fix_route_name(row):
    if pd.isna(row['route_name']) or row['route_name'].strip() == "" or row['route_name'].strip() == row['origin'].strip():
        return f"{row['origin']} → {row['destination']}"
    else:
        return row['route_name']

# ----------------------------
# FUNCTION: Resolve country
# ----------------------------
def resolve_country(station_normalized, folder_country):
    if folder_country is not None:
        return folder_country
    for key, code in STATION_COUNTRY_MAP.items():
        if key in station_normalized:
            return code
    return None

# ----------------------------
# FUNCTION: Extract routes from a GTFS folder
# ----------------------------
def extract_routes(folder):
    required_files = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
    missing = [f for f in required_files if not os.path.exists(os.path.join(folder, f))]
    if missing:
        print(f"⚠ Skipping {folder}, missing files: {missing}")
        return pd.DataFrame()

    stops = pd.read_csv(os.path.join(folder, "stops.txt"), dtype=str, low_memory=False)
    stop_times = pd.read_csv(os.path.join(folder, "stop_times.txt"), dtype=str, low_memory=False)
    trips = pd.read_csv(os.path.join(folder, "trips.txt"), dtype=str, low_memory=False)
    routes = pd.read_csv(os.path.join(folder, "routes.txt"), dtype=str, low_memory=False)

    stop_times = stop_times.sort_values(["trip_id", "stop_sequence"])
    first_stops = stop_times.groupby("trip_id").first().reset_index()
    last_stops = stop_times.groupby("trip_id").last().reset_index()

    first_stops = first_stops.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')
    last_stops = last_stops.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    df = trips.merge(routes[['route_id', 'route_short_name']], on='route_id', how='left')
    df = df.merge(first_stops[['trip_id', 'stop_name']], on='trip_id')
    df = df.merge(last_stops[['trip_id', 'stop_name']], on='trip_id', suffixes=('_origin', '_destination'))

    df = df[['route_short_name', 'stop_name_origin', 'stop_name_destination']]
    df = df.dropna(subset=['stop_name_origin', 'stop_name_destination'])
    df = df[df['stop_name_origin'].str.strip() != ""]
    df = df[df['stop_name_destination'].str.strip() != ""]

    df['origin'] = df['stop_name_origin'].apply(normalize)
    df['destination'] = df['stop_name_destination'].apply(normalize)
    df['route_name'] = df['route_short_name']
    df['service_type'] = "night"

    df['route_name'] = df.apply(fix_route_name, axis=1)

    df['pair'] = df.apply(lambda r: tuple(sorted([r['origin'], r['destination']])), axis=1)
    df = df.drop_duplicates(subset='pair')

    # Country mapping
    folder_country = FOLDER_COUNTRY_MAP.get(folder)
    df['origin_country'] = df['origin'].apply(lambda o: resolve_country(o, folder_country))
    df['destination_country'] = df['destination'].apply(lambda d: resolve_country(d, folder_country))

    unresolved = df[df['origin_country'].isna() | df['destination_country'].isna()]
    if not unresolved.empty:
        print(f"   ⚠ {len(unresolved)} routes with unresolved country:")
        print(unresolved[['origin', 'destination', 'origin_country', 'destination_country']].to_string())

    df['origin_simple'] = df['origin'].apply(simplify_station_advanced)
    df['destination_simple'] = df['destination'].apply(simplify_station_advanced)
    df['route_name_simple'] = df['origin_simple'] + " → " + df['destination_simple']

    return df[['route_name', 'origin', 'destination', 'service_type', 'route_name_simple', 'origin_country', 'destination_country']]

# ----------------------------
# MAIN SCRIPT
# ----------------------------
all_routes = []

for folder in GTFS_NIGHT_FOLDERS:
    print(f"\n➡ Processing folder: {folder}")
    df_routes = extract_routes(folder)
    if not df_routes.empty:
        print(f"   Extracted {len(df_routes)} routes")
        all_routes.append(df_routes)
    else:
        print("   No routes extracted")

if all_routes:
    master_night = pd.concat(all_routes).drop_duplicates(subset=['origin', 'destination', 'route_name'])
    master_night.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Master night routes CSV created: {OUTPUT_FILE}")
    print(f"Total routes: {len(master_night)}")
    print(f"\nCountry breakdown (origin):")
    print(master_night['origin_country'].value_counts(dropna=False).to_string())
else:
    print("❌ No night routes were extracted from any folder")
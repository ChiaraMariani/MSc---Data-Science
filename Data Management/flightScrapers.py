from utils import ampmTo24h, getDatetime, getMostSimilarIATA
from flightCleaners import cleanFlightFromMXP, cleanFlightFromBOG, cleanFlightFromRPLL, \
    cleanFlightFromATH
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests


def getMXPHeaders():
    """
    Get headers to make requests to Malpensa Airport
    :return: a dict containing all the headers
    """

    return {
        'Host': 'apiextra.seamilano.eu',
        'Origin': 'https://www.milanomalpensa-airport.com',
        'Referer': 'https://www.milanomalpensa-airport.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'keyId': '6bc034ea-ae66-40ce-891e-3dccf63cb2eb',
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }


def getMPXFlights(iatas):
    """
    Get all flights related to Malpensa until yesterday
    :return: all the flights from Malpensa Airport
    """

    flights = []

    # get the headers to make the request to Malpensa
    headers = getMXPHeaders()

    # yesterday's date for making the request
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    # request to Malpensa site
    malp = requests.get(
        f'https://apiextra.seamilano.eu/ols-flights/v1/en/operative/flights/lists?movementType=D&dateTo={yesterday}+23%3A59&loadingType=P&airportReferenceIata=mxp&mfFlightType=P',
        headers=headers)

    # extraction of data from the response
    malpJson = malp.json()
    malpJson = malpJson["data"]

    # cleaning flights keeping only the information we need
    for flight in malpJson:
        if "malpensa" in flight["routing"][0]["airportDescription"].lower():
            flights.append(cleanFlightFromMXP(flight, iatas))
    return flights


def getNRTFlights(iatas):
    """
    Get all flights departed or canceled of today related to Narita
    :return: all the flights from Narita Airport
    """

    flights = []

    # today's date to do the request
    today = datetime.now().strftime('%Y%m%d')

    timeToSearch = 6

    while timeToSearch <= 21:
        if timeToSearch < 10:
            timeToSearch = "0" + str(timeToSearch)
        # Narita airport site request
        narita = requests.get(
            f"https://www.narita-airport.jp/en/api/flight/?DepArr=D&flightDate={today}&ontime={timeToSearch}00")

        # response in the form of a string
        narita = narita.text
        narita = BeautifulSoup(narita, 'html.parser')

        # scraping
        rawFlights = narita.find_all("tr")
        for flight in rawFlights:
            clean = {"airportDep": "NRT", "date": today[0:4] + "-" + today[4:6] + "-" + today[6:]}
            schedDep = flight.find_all("td", {"class": "t002-daily__ontime"})
            actDep = flight.find_all("td", {"class": "t002-daily__updtime"})
            status = flight.find_all("td", {"class": "t002-daily__status"})
            dest = flight.find_all("a")
            number = flight.find_all("span")
            if len(schedDep) > 0:
                schedDep = schedDep[0].string
                hour, minutes = ampmTo24h("am" in schedDep, int(schedDep.split(":")[0]),
                                          int(schedDep.split(":")[1][0:2]))
                schedTime = str(hour) + ":" + str(minutes)
                clean["scheduledDep"] = getDatetime(clean["date"], schedTime, "Asia/Tokyo")
            if len(actDep) > 0:
                actDep = actDep[0].string
                if actDep is None:
                    if "scheduledDep" in clean:
                        clean["actualDep"] = clean["scheduledDep"]
                else:
                    actDep = actDep[1:-1]
                    hour, minutes = ampmTo24h("am" in actDep, int(actDep.split(":")[0]), int(actDep.split(":")[1][0:2]))
                    actTime = str(hour) + ":" + str(minutes)
                    clean["actualDep"] = getDatetime(clean["date"], actTime, "Asia/Tokyo")
            if len(dest) > 0:
                dest = dest[0].string
                if dest is not None:
                    airportName = dest.replace(" ", "").replace("\n", "").replace("\t", "").replace("\r", "")
                    clean["airportArr"] = getMostSimilarIATA(iatas, airportName)
            if len(status) > 0:
                clean["status"] = status[0].string
            if len(number) > 1:
                number = number[1].string
                clean["number"] = str(number).replace(" ", "")
            if len(clean) == 6 and clean["status"] is not None and (
                    clean["status"].lower() == "departed" or clean["status"].lower() == "cancelled"):
                flights.append(clean)
        timeToSearch = int(timeToSearch) + 3
    return flights


def getRKVFlights(iatas):
    """
    Get all flights departed or canceled of today related to Reykjavík
    :return: all the flights from Reykjavík Airport
    """

    flights = []

    # Reykjavik airport site request
    rkv = requests.get("https://www.isavia.is/en/reykjavik-airport/flight-information/departures?dep=0")

    # response in the form of a string
    rkv = rkv.text
    rkv = BeautifulSoup(rkv, 'html.parser')

    # scraping
    rawFlights = rkv.find_all("tr", {"class": "schedule-items-entry"})
    for flight in rawFlights:
        clean = {"airportDep": "RKV", "date": datetime.now().strftime('%Y-%m-%d')}
        cutoff = flight.find_all("span", {"class": "cutoff"})
        tds = flight.find_all("td")
        if len(tds) == 6:
            schedTime = tds[0].string
            clean["scheduledDep"] = getDatetime(clean["date"], schedTime, "Atlantic/Reykjavik")
            clean["number"] = tds[2].string.replace(" ", "").replace("\n", "").replace("\t", "").replace("\r", "")
        if len(cutoff) == 3:
            clean["airportArr"] = getMostSimilarIATA(iatas, cutoff[0].string.upper())
            actDep = cutoff[2].string
            if "departed" in actDep.lower():
                actTime = actDep.split(" ")[1]
                clean["actualDep"] = getDatetime(clean["date"], actTime, "Atlantic/Reykjavik")
                clean["status"] = "DEPARTED"
            elif "cancelled" in actDep.lower():
                clean["actualDep"] = None
                clean["status"] = "CANCELLED"
        if "status" in clean and (clean["status"].lower() == "departed" or clean["status"].lower() == "cancelled"):
            flights.append(clean)
    return flights


def getMIAFlights(iatas):
    """
    Get all flights departed or canceled of today related to Miami
    :return: all the flights from Miami Airport
    """

    flights = []

    # Miami airport site request
    mia = requests.get(
        "https://webvids.miami-airport.com/webfids/webfids?action=searchResults&who=Departures&flightnumberSelect=-%20All%20Flights%20-&airlineSelect=-%20All%20Airlines%20-&citySelect=-%20All%20Cities%20-&startTimeSelect=-%20Start%20Time%20-&endTimeSelect=-%20End%20Time%20-")

    # response in the form of a string
    mia = mia.text
    mia = BeautifulSoup(mia, 'html.parser')

    # scraping
    rawFlights = mia.find_all("tr", {"class": "flightData1"})
    for flight in rawFlights:
        clean = {"airportDep": "MIA"}
        tds = flight.find_all("td")
        number = tds[0].get("id")
        splitted = number.split(" ")
        if len(splitted) > 1:
            number = str(splitted[0][0]) + str(splitted[1][0])
        else:
            number = splitted[0][0:2].upper()
        number += tds[1].get("id")
        clean["number"] = number.replace(" ", "")
        clean["airportArr"] = getMostSimilarIATA(iatas, tds[2].get("id"))
        date = tds[3].string.split("\xa0")[1]
        date = f"20{date.split('-')[2]}-{date.split('-')[0]}-{date.split('-')[1]}"
        clean["date"] = date
        hour = int(tds[3].string.split("\xa0")[0].split(":")[0].replace(" ", ""))
        minutes = int(tds[3].string.split("\xa0")[0].split(":")[1][:-1])
        am = tds[3].string.split("\xa0")[0].split(":")[1][-1] == "A"
        hour, minutes = ampmTo24h(am, hour, minutes)
        schedTime = str(hour) + ":" + str(minutes)
        clean["scheduledDep"] = getDatetime(clean["date"], schedTime, "America/New_York")
        status = tds[4].font.string.replace("\t", "").replace("\n", "").replace("\r", "")[1:-1].split(" ")
        clean["status"] = status[0].upper()
        if "departed" in clean["status"].lower():
            hour, minutes = ampmTo24h("A" in status[1], int(status[1].split(":")[0]), int(status[1].split(":")[1][0:2]))
            actTime = str(hour) + ":" + str(minutes)
            clean["actualDep"] = getDatetime(clean["date"], actTime, "America/New_York")
        elif "cancelled" in clean["status"].lower():
            clean["actualDep"] = None
        if "status" in clean and (clean["status"].lower() == "departed" or clean["status"].lower() == "cancelled"):
            flights.append(clean)
    return flights


def getBOGFlights(iatas):
    """
    Get all flights departed or canceled of today related to El Dorado
    :return: all the flights from El Dorado Airport
    """

    flights = []

    # El Dorado airport site request
    bog = requests.get(f'https://api.eldorado.aero/api/flights')

    # extraction of data from the response
    bog = bog.json()
    bog = bog["data"]["departures"]
    for flight in bog:
        clean = cleanFlightFromBOG(flight, iatas)
        if "departed" in clean["status"].lower() or "cancelled" in clean["status"].lower():
            flights.append(clean)

    return flights


def getRPLLFlights(iatas):
    """
    Get all flights departed or canceled of today related to Manila
    :return: all the flights from Manila Airport
    """

    flights = []

    # Manila airport site request
    rpll = requests.get("https://miaagov.online/flight-dep.json")

    # extraction of data from the response
    rpll = rpll.json()
    rpll = rpll["data"]
    for flight in rpll:
        clean = cleanFlightFromRPLL(flight, iatas)
        if "status" in clean and ("departed" in clean["status"].lower() or "cancelled" in clean["status"].lower()):
            flights.append(clean)
    return flights


def getATHFlights(iatas):
    """
    Get all flights departed or canceled of today related to Athens
    :return: all the flights from Athens Airport
    """

    flights = []

    # Athens airport site request
    ath = requests.get(
        "https://www.aia.gr/handlers/rtfiV2.ashx?action=getRtfiJson&cultureId=50&bringRecent=1&timeStampFormat=dd-MM-yyyy HH%3Amm&allRecs=1&airportId=&airlineId=&flightNo=")

    # extraction of data from the response
    ath = ath.json()
    ath = ath["departures"]
    for timeZone in ath:
        flightsForTimeZone = timeZone["data"]
        for flight in flightsForTimeZone:
            clean = cleanFlightFromATH(flight, iatas)
            if "status" in clean and ("departed" in clean["status"].lower() or "cancelled" in clean["status"].lower()):
                flights.append(clean)
    return flights

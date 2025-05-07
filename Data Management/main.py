from typing import Callable

import pandas
import requests
from datetime import datetime

import PyPDF2

from DBConnection import DBConnection
from flightScrapers import getMPXFlights, getNRTFlights, getRKVFlights, getBOGFlights, getMIAFlights, getRPLLFlights, \
    getATHFlights
from utils import delaysCorrelations, printMeasures, flightsListToDataframe, splitDatetime, reportToCsv


def getAndInsertFlights(conn: DBConnection, isServer: bool, iatas) -> None:
    """
    Get all the flights from each airport and load them in MongoDB
    :param conn: connection object
    :param isServer: boolean that track if the code is running on the server or not
    :return: None
    """

    flights = []

    # flights of Malpensa
    print("Malpensa..")
    flights += getMPXFlights(iatas)
    # flights of Narita
    print("Narita..")
    flights += getNRTFlights(iatas)
    # Reykjavik flights
    print("Rejkyavik..")
    flights += getRKVFlights(iatas)
    # El Dorado flights
    print("Bogotà..")
    flights += getBOGFlights(iatas)
    # Miami flights
    print("Miami..")
    flights += getMIAFlights(iatas)
    # Manila flights
    print("Manila..")
    flights += getRPLLFlights(iatas)
    # Atene flights
    print("Atene..")
    flights += getATHFlights(iatas)

    print("\nAdding flights...")
    for flight in flights:
        conn.insertOneFlight(flight)
    print("Added all flights!\n")

    if isServer:
        f = open("added.out", "a")
        f.write(f"{datetime.now().strftime('%m/%d/%Y, %H:%M:%S')} -> {len(flights)}\n")
        f.close()


def getAirportsCoordinates(flights: list) -> dict:
    """
    Get latitude and longitude for each airport using a dedicated API
    :param flights: list of all the flights
    :return: A dict having the name of the airport as a key and latitude and longitude as value
    """
    # getAirportsCoordinates -> function to obtain latitude and longitude from API for each airport
    airports = {}

    for flight in flights:
        if flight["airportDep"] not in airports:
            airport = flight["airportDep"].replace(" ", "%20")
            res = requests.get(f"https://photon.komoot.io/api/?lang=en&limit=5&q={airport}")
            res = res.json()
            latitude = res["features"][0]["geometry"]["coordinates"][1]
            longitude = res["features"][0]["geometry"]["coordinates"][0]
            airports[flight["airportDep"]] = {"latitude": latitude, "longitude": longitude}
    return airports


def dayForEachAirport(airports: dict, flights: list) -> dict:
    """
    Function that create a new dict based on the one with latitude and longitude,
    in which are stored all the meteo information as a list
    :param airports:
    :param flights:
    :return: A dict having the name of the airport as a key and a dict as value,
    the inner dict has the date as key and the meteo information as a value
    """
    airportsNew = {}
    for flight in flights:
        if flight["airportDep"] not in airportsNew:
            airportsNew[flight["airportDep"]] = {}
        date, schedDep = splitDatetime(flight["scheduledDep"])
        if date not in airportsNew[flight["airportDep"]]:
            airportsNew[flight["airportDep"]][date] = {}

    for airportName, dates in airportsNew.items():
        for date in dates:
            res = requests.get(
                f"https://archive-api.open-meteo.com/v1/archive?latitude={airports[airportName]['latitude']}&longitude={airports[airportName]['longitude']}&start_date={date}&end_date={date}&hourly=precipitation,cloud_cover,wind_speed_10m,wind_speed_100m&timezone=GMT")
            airportsNew[airportName][date] = res.json()

    return airportsNew


def addMeteoToFlights(conn: DBConnection, flights: list) -> None:
    """
    Add weather conditions for flight in the database
    :param conn: connection object
    :param flights: list of all the flights
    :return: None
    """

    airports = getAirportsCoordinates(flights)
    airports = dayForEachAirport(airports, flights)

    for flight in flights:
        stats = ["precipitation", "cloud_cover", "wind_speed_10m", "wind_speed_100m"]
        found = 0

        for statName in stats:
            if statName in flight:
                found += 1

        if found != len(stats):
            airport = flight["airportDep"]
            date, schedDep = splitDatetime(flight["scheduledDep"])
            hour = schedDep.split(":")[0] + ":00"
            percentageHour = int(schedDep.split(":")[1]) / 60

            statsObj = airports[airport][date]

            index = -1
            for i, time in enumerate(statsObj["hourly"]["time"]):
                if hour in time:
                    index = i

            for stat in stats:
                if index < len(statsObj["hourly"][stat]) - 1:
                    left = statsObj["hourly"][stat][index]
                    right = statsObj["hourly"][stat][index + 1]
                    if left is not None and right is not None:
                        value = round(left * (1 - percentageHour) + right * percentageHour, 4)
                        flight[stat] = value
                elif statsObj["hourly"][stat][index] is not None:
                    flight[stat] = statsObj["hourly"][stat][index]

            conn.updateFlight(flight)


def readIATApdf() -> list:
    """
    Read the pdf containing the IATA acronyms
    :return: A list containing all the IATAs in the pdf file as dict
    """
    file = open("IATA.pdf", "rb")
    reader = PyPDF2.PdfReader(file)
    airports = []
    for page in range(len(reader.pages)):
        pageObj = reader.pages[page]
        airports += pageObj.extract_text().split("\n")
    file.close()
    # rimozione primo e ultimo elemento
    airports = airports[1: -1]
    iatas = []
    for airport in airports:
        splitted = airport.split(" – ")
        acronym = splitted[0].replace(" ", "")
        name = ""
        for el in splitted[1:]:
            name += el + " "
        name = name.replace(" ", "").replace(",", ", ")
        if name != "" and acronym != "":
            iatas.append({"acronym": acronym, "name": name})
    return iatas


def insertIATA(conn, iatas) -> None:
    """
    Insert all IATAs in the dedicated collection
    :param conn: connection object
    :param iatas: list of IATA objects to insert
    :return: None
    """
    for iata in iatas:
        conn.insertOneIATA(iata)


def handleQuery(queryFunction: Callable, airportNames: list, report: dict, key: str) -> dict:
    """
    Execute the query passed as function and return the prettified response in a dict
    :param queryFunction: query to execute
    :param airportNames: a list of airport names in the database
    :param report: a dict containing the previous query responses
    :param key: a string explaining the query
    :return: A dict containing the actual query response
    """
    toPrint = {}
    response = queryFunction()
    for el in response:
        keys = list(el.keys())
        keys.remove("airport")
        value = el[keys[0]]
        if value is not None:
            value = round(value, 3)
        toPrint[el["airport"]] = value
    remaining = list(set(airportNames) - set(toPrint.keys()))
    for el in remaining:
        toPrint[el] = 0
    for el in toPrint:
        print(el + ": " + str(toPrint[el]))
    report[key] = toPrint
    return report


def dataQuality(dfFlights: pandas.DataFrame, iatas: list) -> dict:
    """
    Execute quality measures over the dataset
    :param dfFlights: a dataframe containing flights details
    :param iatas: a list containing all the iata acronyms
    :return: a dict containing the quality dimensions
    """
    qualities = {}
    matches = dfFlights.loc[dfFlights['actualDep'].notnull()]
    qualities["completeness"] = len(list(matches.index)) / len(list(dfFlights.index))
    acronyms = []
    for iata in iatas:
        acronyms.append(iata["acronym"])
    matches = dfFlights.loc[dfFlights['airportDep'].isin(acronyms)]
    qualities["consistency"] = len(list(matches.index)) / len(list(dfFlights.index))
    return qualities


def analysisAndQuery(conn: DBConnection, dfFlights: pandas.DataFrame) -> None:
    """
    Handle queries, create and print a dict to a csv
    :param conn: connection object
    :param dfFlights: a dataframe containing flights details
    :return: None
    """
    airportNames = conn.getDistinctAirportDepNames()

    print("Analysis and queries: \n")

    report = {}

    print("Number of flights grouped by airport")
    report = handleQuery(conn.flightsGroupedByAirport, airportNames, report, "countFlights")
    print()

    print("General correlation between delays and weather measures")
    measures = delaysCorrelations(dfFlights[
                                      ["delay", "precipitation", "cloud_cover", "wind_speed_10m", "wind_speed_100m"]])
    printMeasures(measures)
    print()

    print("Correlation between delays and weather measures grouped by airport")
    for airport in airportNames:
        print(airport)
        subset = dfFlights.loc[dfFlights['airportDep'] == airport]
        measures = delaysCorrelations(
            subset[["delay", "precipitation", "cloud_cover", "wind_speed_10m", "wind_speed_100m"]])
        printMeasures(measures)
        print()

    print("Mean of the wind speed at 100m grouped by airport [kilometres per hour]")
    report = handleQuery(conn.meanWindSpeed100mGroupedByAirport, airportNames, report, "meanWind")
    print()

    print("Mean of the delays grouped by airport [minutes]")
    report = handleQuery(conn.meanDelaysGroupedByAirport, airportNames, report, "meanDelays")
    print()

    print("Mean of the delays grouped by airport filtering only wind speed 100m > airport mean respectively [minutes]")
    report = handleQuery(conn.meanDelaysGroupedByAirportFilteredOnWind100mGt, airportNames, report, "meanWindDelaysGt")
    print()

    print("Mean of the delays grouped by airport filtering only wind speed 100m < airport mean respectively [minutes]")
    report = handleQuery(conn.meanDelaysGroupedByAirportFilteredOnWind100mLt, airportNames, report, "meanWindDelaysLte")
    print()

    print("Mean of the precipitation grouped by airport [millimetres]")
    report = handleQuery(conn.meanPrecipitationGroupedByAirport, airportNames, report, "meanPrecipitation")
    print()

    print("Mean of the delays grouped by airport filtering only precipitation > airport mean respectively [minutes]")
    report = handleQuery(conn.meanDelaysGroupedByAirportFilteredOnPrecipitationGt, airportNames, report,
                         "meanPrecDelaysGt")
    print()

    print("Mean of the delays grouped by airport filtering only precipitation < airport mean respectively [minutes]")
    report = handleQuery(conn.meanDelaysGroupedByAirportFilteredOnPrecipitationLt, airportNames, report,
                         "meanPrecDelaysLte")
    print()

    report["windPercentageIncrese"] = percentageIncrease(report["meanWindDelaysLte"], report["meanWindDelaysGt"])
    report["precPercentageIncrese"] = percentageIncrease(report["meanPrecDelaysLte"], report["meanPrecDelaysGt"])

    reportToCsv(report, fileName="reportQuery")


def percentageIncrease(left: dict, right: dict) -> dict:
    """
    Calculate the increase from left dict to right dict
    :param left: a dict containing starting values
    :param right: a dict containing endind values
    :return: a dict containing the percentage increases
    """
    percentage = {}
    for el in left:
        percentage[el] = round(((right[el] / left[el]) - 1) * 100, 3)
    return percentage


def main() -> None:
    # variable defining the device
    isServer = False

    # connection to the database
    print("Connecting to db...")
    conn = DBConnection()
    print("Connected to db!\n")

    if isServer:
        iatas = readIATApdf()
        insertIATA(conn, iatas)
        getAndInsertFlights(conn, isServer, iatas)
        flights = conn.getAllFlights()
        addMeteoToFlights(conn, flights)

    print("Fetching all iatas...")
    iatas = conn.getAllIATA()
    print("IATAS fetched!\n")

    print("Fetching all flights...")
    flights = conn.getAllFlights()
    dfFlights = flightsListToDataframe(flights)
    print("Flights fetched!\n")

    qualities = dataQuality(dfFlights, iatas)
    for quality in qualities:
        print(quality + ": " + str(round(qualities[quality], 5)))
    print()

    analysisAndQuery(conn, dfFlights)


if __name__ == '__main__':
    main()

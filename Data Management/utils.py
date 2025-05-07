import csv
import json
from datetime import datetime

import pandas
from bson import json_util
import pandas as pd

import pytz

from DBConnection import DBConnection


def getDatetime(date: str, time: str, timezone: str) -> datetime:
    """
    Converts string formatted date to datetime considering timezone
    :param date: a string containing the date in the format YYYY-MM-DD
    :param time: a string containing the time in the format HH:MM
    :param timezone: a string containing the timezone
    :return: a datetime with updated time based on the time zone
    """
    tz = pytz.timezone(timezone)
    naive_datetime = datetime.strptime(date + " " + time, '%Y-%m-%d %H:%M')
    localized_datetime = tz.localize(naive_datetime)
    return localized_datetime


def ampmTo24h(am: bool, hour: int, minutes: int) -> (int, int):
    """
    Function that converts a 12h time to 24h time, returns split hour and minutes
    :param am: value that express if the time is before 12:00 or after
    :param hour: hour of the time
    :param minutes: minutes of the time
    :return: hour: int and minutes: int
    """

    if hour == 12 and am:
        hour = 0
    elif hour != 12 and not am:
        hour += 12
    if hour < 10:
        hour = "0" + str(hour)
    if minutes < 10:
        minutes = "0" + str(minutes)
    return hour, minutes


def replaceSpacesNumber(conn: DBConnection, flight: dict) -> None:
    """
    Update the number of each flight to have no space in them
    :param conn: connection object
    :param flight: list of all the flights
    :return: None
    """

    if " " in flight["number"]:
        flight["number"] = flight["number"].replace(" ", "")
        conn.updateFlight(flight)


def getMostSimilarIATA(iatas: list, airportName: str) -> str:
    """
    Find the IATA based on airport name, if not found just returns the name provided as parameter
    :param iatas: a list containing the IATAs acronym
    :param airportName: a string containing the name to search
    :return: a string containing converted name
    """
    airportName = airportName.lower().replace(" ", "")
    for iata in iatas:
        clearName = iata["name"].split(",")[0].lower().replace(" ", "")
        if clearName in airportName:
            return iata["acronym"]
    return airportName.upper()


def printJSONToFile(data: dict, fileName="data") -> None:
    """
    Print a dict to a json file
    :param data: a dict to print
    :param fileName: a string containing the name of the file with no extension
    :return: None
    """
    f = open(fileName + ".json", "w", encoding="utf8")
    f.write(json.dumps(data, indent=4))
    f.close()


def printBSONToFile(data: dict, fileName="data") -> None:
    """
    Print a dict containing bson objects to a json file
    :param data: a dict to print
    :param fileName: a string containing the name of the file with no extension
    :return: None
    """
    f = open(fileName + ".json", "w", encoding="utf8")
    f.write(json_util.dumps(data, indent=4))
    f.close()


def printToFile(data: str, fileName="data", fileExtension="txt") -> None:
    """
    Print a string to a file
    :param data: a string to print
    :param fileName: a string containing the name of the file
    :param fileExtension: a string containing the extension of the file
    :return:
    """
    f = open(fileName + "." + fileExtension, "w", encoding="utf8")
    f.write(data)
    f.close()


def reportToCsv(data: dict, fileName="data") -> None:
    """
    Print a report dict to a json file
    :param data: a dict to print
    :param fileName: a string containing the name of the file with no extension
    :return: None
    """
    f = open("./airportsDetails/" + fileName + ".csv", 'w', newline="")
    writer = csv.writer(f)
    writer.writerow(list(data.keys()))
    airports = sorted(list(data[list(data.keys())[0]].keys()))
    for airport in airports:
        values = []
        for el in data:
            values.append(data[el][airport])
        writer.writerow(values)
    f.close()


def delaysCorrelations(dataframe: pandas.DataFrame) -> dict:
    """
    Calculate the correlation and returns only the delay related ones
    :param dataframe: a dataframe containing the data to correlate
    :return: a dict containing the correlation matrix
    """
    corr = dataframe.corr()
    rows = list(corr.index)[1:]
    measures = {}
    for el in rows:
        measures[el] = corr.loc[el, "delay"]
    return measures


def flightsListToDataframe(flights: list) -> pandas.DataFrame:
    """
    Convert a dict to a dataframe
    :param flights: a dict to convert
    :return: a dataframe
    """
    dataframe = pd.DataFrame(flights)
    dataframe.index = dataframe["_id"]
    dataframe = createDelaysColumn(dataframe)
    return dataframe


def createDelaysColumn(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    """
    Calculate and add the delay column to the dataframe
    :param dataframe: a dataframe containing flights information
    :return: a dataframe updated
    """
    delays = []
    for objId in dataframe.index:
        flight = dataframe.loc[objId]
        if flight["actualDep"] is not None:
            try:
                delay = flight["actualDep"] - flight["scheduledDep"]
                delay = delay.total_seconds() / 60
                if delay >= 0:
                    delays.append(delay)
                else:
                    delays.append(None)
            except Exception as e:
                delays.append(None)
        else:
            delays.append(None)
    dataframe.insert(len(dataframe.columns), "delay", delays)
    return dataframe


def printMeasures(measures: dict) -> None:
    """
    Print correlation matrix values
    :param measures: a dict containing the correlations
    :return: None
    """
    for measure in measures:
        print(measure, ": ", round(measures[measure], 3), sep="")


def splitDatetime(dt: str) -> (str, str):
    """
    Split a datetime string into date and time
    :param dt: a string representing the datetime
    :return: a tuple containing date and time respectively
    """
    dt = str(dt)
    date = dt.split(" ")[0]
    time = dt.split(" ")[1][:-3]
    return date, time

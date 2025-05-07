from utils import getDatetime, getMostSimilarIATA


def cleanFlightFromMXP(flight: dict, iatas: list) -> dict:
    """
    Remove unused data from the raw flight from Malpensa
    :param flight: all the raw data from a single flight
    :param iatas: a list containing all IATAs acronyms
    :return: cleaned version of the flight
    """
    timeSplit = flight["scheduledTime"].split(" ")
    date = timeSplit[0]
    schedTime = timeSplit[1]
    actualDep = None
    if flight["actualTime"] is not None:
        actTime = flight["actualTime"].split(" ")[1]
        actualDep = getDatetime(date, actTime, "Europe/Rome")
    return {
        "number": flight["flightNumber"].replace(" ", ""),
        "status": flight["statusPubblicDescription"],
        "scheduledDep": getDatetime(date, schedTime, "Europe/Rome"),
        "actualDep": actualDep,
        "airportDep": "MXP",
        "airportArr": getMostSimilarIATA(iatas, flight["routing"][1]["airportDescription"]),
    }


def cleanFlightFromBOG(flight: dict, iatas: list) -> dict:
    """
    Remove unused data from the raw flight from El Dorado
    :param flight: all the raw data from a single flight
    :param iatas: a list containing all IATAs acronyms
    :return: cleaned version of the flight
    """
    date = flight["scheduleDate"].split(" ")[0]
    schedTime = flight["scheduleDate"].split(" ")[1][:-3]
    actTime = flight["actualDate"].split(" ")[1][:-3]

    return {
        "number": str(flight["airline"]["code"] + flight["number"]).replace(" ", ""),
        "status": flight["status"]["en"].upper(),
        "scheduledDep": getDatetime(date, schedTime, "America/Bogota"),
        "actualDep": getDatetime(date, actTime, "America/Bogota"),
        "airportDep": "BOG",
        "airportArr": getMostSimilarIATA(iatas, flight["city"]["cityName"]),
    }


def cleanFlightFromRPLL(flight: dict, iatas: list) -> dict:
    """
    Remove unused data from the raw flight from Manila
    :param flight: all the raw data from a single flight
    :param iatas: a list containing all IATAs acronyms
    :return: cleaned version of the flight
    """
    if flight["AtaAtd"] == "":
        return {}

    date = flight["StaStd"].split(" ")[0]
    schedTime = flight["StaStd"].split(" ")[1][:-3]
    actTime = flight["AtaAtd"].split(" ")[1][:-3]

    return {
        "number": flight["Airline_Code"] + flight["Flight_Number"],
        "status": flight["Status"].upper(),
        "scheduledDep": getDatetime(date, schedTime, "Asia/Manila"),
        "actualDep": getDatetime(date, actTime, "Asia/Manila"),
        "airportDep": "RPLL",
        "airportArr": getMostSimilarIATA(iatas, flight["Destination"]),
    }


def cleanFlightFromATH(flight: dict, iatas: list) -> dict:
    """
    Remove unused data from the raw flight from Athens
    :param flight: all the raw data from a single flight
    :param iatas: a list containing all IATAs acronyms
    :return: cleaned version of the flight
    """
    date = flight["ScheduledTime"].split(" ")[0].split("/")
    date = date[2] + "-" + date[1] + "-" + date[0]
    actualDep = flight["ActualTime"]
    if actualDep == "":
        actualDep = None
    if actualDep is not None:
        actualDep = getDatetime(date, actualDep, "Europe/Athens")
    schedTime = flight["ScheduledTime"].split(" ")[1]
    return {
        "number": flight["FlightNo"].replace(" ", "").upper(),
        "status": flight["FlightStateName"].upper(),
        "scheduledDep": getDatetime(date, schedTime, "Europe/Athens"),
        "actualDep": actualDep,
        "airportDep": "ATH",
        "airportArr": getMostSimilarIATA(iatas, flight["AirportName"]),
    }

import pymongo.collection
from pymongo import MongoClient
from bson import ObjectId

from dbSecrets import getPassword, getUsername


class DBConnection:

    def __init__(self):
        self.client = self.connectDb()
        self.itemColl = self.getItemColl(self.client)
        self.iataColl = self.getIATAColl(self.client)

    def getConnectionUri(self) -> str:
        """
        :return: database connection string
        """
        return f"mongodb+srv://{getUsername()}:{getPassword()}@projectflights.3ekyjwj.mongodb.net/?retryWrites=true&w=majority"

    def connectDb(self) -> MongoClient:
        """
        Handle the connection to the database
        :return: MongoClient object
        """
        uri = self.getConnectionUri()
        return MongoClient(uri)

    def getItemColl(self, client: MongoClient) -> pymongo.collection.Collection:
        """
        returns the flights collection
        :param client: MongoClient object
        :return: the flight collection to use in the database
        """
        db = client["dbflights"]
        return db["collflights"]

    def getIATAColl(self, client: MongoClient) -> pymongo.collection.Collection:
        """
        returns the iata collection
        :param client: MongoClient object
        :return: the iata collection to use in the database
        """
        db = client["dbflights"]
        return db["colliata"]

    def isFlightInDatabase(self, element: dict) -> bool:
        """
        Check if a flight is in the database using only some parameters
        :param element: a flight
        :return: true if the flight is already in the database, false if not
        """
        el = {
            "scheduledDep": element["scheduledDep"],
            "actualDep": element["actualDep"],
            "airportDep": element["airportDep"],
            "airportArr": element["airportArr"],
        }
        res = list(self.itemColl.find(el))
        return len(res) >= 1

    def isIATAInDatabase(self, iata: str) -> bool:
        """
        Check if a IATA is in the database
        :param iata: IATA acronym to check
        :return: true if the IATA is already in the database, false if not
        """
        res = list(self.iataColl.find({"acronym": iata}))
        return len(res) >= 1

    def countInDatabase(self, element: dict) -> int:
        """
        Count how many appearence of a flight are in the database using only some parameters
        :param element: a flight
        :return: the counter of similar flight in the database
        """
        try:
            el = {
                "scheduledDep": element["scheduledDep"],
                "actualDep": element["actualDep"],
                "airportDep": element["airportDep"],
                "airportArr": element["airportArr"],
            }
            res = list(self.itemColl.find(el))
            return len(res)
        except Exception as e:
            print(element)
            return 0

    def insertOneFlight(self, element: dict) -> None:
        """
        Insert a flight in the database checking if it is already inserted
        :param element: a flight
        :return: None
        """
        if not self.isFlightInDatabase(element):
            self.itemColl.insert_one(element)
            print(f"[+] Caricato {element['number']} {element['scheduledDep']}")

    def getAllFlights(self) -> list:
        """
        :return: All the flights in the database
        """
        return list(self.itemColl.find({}))

    def getAllIATA(self) -> list:
        """
        :return: All the iatas in the database
        """
        return list(self.iataColl.find({}))

    def updateFlight(self, element: dict) -> None:
        """
        Update a flight in the database using the _id as a filter
        :param element: a flight
        :return: None
        """
        self.itemColl.replace_one({"_id": element["_id"]}, element)
        print(f"[*] Aggiornato {element['_id']}")

    def deleteFlight(self, element: dict) -> None:
        """
        Delete a flight in the database using the _id as a filter
        :param element: a flight
        :return: None
        """
        self.itemColl.delete_one({"_id": ObjectId(element["_id"]["$oid"])})

    def getDistinctAirportArrNames(self) -> list:
        """
        :return: a list containing arrival airport names
        """
        return list(self.itemColl.distinct("airportArr"))

    def getDistinctAirportDepNames(self) -> list:
        """
        :return: a list containing departure airport names
        """
        return list(self.itemColl.distinct("airportDep"))

    def insertOneIATA(self, element: dict) -> None:
        """
        Insert a IATA in the database checking if it is already inserted
        :param element: a IATA
        :return: None
        """
        if not self.isIATAInDatabase(element["acronym"]):
            self.iataColl.insert_one(element)
            print(f"[+] Caricato {element['acronym']}")

    def flightsGroupedByAirport(self) -> list:
        """
        QUERY returning number of flights for each airport
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$group": {
                    "_id": "$airportDep",
                    "count": {
                        "$sum": 1
                    }
                }
            },
            {
                "$project": {
                    "airport": "$_id",
                    "_id": 0,
                    "count": 1
                }
            }
        ]))

    def meanWindSpeed100mGroupedByAirport(self) -> list:
        """
        QUERY returning mean of wind speed for each airport
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$group": {
                    "_id": "$airportDep",
                    "mean": {
                        "$avg": "$wind_speed_100m"
                    }
                }
            },
            {
                "$project": {
                    "airport": "$_id",
                    "_id": 0,
                    "mean": 1
                }
            }
        ]))

    def meanToDict(self, operator: str, attribute: str, mean: list) -> list:
        """
        Returns the paramater to add after the $or block
        :param operator: a string containing the operator to execute
        :param attribute: a string containing the attribute to evaluate
        :param mean: a list containing the means for each airport
        :return: a list containing all the filter to add
        """
        values = []
        for el in mean:
            values.append({"airportDep": el["airport"], attribute: {operator: el["mean"]}})
        return values

    def meanDelaysGroupedByAirport(self) -> list:
        """
        QUERY returning mean of delays for each airport
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$project": {
                    "delays": {
                        "$dateDiff": {
                            "startDate": "$scheduledDep",
                            "endDate": "$actualDep",
                            "unit": "minute"
                        }
                    },
                    "airportDep": 1,
                    "_id": 0
                }
            },
            {
                "$group": {
                    "_id": "$airportDep",
                    "avgDelays": {
                        "$avg": "$delays"
                    }
                }
            },
            {
                "$project": {
                    "avgDelays": 1,
                    "airport": "$_id",
                    "_id": 0
                }
            }
        ]))

    def meanDelaysGroupedByAirportFilteredOnWind100mGt(self) -> list:
        """
        QUERY returning mean of delays for each airport filtering on wind speed greater than its mean
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$match": {
                    "$or": self.meanToDict("$gt", "wind_speed_100m", self.meanWindSpeed100mGroupedByAirport())
                }
            },
            {
                "$project": {
                    "delays": {
                        "$dateDiff": {
                            "startDate": "$scheduledDep",
                            "endDate": "$actualDep",
                            "unit": "minute"
                        }
                    },
                    "airportDep": 1,
                    "_id": 0
                }
            },
            {
                "$group": {
                    "_id": "$airportDep",
                    "avgDelays": {
                        "$avg": "$delays"
                    }
                }
            },
            {
                "$project": {
                    "avgDelays": 1,
                    "airport": "$_id",
                    "_id": 0
                }
            }
        ]))

    def meanDelaysGroupedByAirportFilteredOnWind100mLt(self) -> list:
        """
        QUERY returning mean of delays for each airport filtering on wind speed less or equal than its mean
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$match": {
                    "$or": self.meanToDict("$lte", "wind_speed_100m", self.meanWindSpeed100mGroupedByAirport())
                }
            },
            {
                "$project": {
                    "delays": {
                        "$dateDiff": {
                            "startDate": "$scheduledDep",
                            "endDate": "$actualDep",
                            "unit": "minute"
                        }
                    },
                    "airportDep": 1,
                    "_id": 0
                }
            },
            {
                "$group": {
                    "_id": "$airportDep",
                    "avgDelays": {
                        "$avg": "$delays"
                    }
                }
            },
            {
                "$project": {
                    "avgDelays": 1,
                    "airport": "$_id",
                    "_id": 0
                }
            }
        ]))

    def meanPrecipitationGroupedByAirport(self) -> list:
        """
        QUERY returning mean of precipitation for each airport
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$group": {
                    "_id": "$airportDep",
                    "mean": {
                        "$avg": "$precipitation"
                    }
                }
            },
            {
                "$project": {
                    "airport": "$_id",
                    "_id": 0,
                    "mean": 1
                }
            }
        ]))

    def meanDelaysGroupedByAirportFilteredOnPrecipitationGt(self) -> list:
        """
        QUERY returning mean of delays for each airport filtering on precipitation greater than its mean
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$match": {
                    "$or": self.meanToDict("$gt", "precipitation", self.meanPrecipitationGroupedByAirport())
                }
            },
            {
                "$project": {
                    "delays": {
                        "$dateDiff": {
                            "startDate": "$scheduledDep",
                            "endDate": "$actualDep",
                            "unit": "minute"
                        }
                    },
                    "airportDep": 1,
                    "_id": 0
                }
            },
            {
                "$group": {
                    "_id": "$airportDep",
                    "avgDelays": {
                        "$avg": "$delays"
                    }
                }
            },
            {
                "$project": {
                    "avgDelays": 1,
                    "airport": "$_id",
                    "_id": 0
                }
            }
        ]))

    def meanDelaysGroupedByAirportFilteredOnPrecipitationLt(self) -> list:
        """
        QUERY returning mean of delays for each airport filtering on precipitation less or equal than its mean
        :return: a list containing the query response
        """
        return list(self.itemColl.aggregate([
            {
                "$match": {
                    "$or": self.meanToDict("$lte", "precipitation", self.meanPrecipitationGroupedByAirport())
                }
            },
            {
                "$project": {
                    "delays": {
                        "$dateDiff": {
                            "startDate": "$scheduledDep",
                            "endDate": "$actualDep",
                            "unit": "minute"
                        }
                    },
                    "airportDep": 1,
                    "_id": 0
                }
            },
            {
                "$group": {
                    "_id": "$airportDep",
                    "avgDelays": {
                        "$avg": "$delays"
                    }
                }
            },
            {
                "$project": {
                    "avgDelays": 1,
                    "airport": "$_id",
                    "_id": 0
                }
            }
        ]))

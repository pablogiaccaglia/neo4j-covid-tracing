import datetime
import enum
import random
import string
from collections import namedtuple
from csv import DictReader, DictWriter, reader, writer
from OSMPythonTools.api import Api
from OSMPythonTools.nominatim import Nominatim
import pandas as pd

# using the reduced version of the original 'personal_info'
# because it is huge (part of 2019 Facebook data breach)
personalInfoFilePath = "datasets/personal_info_reduced.txt"
namesFilePath = "datasets/names.csv"


def getRandomDate() -> datetime:
    start_date = datetime.date(1930, 1, 1)
    end_date = datetime.date(2021, 1, 1)

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days = random_number_of_days)

    return random_date


def getRandomResidence() -> namedtuple:
    data = DictReader(open('datasets/original/italy_cities.csv'))
    term = random.choice([i for i in data])
    residenceTuple = namedtuple('residence', ['city', 'region', 'country'])
    residence = residenceTuple(term['city_ascii'], term['admin_name'], term['country'])

    return residence


def generateRandomCIENumber() -> str:
    """2 letters – 5 digits – 2 letters"""

    NumeroUnicoNazionale = \
        str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.randrange(10000, 100000)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase))

    return NumeroUnicoNazionale


def generateRandomItalianDrivingLicenseNumber() -> str:
    """2 letters - 7 digits - 1 letter"""

    DrivingLicenseNumber = \
        str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.randrange(1000000, 10000000)) \
        + str(random.choice(string.ascii_uppercase))

    return DrivingLicenseNumber


def generateRandomItalianPassportNumber() -> str:
    """2 letters - 7 digits"""

    ItalianPassportNumber = \
        str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.randrange(1000000, 10000000))

    return ItalianPassportNumber


class Document(enum.Enum):
    _documentTuple = namedtuple("document", ["name", "id"])

    CIE = _documentTuple("Italian electronic identity card", generateRandomCIENumber)
    DrivingLicenseCard = _documentTuple("Italian driving license card", generateRandomItalianDrivingLicenseNumber)
    ItalianPassport = _documentTuple("Italian passport", generateRandomItalianPassportNumber)

    values = [CIE, DrivingLicenseCard, ItalianPassport]


def getRandomDocument() -> Document:
    return random.choice(Document.values.value)


def removeNullRows(csvPath, delimiter = ':') -> None:
    """ removes all the row which contains at least one null value"""
    try:
        dictReader = DictReader(open(csvPath), delimiter = delimiter)
    except:
        print("cannot remove null values")
        return

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_no_nulls' + csvPath[fileExtension:]

    dictWriter = DictWriter(open(outputString, 'w'), dictReader.fieldnames, delimiter = delimiter)
    dictWriter.writeheader()

    for row in dictReader:
        nullFound = False
        for column, value in row.items():
            if value is None or value == "":
                nullFound = True
        if not nullFound:
            dictWriter.writerow(row)


def removeDuplicateRows(csvPath) -> None:
    written_entries = []

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_no_duplicates' + csvPath[fileExtension:]

    with open(outputString, 'w') as out_file:
        with open(csvPath, 'r') as my_file:
            for line in my_file:
                columns = line.strip().split(',')
                for column in columns:
                    if column not in written_entries:
                        print(line.strip())
                        out_file.write(line)
                        written_entries.append(column)


def standardizeCSVColumn(csvPath, columnName, delimiter = ':') -> None:
    """converts each row of the given column in the csv in a standard lowercase format with first letter uppercase"""
    try:
        dictReader = DictReader(open(csvPath), delimiter = delimiter)
        if columnName not in dictReader.fieldnames:
            raise Exception
    except:
        print("cannot standardize")
        return

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_standardized' + csvPath[fileExtension:]

    dictWriter = DictWriter(open(outputString, 'w'), dictReader.fieldnames, delimiter = delimiter)
    dictWriter.writeheader()
    for row in dictReader:
        row[columnName] = row[columnName].title()
        dictWriter.writerow(row)


def convertCSVDelimiter(csvPath, oldDelimiter, newDelimiter) -> None:
    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_converted' + csvPath[fileExtension:]

    with open(csvPath) as in_file, open(outputString, 'w') as out_file:
        inCSV = reader(in_file, delimiter = oldDelimiter)
        outCSV = writer(out_file, delimiter = newDelimiter)
        for row in inCSV:
            outCSV.writerow(row)


def getOSMAddressFromID(ID) -> str:
    try:
        api = Api()
        busStop = api.query('way/' + str(ID))

        tags = busStop.tags()

        return tags["addr:street"]
    except:
        return "None"


def findAddress(queryString) -> ['namedtuple']:
    placeInfo = namedtuple("placeInfo", ['id', 'address'])

    try:
        nominatim = Nominatim()
        result = nominatim.query(queryString)

        placeElements = result.displayName().split(',')

        if placeElements[1].isdigit() or placeElements[1][1:].isdigit() or len(placeElements[1]) < 7:

            streetName = placeElements[2]

            if placeElements[1][0] == ' ':
                placeElements[1] = placeElements[1][1:]

            streetNumber = placeElements[1]

        else:
            streetNumber = random.randint(1, 199)
            streetName = placeElements[1]

        if streetName[0] == ' ':
            streetName = streetName[1:]

        address = streetName + " " + str(streetNumber)

        info = placeInfo(id = result.id, address = address)

    except:
        info = placeInfo(id = "None", address = getRandomCityName() + " " + str(random.randint(1, 199)))
        return info

    return info


def getRandomCityName() -> str:
    csvfile = pd.read_csv('datasets/final/italy_cities.csv')
    sample = csvfile.sample()
    city = sample.values[0][0]
    return city


def addStreetToPlacesCSV(csvPath, delimiter) -> None:
    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_with_streets' + csvPath[fileExtension:]

    addr = []

    dictReader = DictReader(open(csvPath), delimiter = delimiter)

    for row in dictReader:
        queryString = row['city'] + " " + row['admin_name'] + " " + row['name']
        addr.append(findAddress(queryString))

    inCSV = reader(open(csvPath, 'r'))
    outCSV = writer(open(outputString, 'w'))
    headers = next(inCSV)
    headers.append("address")
    outCSV.writerow(headers)

    for (row, value) in zip(inCSV, addr):
        row.append(value.address)
        outCSV.writerow(row)


def buildPersonsList() -> list:
    lines = tuple(open(personalInfoFilePath, 'r'))
    parsedInfo = []
    peoplesData = []

    for line in lines:
        parsedInfo.append(line.split(":"))

    for info in parsedInfo:
        if info[2] != "" and info[3] != "" and info[4] != "":
            document = getRandomDocument()
            Person = namedtuple("person", ["firstName", "lastName", "fullName", "sex", "birth_date", "id", "id_type"])
            personData = Person(info[2], info[3], info[2] + " " + info[3], info[4], str(getRandomDate()),
                                document.id(), document.name)
            peoplesData.append(personData)

    return peoplesData


def createCSV(header, data) -> None:
    for entry in data:
        if len(entry) != len(header):
            return

    with open(namesFilePath, 'w') as f:
        wrt = writer(f)
        wrt.writerow(header)
        wrt.writerows(data)


def createNamesCSV() -> None:
    names = buildPersonsList()
    header = ["name", "surname", "fullname", "sex", "birth date", "id", "id_type"]
    createCSV(header, names)


if __name__ == '__main__':
    addStreetToPlacesCSV("datasets/final/italian_cafes_standardized_no_nulls.csv", ',')

import datetime
import enum
import random
import string
from collections import namedtuple
from csv import DictReader, DictWriter, reader, writer
from OSMPythonTools.api import Api
from OSMPythonTools.nominatim import Nominatim
import pandas as pd

""" paths of CSV files in the 'final' folder """
italianCafesCSVPath = 'datasets/final/italian_cafes_standardized_no_nulls_with_streets_no_duplicates.csv'
italianRestaurantsCSVPath = 'datasets/final/italian_restaurants_standardized_no_nulls_no_duplicates_with_streets.csv'
italianCinemasCSVPath = 'datasets/final/italian_cinemas_standardized_no_nulls_no_duplicates_with_streets.csv'
italianHospitalsCSVPath = 'datasets/final/italian_hospitals_standardized_no_nulls_no_duplicates_with_streets.csv'
italianTheatersCSVPath = 'datasets/final/italian_theaters_standardized_no_nulls_no_duplicates_with_streets.csv'
italianCitiesCSVPath = 'datasets/final/italy_cities.csv'
italianStreetsCSVPath = 'datasets/final/italian_streets_no_duplicates_standardized.csv'
vaccinesCSVPath = 'datasets/final/italian_vaccines.csv'
peopleCSVPath = 'datasets/final/people.csv'
covidTestsCSVPath = 'datasets/final/covid_tests.csv'

# using the reduced version of the original 'personal_info'
# because it is huge (part of 2019 Facebook data breach)
personalInfoCSVPath = "datasets/original/personal_info_reduced.txt"

""" just a container list of Italian places' paths """
italianPlaces = [italianCafesCSVPath,
                 italianRestaurantsCSVPath,
                 italianTheatersCSVPath,
                 italianHospitalsCSVPath,
                 italianCinemasCSVPath]


def getRandomDate(start_date = datetime.date(1930, 1, 1), end_date = datetime.date(2021, 1, 1)) -> datetime:
    """ Generates a random datetime in between the start_date and the end_date.
        In case of one or both missing parameters, the default values are used """

    start_date = start_date
    end_date = end_date

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days = random_number_of_days)

    return random_date


def getRandomResidence() -> namedtuple:
    """ Creates a new namedtuple named 'residence' with the following structure :

        'city' , 'region' , 'country'

        The fields' values are retrieved from the CSV 'italian_cities'

        Example :

        'city'    :  'Brescia'
        'region'  :  'Lombardy'
        'country' :  'ITA'

        """
    data = DictReader(open(italianCitiesCSVPath))
    term = random.choice([i for i in data])
    residenceTuple = namedtuple('residence', ['city', 'region', 'country'])
    residence = residenceTuple(term['city_ascii'], term['admin_name'], term['country'])

    return residence


def generateRandomCIENumber() -> str:
    """ Generates a random "Carta d'Identità Elettronica" identification number,
        whose structure is 2 letters – 5 digits – 2 letters """

    NumeroUnicoNazionale = \
        str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.randrange(10000, 100000)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase))

    return NumeroUnicoNazionale


def generateRandomItalianDrivingLicenseNumber() -> str:
    """ Generates a random Italian Driving License identification number,
        whose structure is 2 letters - 7 digits - 1 letter """

    DrivingLicenseNumber = \
        str(random.choice(string.ascii_uppercase)) \
        + str(random.choice(string.ascii_uppercase)) \
        + str(random.randrange(1000000, 10000000)) \
        + str(random.choice(string.ascii_uppercase))

    return DrivingLicenseNumber


def generateRandomItalianPassportNumber() -> str:
    """ Generates a random Italian Passport identification number,
        whose structure is 2 letters - 7 digits """

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


# RANDOM GETTERS

def getRandomDocument() -> Document:
    return random.choice(Document.values.value)


def getRandomCityName() -> str:
    """ Picks a random Italian city name from the CSV 'italy_cities' """

    csvFile = pd.read_csv(italianCitiesCSVPath)
    sample = csvFile.sample()
    city = sample.values[0][0]
    return city


def getRandomCityID() -> str:
    """ Picks a random city ID (OpenStreetMap ID) from the CSV 'italy_cities' """

    csvFile = pd.read_csv(italianCitiesCSVPath)
    sample = csvFile.sample()
    cityID = sample.values[0][5]
    return cityID


def getRandomItalianVaccine() -> str:
    """ Picks a random Italian vaccine from the CSV 'italian_vaccine'.
        The random choice is among:
        -> Pfizer/BioNTech
        -> Moderna
        -> Johnson&Johnson
        -> Oxford/AstraZeneca """

    csvFile = pd.read_csv(vaccinesCSVPath)
    sample = csvFile.sample()
    vaccine = sample.values[0][1]
    return vaccine


def getRandomItalianAddress() -> str:
    """ Picks a random Italian street address (name + number) from the CSV 'italian_streets_no_duplicates_standardized.
        Example : 'Via Certaldese 155' """

    csvFile = pd.read_csv(italianStreetsCSVPath)
    sample = csvFile.sample()
    address = sample.values[0][0]
    return address


def getRandomCovidTest() -> str:
    """ Picks a random Covid test from the CSV 'covid_tests'
        The random choice is among:
        -> Molecular test
        -> Antigen test
        -> Antibody test """

    csvFile = pd.read_csv(covidTestsCSVPath)
    sample = csvFile.sample()
    test = sample.values[0][0]
    return test


def getRandomPersonID() -> str:
    """ Picks a random person's ID from the CSV 'people'.
        The ID belongs to a person node in the database """

    csvFile = pd.read_csv(peopleCSVPath)
    sample = csvFile.sample()
    personID = sample.values[0][5]
    return personID


def getRandomPlaceID() -> int:
    """ Picks a random place ID from one of the following CSVs:

        -> italian_cafes_standardized_no_nulls_with_streets_no_duplicates
        -> italian_restaurants_standardized_no_nulls_no_duplicates_with_streets
        -> italian_cinemas_standardized_no_nulls_no_duplicates_with_streets
        -> italian_theaters_standardized_no_nulls_no_duplicates_with_streets

        """

    csvFile = pd.read_csv(random.choice(italianPlaces))
    sample = csvFile.sample()
    placeID = sample.values[0][3]

    return placeID


# CSV MANIPULATION METHODS

def removeNullRows(csvPath, delimiter = ':') -> None:
    """ Given the path of a CSV file and eventually its delimiter,
        removes all the rows which contains at least one null value,
        creating (or overwriting) a new file"""

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
    """ Given the path of a CSV file,
        removes all the duplicated rows
        creating (or overwriting) a new file """

    written_entries = []

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_no_duplicates' + csvPath[fileExtension:]

    with open(outputString, 'w') as out_file:
        with open(csvPath, 'r') as my_file:
            for line in my_file:
                if line not in written_entries:
                    print(line)
                    out_file.write(line)
                    written_entries.append(line)


def standardizeCSVColumn(csvPath, columnName, delimiter = ':') -> None:
    """ Given a path of a CSV file and eventually its delimiter,
        converts each row of the given column in the CSV in a
        standard lowercase format with first letter uppercase
        creating (or overwriting) a new file """

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
        word = row[columnName] + " " + str(random.randint(1, 199))
        row[columnName] = word
        dictWriter.writerow(row)


def convertCSVDelimiter(csvPath, oldDelimiter, newDelimiter) -> None:
    """ Given a path of a CSV file, its delimiter and a new delimiter,
        creates (or overwrites) a new CSV file replacing the old delimiter
         with the new one provided as a parameter """

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_converted' + csvPath[fileExtension:]

    with open(csvPath) as in_file, open(outputString, 'w') as out_file:
        inCSV = reader(in_file, delimiter = oldDelimiter)
        outCSV = writer(out_file, delimiter = newDelimiter)
        for row in inCSV:
            outCSV.writerow(row)


def getOSMAddressFromID(ID) -> str:
    """ Given an ID, tries to retrieve an OpenStreetMap address with that ID.
        If not found, return 'None' string """

    try:
        api = Api()
        way = api.query('way/' + str(ID))

        tags = way.tags()

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
    lines = tuple(open(personalInfoCSVPath, 'r'))
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

    with open(peopleCSVPath, 'w') as f:
        wrt = writer(f)
        wrt.writerow(header)
        wrt.writerows(data)


def createNamesCSV() -> None:
    names = buildPersonsList()
    header = ["name", "surname", "fullname", "sex", "birth date", "id", "id_type"]
    createCSV(header, names)


if __name__ == '__main__':
    createNamesCSV()
    pass

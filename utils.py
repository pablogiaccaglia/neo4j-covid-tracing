import datetime
import enum
import random
import string
from collections import namedtuple
from csv import DictReader, DictWriter, reader, writer

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


def standardizeCSVColumn(csvPath, columnName, delimiter = ':') -> None:
    """converts each row of the given column in the csv in a standard lowercase format with first letter uppercase"""
    try:
        reader = DictReader(open(csvPath), delimiter = delimiter)
        if columnName not in reader.fieldnames:
            raise Exception
    except:
        print("cannot standardize")
        return

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_standardized' + csvPath[fileExtension:]

    writer = DictWriter(open(outputString, 'w'), reader.fieldnames, delimiter = delimiter)
    writer.writeheader()
    for row in reader:
        row[columnName] = row[columnName].title()
        writer.writerow(row)


def removeNullRows(csvPath, delimiter = ':') -> None:
    """ removes all the row which contains at least one null value"""
    try:
        reader = DictReader(open(csvPath), delimiter = delimiter)
    except:
        print("cannot remove null values")
        return

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_no_nulls' + csvPath[fileExtension:]

    writer = DictWriter(open(outputString, 'w'), reader.fieldnames, delimiter = delimiter)
    writer.writeheader()

    for row in reader:
        nullFound = False
        for column, value in row.items():
            if value is None or value == "":
                nullFound = True
        if not nullFound:
            writer.writerow(row)


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


def convertCSVDelimiter(csvPath, oldDelimiter, newDelimiter) -> None:

    fileExtension = csvPath.find('.csv')
    outputString = csvPath[:fileExtension] + '_converted' + csvPath[fileExtension:]

    with open(csvPath) as in_file, open(outputString, 'w') as out_file:
        inCSV = reader(in_file, delimiter = oldDelimiter)
        outCSV = writer(out_file, delimiter = newDelimiter)
        for row in inCSV:
            outCSV.writerow(row)


if __name__ == '__main__':
    #  standardizeCSVColumn("datasets/original/italian_theaters.csv", 'city')
    #  removeNullRows("datasets/original/italian_theaters_standardized.csv")
    convertCSVDelimiter("datasets/final/italian_hospitals_standardized_no_nulls.csv", ';', ',')

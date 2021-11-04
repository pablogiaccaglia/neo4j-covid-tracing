from collections import namedtuple
import utils
import csv


def buildPersonsList() -> list:

    lines = tuple(open(utils.personalInfoFilePath, 'r'))
    parsedInfo = []
    peoplesData = []

    for line in lines:
        parsedInfo.append(line.split(":"))

    for info in parsedInfo:
        if info[2] != "" and info[3] != "" and info[4] != "":
            document = utils.getRandomDocument()
            Person = namedtuple("person", ["firstName", "lastName", "fullName", "sex", "birth_date", "id", "id_type"])
            personData = Person(info[2], info[3], info[2] + " " + info[3], info[4], str(utils.getRandomDate()), document.id(), document.name)
            peoplesData.append(personData)

    return peoplesData


def createCSV(header, data) -> None:

    for entry in data:
        if len(entry) != len(header):
            return

    with open(utils.namesFilePath, 'w', encoding = 'UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


if __name__ == '__main__':
    names = buildPersonsList()
    header = ["name", "surname", "fullname", "sex", "birth date", "id", "id_type"]
    createCSV(header, names)



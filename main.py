import random
import time

from neo4j import GraphDatabase
import csv
import operator
import math
import utils
import datetime


class CovidGraphHandler:

    def __init__(self, uri, user, password):

        self.driver = GraphDatabase.driver(uri, auth = (user, password), max_connection_lifetime = 1000)
        self.session = self.driver.session()

        ''' Neo4j 'import' folder files paths'''
        self.italianCafesCSVInternalPath = 'file:///italian_cafes_standardized_no_nulls_with_streets_no_duplicates.csv'
        self.italianRestaurantsCSVInternalPath = 'file:///italian_restaurants_standardized_no_nulls_no_duplicates_with_streets.csv'
        self.italianCinemasCSVInternalPath = 'file:///italian_cinemas_standardized_no_nulls_no_duplicates_with_streets.csv'
        self.italianHospitalsCSVInternalPath = 'file:///italian_hospitals_standardized_no_nulls_no_duplicates_with_streets.csv'
        self.italianTheatersCSVInternalPath = 'file:///italian_theaters_standardized_no_nulls_no_duplicates_with_streets.csv'
        self.italianCitiesCSVInternalPath = 'file:///italy_cities.csv'
        self.peopleCSVInternalPath = 'file:///people.csv'
        self.italianVaccines = 'file:///italian_vaccines.csv'
        self.covidTestsCSVInternalPath = 'file:///covid_tests.csv'
        self.italianPlacesCSVsPaths = utils.italianPlaces

    def close(self):
        self.driver.close()

    def populateDatabase(self):

        """ if the database is not empty, clear it"""
        if not self.session.write_transaction(self._checkIfDBIsEmpty):
            self.session.write_transaction(self._clearDatabase)

        self.createPersonNodes()
        self.createPlaceNodes()
        self.createVaccineNodes()
        self.createTestsNodes()
        self.createCityNodes()
        self.createCountryNodes()
        self.createRelationships()

    def _checkIfDBIsEmpty(self, tx) -> bool:
        result = tx.run("""
        MATCH (n)
        RETURN COUNT(n) = 0""")
        return result.single()[0]

    def _clearDatabase(self, tx) -> None:
        tx.run("""
        MATCH(n)
        DETACH DELETE n;""")

    def createPersonNodes(self):
        self.session.write_transaction(self._createPersonNodes)

    def createPlaceNodes(self):
        self.session.write_transaction(self._createCinemaNodes)
        self.session.write_transaction(self._createHospitalNodes)
        self.session.write_transaction(self._createRestaurantNodes)
        self.session.write_transaction(self._createTheaterNodes)
        self.session.write_transaction(self._createCafeNodes)

    def createVaccineNodes(self):
        self.session.write_transaction(self._createVaccineNodes)

    def createTestsNodes(self):
        self.session.write_transaction(self._createTestsNodes)

    def createCityNodes(self):
        self.session.write_transaction(self._createCityNodes)

    def createCountryNodes(self):
        self.session.write_transaction(self._createCountryNodes)

    def createRelationships(self):
        self.session.write_transaction(self._createPartOfRelationship)
        self._createLocateRelationship()
        self._livesInAndLivesWithRelationshipsHandler()
        self._handleTookTestRelationship()
        self._createReceivedVaccineRelationship()
        self._handleMetRelationship()
        self._handleWentToRelationship()

    def _createTestsNodes(self, tx):
        query = """ LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Test {{type : line.type}})
                """.format(self.covidTestsCSVInternalPath)

        tx.run(query)

    def _createPersonNodes(self, tx):
        query = """
        LOAD CSV WITH HEADERS FROM '{0}'
        AS line FIELDTERMINATOR ','
        WITH line
        CREATE (:Person {{name: line.name, surname: line.surname,
        fullname: line.fullname, sex: line.sex, birthDate: date(line.birth_date), id: line.id, idType: line.id_type}})
        """.format(self.peopleCSVInternalPath)

        tx.run(query)

    def _createCafeNodes(self, tx):
        query = """
                LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'cafe'}})
        """.format(self.italianCafesCSVInternalPath)

        tx.run(query)

    def _createCinemaNodes(self, tx):
        query = """
                LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'cinema'}})
        """.format(self.italianCinemasCSVInternalPath)

        tx.run(query)

    def _createRestaurantNodes(self, tx):
        query = """
                LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'restaurant'}})
                """.format(self.italianRestaurantsCSVInternalPath)

        tx.run(query)

    def _createHospitalNodes(self, tx):
        query = """LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'hospital'}})
                """.format(self.italianHospitalsCSVInternalPath)

        tx.run(query)

    def _createTheaterNodes(self, tx):
        query = """LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'theater'}})
                """.format(self.italianTheatersCSVInternalPath)

        tx.run(query)

    def _createVaccineNodes(self, tx):
        query = """LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:Vaccine {{manufacturer: line.vaccine}})""".format(self.italianVaccines)

        tx.run(query)

    def _createCityNodes(self, tx):
        query = """ LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                WITH line
                CREATE (:City {{name: line.city_ascii, id: toInteger(line.id), population: toInteger(line.population), region: line.admin_name}})
                """.format(self.italianCitiesCSVInternalPath)

        tx.run(query)

    def _createCountryNodes(self, tx):
        # since we are storing just Italian data, we do not need to store all these nodes
        #    tx.run('''
        #            LOAD CSV WITH HEADERS FROM
        #            'file:///countries.csv'
        #            AS line FIELDTERMINATOR ','
        #            WITH line
        #            CREATE (:Country {name: line.Country, population: toInteger(line.Population)})
        #    ''')

        tx.run('''
        CREATE (:Country {name: 'Italy', population: toInteger('60446035')});
        ''')

    def _createLocateRelationship(self):

        for CSVPath in self.italianPlacesCSVsPaths:
            dictReader = csv.DictReader(open(CSVPath))

            for row in dictReader:
                """ the replace method is needed to escape single quotes, 
                    otherwise query will fail if string contains any """
                name = row['name'].replace("'", r"\'")
                city = row['city'].replace("'", r"\'")
                address = row['address'].replace("'", r"\'")
                placeId = row['ID']

                query = '''
                            MATCH (p:Place)
                            WHERE p.id = {1}
                            AND p.name = '{0}'
                            WITH p
                            MATCH(c:City)
                            WHERE c.name = '{2}'
                            WITH c,p
                            OPTIONAL MATCH (c2:City) 
                            WHERE c2.id = {4}
                            WITH c,p,c2
                            
                            FOREACH(ignoreMe IN CASE WHEN c IS NULL THEN [1] ELSE [] END |
                            MERGE (p)-[:LOCATED {{address : '{3}'}}]->(c2))
                            
                            FOREACH(ignoreMe IN CASE WHEN c IS NOT NULL THEN [1] ELSE [] END | 
                            MERGE (p)-[:LOCATED {{address : '{3}'}}]->(c))
                             '''.format(name, placeId, city, address, int(utils.getRandomCityID()))

                self.session.write_transaction(self._queryExecutor, query)

    def _queryExecutor(self, tx, query):
        tx.run(query)

    def _createPartOfRelationship(self, tx):
        tx.run('''
                MATCH (ci:City) MATCH (co:Country)
                WHERE co.name = 'Italy'
                MERGE (ci)-[r:PART_OF]->(co);
                ''')

    def _createReceivedVaccineRelationship(self):
        self._createRelationshipsForDoubleDoseVaccinated()
        self._createRelationshipForSingleDoseVaccinated()

    def _createRelationshipForSingleDoseVaccinated(self):

        peopleData = csv.reader(open(utils.peopleCSVPath))
        data = sorted(peopleData, key = operator.itemgetter(4), reverse = True)

        val = math.floor(len(data) * 0.25)
        maxNumOfVaccinatedWith1Dose = val
        numOfVaccinatedWith1Dose = 0
        dailyNumberOfVaccinesWith1Dose = math.floor(val / 30)
        currentDailyNumberOfVaccinesWith1Dose = 0
        currentDate = datetime.date(2021, 11, 7)

        for entry in data:
            vaccine = utils.getRandomItalianVaccine()

            query = '''
                       
                       MATCH (p:Person)-[:LIVES_IN]-(c:City)
                       WHERE p.id = '{0}'
                       MATCH (v:Vaccine)
                       WHERE v.manufacturer =  '{1}'
                       CREATE (p)-[r:RECEIVED {{date : date('{2}'), city_id: c.id, number : 1}}]->(v); 
                                                        '''.format(entry[5], vaccine, currentDate)

            self.session.write_transaction(self._queryExecutor, query)

            currentDailyNumberOfVaccinesWith1Dose += 1
            numOfVaccinatedWith1Dose += 1

            if dailyNumberOfVaccinesWith1Dose < currentDailyNumberOfVaccinesWith1Dose:
                currentDate -= datetime.timedelta(days = 1)
                currentDailyNumberOfVaccinesWith1Dose = 0

            if numOfVaccinatedWith1Dose > maxNumOfVaccinatedWith1Dose:
                break

    def _createRelationshipsForDoubleDoseVaccinated(self):

        peopleData = csv.reader(open(utils.peopleCSVPath))
        data = sorted(peopleData, key = operator.itemgetter(4), reverse = False)

        val = math.floor(len(data) * 0.25)
        maxNumOfVaccinatedWith2Doses = val
        numOfVaccinatedWith2Doses = 0
        dailyNumberOfVaccinesWith2Doses = math.floor(val / 120)
        currentDailyNumberOfVaccinesWith2Doses = 0
        currentDate = datetime.date(2021, 1, 5)

        for entry in data:
            vaccine = utils.getRandomItalianVaccine()
            dateOfSecondDose = currentDate + datetime.timedelta(days = 28)

            query = ''' 
                       MATCH (p:Person)-[:LIVES_IN]-(c:City)
                       WHERE p.id = '{0}'
                       MATCH (v:Vaccine)
                       WHERE v.manufacturer = '{1}'
                       CREATE (p)-[r1:RECEIVED {{date : date('{2}'), city: c.id, number : 1}}]->(v)
                       CREATE (p)-[r2:RECEIVED {{date : date('{3}'), city: c.id, number : 2}}]->(v);
                       '''.format(entry[5], vaccine, currentDate, dateOfSecondDose)

            self.session.write_transaction(self._queryExecutor, query)
            currentDailyNumberOfVaccinesWith2Doses += 1
            numOfVaccinatedWith2Doses += 1

            if dailyNumberOfVaccinesWith2Doses < currentDailyNumberOfVaccinesWith2Doses:
                currentDate += datetime.timedelta(days = 1)
                currentDailyNumberOfVaccinesWith2Doses = 0

            if numOfVaccinatedWith2Doses > maxNumOfVaccinatedWith2Doses:
                break

    def _createLivesInRelationship(self, tx, entry, address, cityId):

        personId = entry['id']
        query = ''' 
                    MATCH (p:Person) MATCH (c:City)
                    WHERE p.id = '{0}'
                    AND c.id = {1}
                    MERGE (p)-[r:LIVES_IN {{address : '{2}'}}]-(c);
                    '''.format(personId, int(cityId), address)

        tx.run(query)

    def _createLivesWithRelationship(self, tx, id1, id2):

        query = ''' 
                                  MATCH (p1:Person) MATCH (p2:Person)
                                  WHERE p1.id = '{0}'
                                  AND p2.id = '{1}'
                                  MERGE (p1)-[r:LIVES_WITH]-(p2);
                                  '''.format(id1, id2)

        tx.run(query)

    def _livesInAndLivesWithRelationshipsHandler(self):

        components = [1, 2, 3, 4, 5, 6]

        assignedAddresses = []
        multipleEntries = []

        peopleData = csv.DictReader(open(utils.peopleCSVPath))

        enum_iter = enumerate(peopleData)

        for idx, entry in enum_iter:

            address = utils.getRandomItalianAddress().replace("'", r"\'")
            cityId = utils.getRandomCityID()

            while assignedAddresses.__contains__(address):
                address = utils.getRandomItalianAddress().replace("'", r"\'")

            assignedAddresses.append(address)

            selected = random.choice(components)
            if selected == 1:
                self.session.write_transaction(self._createLivesInRelationship, entry, address, cityId)

            else:
                self.session.write_transaction(self._createLivesInRelationship, entry, address, cityId)
                multipleEntries.append(entry)

                for i in range(selected - 1):
                    try:
                        idx2, entry2 = next(enum_iter)
                        multipleEntries.append(entry2)
                        self.session.write_transaction(self._createLivesInRelationship, entry2, address, cityId)
                    except:
                        pass  # StopIteration exception handling

                for entry3 in multipleEntries[:]:
                    multipleEntries.remove(entry3)
                    multipleEntries2 = multipleEntries
                    for entry4 in multipleEntries2:
                        id1 = entry3['id']
                        id2 = entry4['id']
                        self.session.write_transaction(self._createLivesWithRelationship, id1, id2)

    def _handleWentToRelationship(self):
        peopleData = csv.DictReader(open(utils.peopleCSVPath))

        for entry in peopleData:
            for i in range(50):
                placeID = utils.getRandomPlaceID()
                date = (utils.getRandomDate(datetime.date(2020, 3, 10), datetime.date(2021, 11, 7)))
                personID = entry['id']
                self.session.write_transaction(self._createWentToRelatioship, personID, placeID, date)

    def _createWentToRelatioship(self, tx, personID, placeID, date):

        query = '''
                    MATCH (pe:Person) MATCH (pl:Place)
                    WHERE pe.id = '{0}'
                    AND pl.id = {1}
                    MERGE (pe)-[r:WENT_TO {{date : date('{2}')}}]-(pl);'''.format(personID, int(placeID), date)


        tx.run(query)

    def _handleTookTestRelationship(self):

        peopleData = csv.DictReader(open(utils.peopleCSVPath))

        numberOfTakenTests = [0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 4]

        for entry in peopleData:
            takes = random.choice(numberOfTakenTests)
            personID = entry['id']

            for _ in range(takes):
                date = (utils.getRandomDate(datetime.date(2020, 3, 10), datetime.date(2021, 11, 7)))
                test = utils.getRandomCovidTest()
                isPositive = random.choice([True, False])
                self.session.write_transaction(self._createTookTestRelationship, personID, date, test, isPositive)

    def _createTookTestRelationship(self, tx, personID, date, test, isPositive):
        query = '''
                MATCH (p:Person)-[:LIVES_IN]-(c:City)
                WHERE p.id = '{0}'
                MATCH (t:Test)
                WHERE t.type = '{1}'
                MERGE (p)-[r:TOOK {{date : date('{2}') , isPositive : toBoolean({3}) , city_id : c.id}}]-(t)'''.format(
                personID, test,
                date, bool(isPositive))
        tx.run(query)

    def _handleMetRelationship(self):

        peopleData = csv.DictReader(open(utils.peopleCSVPath))
        sources = ['smartphone', 'smartwatch', 'wearable']

        for entry in peopleData:
            numberOfRelationships = random.randint(10, 50)

            person1ID = entry['id']

            date = (utils.getRandomDate(datetime.date(2020, 3, 10), datetime.date(2021, 11, 7)))

            for _ in range(numberOfRelationships):

                placeID = None

                source = random.choice(sources)
                if random.choice([True, False]):
                    placeID = utils.getRandomPlaceID()

                person2ID = utils.getRandomPersonID()

                while person2ID == person1ID:
                    person2ID = utils.getRandomPersonID()

                if not self.session.write_transaction(self._checkLivesWith, person1ID, person2ID):
                    self.session.write_transaction(self._createMetRelationship, person1ID, person2ID, date, source,
                                                   placeID)

    def _createMetRelationship(self, tx, person1ID, person2ID, date, source, placeID = None):

        if placeID is not None:

            query = '''
                    MATCH (pe1:Person) MATCH (pe2:Person) MATCH (pl:Place)
                    WHERE pe1.id = '{0}'
                    AND pe2.id = '{1}'
                    AND pl.id = {2}
                    MERGE (pe1)-[r:MET {{date : date('{3}'), source : '{4}', placeID : {5} }}]-(pe2)'''.format(
                    person1ID,
                    person2ID,
                    int(placeID),
                    date,
                    source,
                    int(placeID))
        else:
            query = '''
                    MATCH (pe1:Person) MATCH (pe2:Person)
                    WHERE pe1.id = '{0}'
                    AND pe2.id = '{1}'
                    MERGE (pe1)-[r:MET {{date : date('{2}'), source : '{3}' }}]-(pe2)'''.format(person1ID, person2ID,
                                                                                                date, source)

        tx.run(query)

    def _checkLivesWith(self, tx, person1ID, person2ID) -> bool:

        query = '''
                RETURN EXISTS( (:Person {{id: '{0}'}})-[:LIVES_WITH]-(:Person {{id: '{1}'}}) )'''.format(person1ID,
                                                                                                         person2ID)

        result = tx.run(query)

        return bool(result.single()[0])


if __name__ == "__main__":
    start_time = time.time()
    handler = CovidGraphHandler("bolt://localhost:7687", "neo4j", "PablitoPablo990")
    handler.populateDatabase()
    handler.close()
    print("--- %s seconds ---" % (time.time() - start_time))

# dqyZajw7-gbE-nV-ZkPAaZ9kqBiJnATN2gJqmFsslfA

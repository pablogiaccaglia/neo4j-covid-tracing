from neo4j import GraphDatabase
import csv
import operator


class CovidGraphHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth = (user, password))
        self.session = self.driver.session()

        ''' Neo4j 'import' folder files paths'''
        self.italianCafesCSVInternalPath = 'file:///italian_cafes_standardized_no_nulls.csv'
        self.italianRestaurantsCSVInternalPath = 'file:///italian_restaurants_standardized_no_nulls.csv'
        self.italianCinemasCSVInternalPath = 'file:///italian_cinemas_standardized_no_nulls_with_streets.csv'
        self.italianHospitalsCSVInternalPath = 'file:///italian_hospitals_standardized_no_nulls.csv'
        self.italianTheatersCSVInternalPath = 'file:///italian_theaters_standardized_no_nulls.csv'
        self.italianCitiesCSVInternalPath = 'file:///italy_cities.csv'
        self.peopleCSVInternalPath = 'file:///people.csv'
        self.italianVaccines = 'file:///italian_vaccines.csv'

        ''' paths to CSV paths in the 'final' folder '''
        self.italianCafesCSVPath = 'datasets/final/italian_cafes_standardized_no_nulls.csv'
        self.italianRestaurantsCSVPath = 'datasets/final/italian_restaurants_standardized_no_nulls.csv'
        self.italianCinemasCSVPath = 'datasets/final/italian_cinemas_standardized_no_nulls.csv'
        self.italianHospitalsCSVPath = 'datasets/final/italian_hospitals_standardized_no_nulls.csv'
        self.italianTheatersCSVPath = 'datasets/final/italian_theaters_standardized_no_nulls.csv'
        self.peopleCSVPath = 'datasets/final/names.csv'

        self.italianPlacesCSVsPaths = [self.italianCafesCSVPath,
                                       self.italianRestaurantsCSVPath,
                                       self.italianTheatersCSVPath,
                                       self.italianHospitalsCSVPath,
                                       self.italianCinemasCSVPath]

    def close(self):
        self.driver.close()

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

    def createCityNodes(self):
        self.session.write_transaction(self._createCityNodes)

    def createCountryNodes(self):
        self.session.write_transaction(self._createCountryNodes)

    def createRelationships(self):
        self.session.write_transaction(self._createPartOfRelationship)
        self.session.write_transaction(self._createLocateRelationship)

    def _createPersonNodes(self, tx):
        query = """
        LOAD CSV WITH HEADERS FROM '{0}'
        AS line FIELDTERMINATOR ','
        with line
        CREATE (:Person {{name: line.name, surname: line.surname,
        fullname: line.fullname, sex: line.sex, birthDate: date(line.birth_date), id: line.id, idType: line.id_type}})
        """.format(self.peopleCSVInternalPath)

        tx.run(query)

    def _createCafeNodes(self, tx):
        query = """
                LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'cafe'}})
        """.format(self.italianCafesCSVInternalPath)

        tx.run(query)

    def _createCinemaNodes(self, tx):
        query = """
                LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'cinema'}})
        """.format(self.italianCinemasCSVInternalPath)

        tx.run(query)

    def _createRestaurantNodes(self, tx):
        query = """
                LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'restaurant'}})
                """.format(self.italianRestaurantsCSVInternalPath)

        tx.run(query)

    def _createHospitalNodes(self, tx):
        query = """LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'hospital'}})
                """.format(self.italianHospitalsCSVInternalPath)

        tx.run(query)

    def _createTheaterNodes(self, tx):
        query = """LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {{name: line.name, id: toInteger(line.ID), type: 'theater'}})
                """.format(self.italianTheatersCSVInternalPath)

        tx.run(query)

    def _createVaccineNodes(self, tx):
        query = """LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Vaccine {{name: line.vaccine}})""".format(self.italianVaccines)

        tx.run(query)

    def _createCityNodes(self, tx):
        query = """ LOAD CSV WITH HEADERS FROM '{0}'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:City {{name: line.city_ascii, id: toInteger(line.id), population: toInteger(line.population), region: line.admin_name}})
                """.format(self.italianCitiesCSVInternalPath)

        tx.run(query)

    @staticmethod
    def _createCountryNodes(tx):
        # since we are storing just Italian data, we do not need to store all these nodes
        #    tx.run('''
        #            LOAD CSV WITH HEADERS FROM
        #            'file:///countries.csv'
        #            AS line FIELDTERMINATOR ','
        #            with line
        #            CREATE (:Country {name: line.Country, population: toInteger(line.Population)})
        #    ''')

        tx.run('''
        CREATE (:Country {name: 'Italy', population: toInteger('60446035')});
        ''')

    def _createLocateRelationship(self, tx):
        for CSVPath in self.italianPlacesCSVsPaths:
            dictReader = csv.DictReader(open(CSVPath))

            for row in dictReader:
                """ the replace method is needed to escape single quotes, 
                    otherwise query will fail if string contains any """
                name = row['name'].replace("'", r"\'")
                city = row['city'].replace("'", r"\'")

                query = '''
                             MATCH (p:Place) MATCH (c:City)
                             WHERE p.name = '{0}'
                             AND c.name = '{1}'
                             MERGE (p)-[l:LOCATED]->(c);
                             '''.format(name, city)

                tx.run(query)

    @staticmethod
    def _createPartOfRelationship(tx):
        tx.run('''
                MATCH (ci:City) MATCH (co:Country)
                WHERE co.name = 'Italy'
                MERGE (ci)-[r:PART_OF]->(co);
                ''')

    def createRecievedVaccineRelationship(self):
        peopleData = csv.reader(open(self.peopleCSVPath))
        sortedData = sorted(peopleData, key = operator.itemgetter(4), reverse = False)


if __name__ == "__main__":
    handler = CovidGraphHandler("bolt://localhost:7687", "neo4j", "PablitoPablo990")
    handler.createRecievedVaccineRelationship()

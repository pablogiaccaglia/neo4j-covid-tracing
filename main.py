from neo4j import GraphDatabase


class CovidGraphHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth = (user, password))
        self.session = self.driver.session()

    def close(self):
        self.driver.close()

    def createPersonNodes(self):
        self.session.write_transaction(self._createPersonNodes)

    def createPlaceNodes(self):
        self.session.write_transaction(self._createCinemaNodes)
        self.session.write_transaction(self._createHospitalNodes)
        self.session.write_transaction(self._createRestaurantNodes)
        self.session.write_transaction(self._createTheaterNodes)

    def createVaccineNodes(self):
        self.session.write_transaction(self._createVaccineNodes)

    def createCityNodes(self):
        self.session.write_transaction(self._createCityNodes)

    def createCountryNodes(self):
        self.session.write_transaction(self._createCountryNodes)

    @staticmethod
    def _createPersonNodes(tx):
        tx.run('''
        LOAD CSV WITH HEADERS FROM
        'file:///people.csv'
        AS line FIELDTERMINATOR ','
        with line
        CREATE (:Person {name: line.name, surname: line.surname, 
        fullname: line.fullname, sex: line.sex, birthDate: date(line.birth_date), id: line.id,
        idType: line.id_type})
          ''')

    @staticmethod
    def _createCafeNodes(tx):
        tx.run()

    @staticmethod
    def _createCinemaNodes(tx):
        tx.run('''
                LOAD CSV WITH HEADERS FROM
                'file:///italian_cinemas_standardized_no_nulls_with_streets.csv'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {name: line.name, id: toInteger(line.ID), type: 'cinema'})
                  ''')

    @staticmethod
    def _createRestaurantNodes(tx):
        tx.run('''
                LOAD CSV WITH HEADERS FROM
                'file:///italian_restaurants_standardized_no_nulls.csv'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {name: line.name, id: toInteger(line.ID), type: 'restaurant'})
                  ''')

    @staticmethod
    def _createTheaterNodes(tx):
        tx.run('''
        LOAD CSV WITH HEADERS FROM
        'file:///italian_theaters_standardized_no_nulls.csv'
        AS line FIELDTERMINATOR ','
        with line
        CREATE (:Place {name: line.name, id: toInteger(line.ID), type: 'theater'})
          ''')

    @staticmethod
    def _createHospitalNodes(tx):
        tx.run('''
                LOAD CSV WITH HEADERS FROM
                'file:///italian_hospitals_standardized_no_nulls.csv'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Place {name: line.name, id: toInteger(line.ID), type: 'hospital'})
                  ''')

    @staticmethod
    def _createVaccineNodes(tx):
        tx.run('''
                LOAD CSV WITH HEADERS FROM
                'file:///italian_vaccines.csv'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:Vaccine {name: line.vaccine})
                  ''')

    @staticmethod
    def _createCityNodes(tx):
        tx.run('''
                LOAD CSV WITH HEADERS FROM
                'file:///italy_cities.csv'
                AS line FIELDTERMINATOR ','
                with line
                CREATE (:City {name: line.city_ascii, id: toInteger(line.id), population: toInteger(line.population), region: line.admin_name})
        ''')

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


if __name__ == "__main__":
    handler = CovidGraphHandler("DATABASE_URL", "USERNAME", "PASSWORD")
    handler.createPersonNodes()
    handler.createPlaceNodes()
    handler.createVaccineNodes()
    handler.createCityNodes()
    handler.createCountryNodes()
    handler.close()
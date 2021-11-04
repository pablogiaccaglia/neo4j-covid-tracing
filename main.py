from neo4j import GraphDatabase


class CovidGraphHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth = (user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self.createPerson)
            print(greeting)

    def testPersonsCreation(self):
        with self.driver.session() as session:
            session.write_transaction(self.createPerson)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message = message)
        return result.single()[0]

    @staticmethod
    def createPerson(tx):
        result = tx.run(""" foreach (i in range(0,1000) |
                            create (p:Person { uid : i })
                            set p += fkr.person('1930-01-01','2021-01-01')
                            remove p.uid
                            remove p.ssn);
                        """)


if __name__ == "__main__":
    greeter = CovidGraphHandler("DATABASE_URL", "USERNAME", "PASSWORD")
    greeter.testPersonsCreation()
    greeter.close()

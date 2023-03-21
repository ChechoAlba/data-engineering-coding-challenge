import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from extract import *

def connection():
    scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
    host_name = "localhost"
    port = 7687
    url = f"{scheme}://{host_name}:{port}"
    user = "neo4j"
    password = "codingchallenge"
    app = App(url, user, password)
    return app

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_protein(self, proteins):
        with self.driver.session() as session:
            elementIds = []
            for protein in proteins:
                result = session.execute_write(
                  self._create_and_return_protein, protein)
                elementIds.append(result)
                #print(result)
            return elementIds

    @staticmethod
    def _create_and_return_protein(tx, protein_id):

        # To learn more about the Cypher syntax,
        # see https://neo4j.com/docs/cypher-manual/current/

        # The Reference Card is also a good resource for keywords,
        # see https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p:Protein { id: $protein_id }) "
            "RETURN elementId(p)"
        )
        result = tx.run(query, protein_id=protein_id)
        try:
            return result.single().value()
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_names(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_name, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_name,result, element)

    @staticmethod
    def _create_name(tx, name):
        query = (
                "CREATE (n:Name { name: $name , type: $type}) "
                "RETURN elementId(n)"
            )
        result = tx.run(query, name=name['name'], type=name['type'])
        try:
            return result.single().value()
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
    
    @staticmethod
    def _create_relationship_protein_name(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Name) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:HAS_NAME]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_genes(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result  = session.execute_write(
                    self._create_gene, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_gene,result, element)

    @staticmethod
    def _create_gene(tx, name):
            query = (
                    "CREATE (g:Gene { name: $name , type: $type}) "
                    "RETURN elementId (g)"
                )
            result = tx.run(query, name=name['name'], type=name['type'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise
    
    @staticmethod
    def _create_relationship_protein_gene(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Gene) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:FROM_GENE]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_organisms(self, data, elementIds):
        with self.driver.session() as session:
            result = session.execute_write(
                self._create_organisms, data)
            session.execute_write(
                self._create_lineage, result, data)
            for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_organism,result, element)

    @staticmethod
    def _create_organisms(tx, name):
            print(name)
            query = (
                    "CREATE (g:Organism { name: $name , type: $type}) "
                    "RETURN elementId(g)"
                )
            result = tx.run(query, name=name['name'], type=name['type'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise
    
    @staticmethod
    def _create_lineage(tx, elementId, name):
            lineages = name['lineage']
            for element in lineages:
                query = (
                        "MATCH (o:Organism) WHERE elementId(o) = $elementId "
                        "CREATE (t:taxon { id: $name}) "
                        "CREATE (o)-[pr:HAS_LINEAGE]->(t) "
                        "RETURN t"
                    )
                result = tx.run(query, name=element, elementId= elementId )
                try:
                    result.single()[0]
                    # Capture any errors along with the query and data for traceability
                except ServiceUnavailable as exception:
                    logging.error("{query} raised an error: \n {exception}".format(
                        query=query, exception=exception))
                    raise

    @staticmethod
    def _create_relationship_protein_organism(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Organism) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:IN_ORGANISM]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_references(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_references, element)
                session.execute_write(
                    self._create_authors, result, element)
                session.execute_write(
                    self._create_scopes, result, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_reference, result, element)
                
    @staticmethod
    def _create_references(tx, data):
            query = (
                    "CREATE (r:Reference { key: $key , title: $title}) "
                    "RETURN elementId(r)"
                )
            result = tx.run(query, key=data['key'], title=data['title'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    @staticmethod
    def _create_authors(tx, elementId, data):
            authors = data['authors']
            for element in authors:
                query = (
                        "MATCH (r:Reference) WHERE elementId(r) = $key "
                        "CREATE (a:Author { name: $name}) "
                        "CREATE (r)-[pr:WAS_WRITED]->(a) "
                        "RETURN a"
                    )
                result = tx.run(query, name=element, key= elementId )
                try:
                    result.single()[0]
                    # Capture any errors along with the query and data for traceability
                except ServiceUnavailable as exception:
                    logging.error("{query} raised an error: \n {exception}".format(
                        query=query, exception=exception))
                    raise
    
    @staticmethod
    def _create_scopes(tx, elementId, data):
            scopes = data['scopes']
            for element in scopes:
                query = (
                        "MATCH (r:Reference) WHERE elementId(r) = $key "
                        "CREATE (s:Scope { text: $name}) "
                        "CREATE (r)-[pr:HAS_SCOPE]->(s) "
                        "RETURN s"
                    )
                result = tx.run(query, name=element, key= elementId )
                try:
                    result.single()[0]
                    # Capture any errors along with the query and data for traceability
                except ServiceUnavailable as exception:
                    logging.error("{query} raised an error: \n {exception}".format(
                        query=query, exception=exception))
                    raise

    @staticmethod
    def _create_relationship_protein_reference(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Reference) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:WAS_REFERENCED_IN]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_comments(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_comments, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_comment,result, element)
                
    @staticmethod
    def _create_comments(tx, data):
            query = (
                    "CREATE (c:Comments { text: $text , type: $type}) "
                    "RETURN elementId(c)"
                )
            result = tx.run(query, text=data['text'], type=data['type'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    @staticmethod
    def _create_relationship_protein_comment(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Comments) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:HAS_COMMENTS]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_dbReferences(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_dbReferences, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_dbReference,result, element)
                
    @staticmethod
    def _create_dbReferences(tx, data):
            query = (
                    "CREATE (dr:DbReferences { id: $id , type: $type}) "
                    "RETURN elementId(dr)"
                )
            result = tx.run(query, id=data['id'], type=data['type'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    @staticmethod
    def _create_relationship_protein_dbReference(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:DbReferences) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:HAS_DB_REFERENCED]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_keywords(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_keywords, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_keyword,result, element)
                
    @staticmethod
    def _create_keywords(tx, data):
            query = (
                    "CREATE (k:Keywords { id: $id , text: $text}) "
                    "RETURN elementId(k)"
                )
            result = tx.run(query, id=data['id'], text=data['text'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    @staticmethod
    def _create_relationship_protein_keyword(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Keywords) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:HAS_KEYWORDS]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
    
    def create_features(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_features, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_feature,result, element)
                
    @staticmethod
    def _create_features(tx, data):
            query = (
                    "CREATE (f:features { id: $id , type: $type, description: $description, evidence: $evidence }) "
                    "RETURN elementId(f)"
                )
            result = tx.run(query, id=data.get('id', None), type=data.get('type', None), description=data.get('description', None), evidence=data.get('evidence', None))
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    @staticmethod
    def _create_relationship_protein_feature(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:features) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:HAS_FEATURE]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_evidences(self, data, elementIds):
        with self.driver.session() as session:
            for element in data:
                result = session.execute_write(
                    self._create_evidences, element)
                for element in elementIds:
                    session.execute_write(
                        self._create_relationship_protein_evidence,result, element)
                
    @staticmethod
    def _create_evidences(tx, data):
            query = (
                    "CREATE (e:Evidences { key: $key , type: $type }) "
                    "RETURN elementId(e)"
                )
            result = tx.run(query, key=data['key'], type=data['type'])
            try:
                return result.single().value()
                # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    @staticmethod
    def _create_relationship_protein_evidence(tx, nameId, proteinId):
        query = (
                "MATCH (p:Protein) where elementId(p) = $proteinId "
                "MATCH (b:Evidences) where elementId(b) = $nameId  "
                "CREATE (p)-[pr:IN_EVIDENCE]->(b)"
                "RETURN pr "
            )
        result = tx.run(query, nameId=nameId, proteinId = proteinId )
        try:
            return result.single()[0]
            # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

if __name__ == "__main__":
    # See https://neo4j.com/developer/aura-connect-driver/ for Aura specific connection URL.
    app = connection()
    root = define_root()
    try:
        proteins = write_protein(root)
        elementIds = app.create_protein(proteins)
        names = write_name(root)
        app.create_names(names, elementIds)
        genes = write_gen(root)
        app.create_genes(genes, elementIds)
        organisms = write_organism(root)
        app.create_organisms(organisms, elementIds)
        ###################################
        references = write_reference(root)
        app.create_references(references, elementIds)
        comments = write_comments(root)
        app.create_comments(comments, elementIds)
        dbReferences = write_dbReference(root)
        app.create_dbReferences(dbReferences, elementIds)
        keywords = write_keyword(root)
        app.create_keywords(keywords, elementIds)
        features = write_feature(root)
        app.create_features(features, elementIds)
        evidences = write_evidence(root)
        app.create_evidences(evidences, elementIds)
    finally:
        app.close()
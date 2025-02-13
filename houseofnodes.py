from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
##ejercicio 1

class MovieDatabase:
    def __init__(self, uri, username, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            self.driver.verify_connectivity()
            logger.info("Conexión establecida exitosamente con Neo4j")
        except Exception as e:
            logger.error(f"Error de conexión: {e}")
            raise

    def clear_database(self):
        """Limpia todos los datos de la base de datos"""
        try:
            with self.driver.session(database="neo4j") as session:
                session.run("MATCH (n) DETACH DELETE n")
                logger.info("Base de datos limpiada exitosamente")
        except Exception as e:
            logger.error(f"Error al limpiar la base de datos: {e}")
            raise

    def create_or_get_user(self, user_id, name):
        """Crea un usuario si no existe, o lo obtiene si ya existe"""
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.execute_write(self._create_or_get_user, user_id, name)
                logger.info(f"Usuario procesado: {name}")
                return result
        except Exception as e:
            logger.error(f"Error al procesar usuario: {e}")
            raise

    @staticmethod
    def _create_or_get_user(tx, user_id, name):
        query = """
        MERGE (u:USER {userId: $user_id})
        ON CREATE SET u.name = $name
        RETURN u
        """
        result = tx.run(query, user_id=user_id, name=name)
        return result.single()

    def create_or_get_movie(self, movie_id, title, year, plot):
        """Crea una película si no existe, o la obtiene si ya existe"""
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.execute_write(
                    self._create_or_get_movie, movie_id, title, year, plot
                )
                logger.info(f"Película procesada: {title}")
                return result
        except Exception as e:
            logger.error(f"Error al procesar película: {e}")
            raise

    @staticmethod
    def _create_or_get_movie(tx, movie_id, title, year, plot):
        query = """
        MERGE (m:MOVIE {movieId: $movie_id})
        ON CREATE SET m.title = $title, m.year = $year, m.plot = $plot
        RETURN m
        """
        result = tx.run(query, movie_id=movie_id, title=title, 
                       year=year, plot=plot)
        return result.single()

    def create_or_update_rating(self, user_id, movie_id, rating):
        """Crea o actualiza un rating entre usuario y película"""
        if not (0 <= rating <= 5):
            raise ValueError("Rating debe estar entre 0 y 5")
            
        timestamp = int(datetime.now().timestamp())
        
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.execute_write(
                    self._create_or_update_rating, user_id, movie_id, rating, timestamp
                )
                logger.info(f"Rating procesado: Usuario {user_id} -> Película {movie_id}")
                return result
        except Exception as e:
            logger.error(f"Error al procesar rating: {e}")
            raise

    @staticmethod
    def _create_or_update_rating(tx, user_id, movie_id, rating, timestamp):
        query = """
        MATCH (u:USER {userId: $user_id})
        MATCH (m:MOVIE {movieId: $movie_id})
        MERGE (u)-[r:RATED]->(m)
        SET r.rating = $rating, r.timestamp = $timestamp
        RETURN r
        """
        result = tx.run(query, user_id=user_id, movie_id=movie_id, 
                       rating=rating, timestamp=timestamp)
        return result.single()

    def create_constraints(self):
        """Crea restricciones de unicidad si no existen"""
        try:
            with self.driver.session(database="neo4j") as session:
                session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:USER) REQUIRE u.userId IS UNIQUE")
                session.run("CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:MOVIE) REQUIRE m.movieId IS UNIQUE")
                logger.info("Constraints verificados")
        except Exception as e:
            logger.error(f"Error al crear constraints: {e}")
            raise

    def close(self):
        if hasattr(self, 'driver'):
            self.driver.close()
            logger.info("Conexión cerrada")




if __name__ == "__main__":
    URI = "neo4j+s://c17208d5.databases.neo4j.io"
    USERNAME = "neo4j"
    PASSWORD = "MYeqn7MYLvzVkXDFB81JQhNafMAWIxsIwQ3v1cpy4GI"

    db = None
    try:
        db = MovieDatabase(URI, USERNAME, PASSWORD)
                
        db.create_constraints()
        ##ejercicio 2
        db.create_or_get_user("user1", "Juan Pérez")
        db.create_or_get_user("user2", "María López")
        db.create_or_get_user("user3", "Carlos Ruiz")
        db.create_or_get_user("user4", "Ana García")
        db.create_or_get_user("user5", "Diego Martín")
        
        db.create_or_get_movie(1, "El Padrino", 1972, "La historia de una familia mafiosa...")
        db.create_or_get_movie(2, "The Matrix", 1999, "Un programador descubre que la realidad es una simulación...")
        db.create_or_get_movie(3, "Inception", 2010, "Un ladrón que roba secretos corporativos...")
        db.create_or_get_movie(4, "Pulp Fiction", 1994, "Las vidas de dos asesinos a sueldo...")
        db.create_or_get_movie(5, "The Shawshank Redemption", 1994, "Un banquero es condenado...")
        
        db.create_or_update_rating("user1", 1, 5)
        db.create_or_update_rating("user1", 2, 4)
        db.create_or_update_rating("user2", 1, 3)
        db.create_or_update_rating("user2", 3, 5)
        db.create_or_update_rating("user3", 2, 5)
        db.create_or_update_rating("user3", 4, 4)
        db.create_or_update_rating("user4", 3, 4)
        db.create_or_update_rating("user4", 5, 5)
        db.create_or_update_rating("user5", 4, 5)
        db.create_or_update_rating("user5", 1, 4)
        
    except Exception as e:
        logger.error(f"Error en la ejecución del programa: {e}")
    finally:
        if db:
            db.close()



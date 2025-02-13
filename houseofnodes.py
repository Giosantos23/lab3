from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MovieDatabase:
    def __init__(self, uri, username, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            self.driver.verify_connectivity()
            logger.info("Conexión establecida exitosamente con Neo4j")
        except Exception as e:
            logger.error(f"Error de conexión: {e}")
            raise

    def close(self):
        if hasattr(self, 'driver'):
            self.driver.close()
            logger.info("Conexión cerrada")

    def find_user(self, user_id):
        """Busca un usuario por su ID"""
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.execute_read(self._find_user, user_id)
                return result
        except Exception as e:
            logger.error(f"Error al buscar usuario {user_id}: {e}")
            raise

    @staticmethod
    def _find_user(tx, user_id):
        query = "MATCH (u:USER {userId: $user_id}) RETURN u"
        result = tx.run(query, user_id=user_id)
        return result.single()

    def find_movie(self, movie_id):
        """Busca una película por su ID"""
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.execute_read(self._find_movie, movie_id)
                return result
        except Exception as e:
            logger.error(f"Error al buscar película {movie_id}: {e}")
            raise

    @staticmethod
    def _find_movie(tx, movie_id):
        query = "MATCH (m:MOVIE {movieId: $movie_id}) RETURN m"
        result = tx.run(query, movie_id=movie_id)
        return result.single()

    def find_user_rating(self, user_id, movie_id):
        """Busca la relación RATED entre un usuario y una película"""
        try:
            with self.driver.session(database="neo4j") as session:
                result = session.execute_read(self._find_user_rating, user_id, movie_id)
                return result
        except Exception as e:
            logger.error(f"Error al buscar rating de usuario {user_id} a película {movie_id}: {e}")
            raise

    @staticmethod
    def _find_user_rating(tx, user_id, movie_id):
        query = """
        MATCH (u:USER {userId: $user_id})-[r:RATED]->(m:MOVIE {movieId: $movie_id})
        RETURN u, r, m
        """
        result = tx.run(query, user_id=user_id, movie_id=movie_id)
        return result.single()

if __name__ == "__main__":
    URI = "neo4j+s://c17208d5.databases.neo4j.io"
    USERNAME = "neo4j"
    PASSWORD = "MYeqn7MYLvzVkXDFB81JQhNafMAWIxsIwQ3v1cpy4GI"

    db = None
    try:
        db = MovieDatabase(URI, USERNAME, PASSWORD)
##Problema 2         
        #db.create_constraints()
        ##ejercicio 2
        #db.create_or_get_user("user1", "Juan Pérez")
        #db.create_or_get_user("user2", "María López")
        #db.create_or_get_user("user3", "Carlos Ruiz")
        #db.create_or_get_user("user4", "Ana García")
        #db.create_or_get_user("user5", "Diego Martín")
        
        #db.create_or_get_movie(1, "El Padrino", 1972, "La historia de una familia mafiosa...")
        #db.create_or_get_movie(2, "The Matrix", 1999, "Un programador descubre que la realidad es una simulación...")
        #db.create_or_get_movie(3, "Inception", 2010, "Un ladrón que roba secretos corporativos...")
        #db.create_or_get_movie(4, "Pulp Fiction", 1994, "Las vidas de dos asesinos a sueldo...")
        #db.create_or_get_movie(5, "The Shawshank Redemption", 1994, "Un banquero es condenado...")
        
        #db.create_or_update_rating("user1", 1, 5)
        #db.create_or_update_rating("user1", 2, 4)
        #db.create_or_update_rating("user2", 1, 3)
        #db.create_or_update_rating("user2", 3, 5)
        #db.create_or_update_rating("user3", 2, 5)
        #db.create_or_update_rating("user3", 4, 4)
        #db.create_or_update_rating("user4", 3, 4)
        #db.create_or_update_rating("user4", 5, 5)
        #db.create_or_update_rating("user5", 4, 5)
        #db.create_or_update_rating("user5", 1, 4)
##Problema 3

        user = db.find_user("user1")
        print(user)
        
        movie = db.find_movie(1)
        print(movie)
        
        rating = db.find_user_rating("user1", 1)
        print(rating)
        
    except Exception as e:
        logger.error(f"Error en la ejecución del programa: {e}")
    finally:
        if db:
            db.close()#

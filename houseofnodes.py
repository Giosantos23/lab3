from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
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

    def cerrar(self):
        if hasattr(self, 'driver'):
            self.driver.close()
            logger.info("Conexión cerrada")

    def crear_usuario(self, user_id, name):
        query = """
        MERGE (u:User {userId: $user_id})
        ON CREATE SET u.name = $name
        RETURN u
        """
        with self.driver.session() as session:
            return session.run(query, user_id=user_id, name=name).single()

    def crear_pelicula(self, movie_id, title, tmdbId, released, imdbRating, year, imdbId, runtime, countries, imdbVotes, url, revenue, plot, poster, budget, languages):
        query = """
        MERGE (m:Movie {movieId: $movie_id})
        ON CREATE SET m.title = $title, m.tmdbId = $tmdbId, m.released = $released, m.imdbRating = $imdbRating,
        m.year = $year, m.imdbId = $imdbId, m.runtime = $runtime, m.countries = $countries, m.imdbVotes = $imdbVotes,
        m.url = $url, m.revenue = $revenue, m.plot = $plot, m.poster = $poster, m.budget = $budget, m.languages = $languages
        RETURN m
        """
        with self.driver.session() as session:
            return session.run(query, movie_id=movie_id, title=title, tmdbId=tmdbId, released=released, imdbRating=imdbRating,
                               year=year, imdbId=imdbId, runtime=runtime, countries=countries, imdbVotes=imdbVotes,
                               url=url, revenue=revenue, plot=plot, poster=poster, budget=budget, languages=languages).single()

    def crear_persona(self, name, tmdbId, born, died, bornIn, url, imdbId, bio, poster, roles, movie_id):
        query = """
        MERGE (p:Person {name: $name, tmdbId: $tmdbId, born: $born, bornIn: $bornIn,
                         url: $url, imdbId: $imdbId, bio: $bio, poster: $poster})
        ON CREATE SET p.died = CASE WHEN $died IS NOT NULL THEN $died ELSE p.died END
        WITH p
        MATCH (m:Movie {movieId: $movie_id})
        FOREACH (role IN $roles |
            MERGE (p)-[:ACTED_IN {role: role}]->(m)
        )
        WITH p, m, $roles AS roles
        FOREACH (_ IN CASE WHEN "Director" IN roles THEN [1] ELSE [] END |
            MERGE (p)-[:DIRECTED]->(m)
        )
        RETURN p, m
        """
        with self.driver.session() as session:
            return session.run(query, name=name, tmdbId=tmdbId, born=born, died=died, bornIn=bornIn, url=url,
                               imdbId=imdbId, bio=bio, poster=poster, roles=roles, movie_id=movie_id).single()
    
    def crear_rating(self, user_id, movie_id, rating, timestamp):
        query = """
        MATCH (u:User {userId: $user_id}), (m:Movie {movieId: $movie_id})
        MERGE (u)-[r:RATED]->(m)
        ON CREATE SET r.rating = $rating, r.timestamp = $timestamp
        RETURN u, r, m
        """
        with self.driver.session() as session:
            return session.run(query, user_id=user_id, movie_id=movie_id, rating=rating, timestamp=timestamp).single()

if __name__ == "__main__":
    URI = "neo4j+s://697a5cac.databases.neo4j.io"
    USERNAME = "neo4j"
    PASSWORD = "wqgBVLKGDUpISy7FFXE1_siBCV3Xg4nUs9SEjxYe0Us"
    
    db = MovieDatabase(URI, USERNAME, PASSWORD)
    
    usuarios = [
        ("user1", "Juan Pérez"),
        ("user2", "María López"),
        ("user3", "Carlos Ruiz"),
        ("user4", "Ana García"),
        ("user5", "Diego Martín")
    ]
    for user_id, name in usuarios:
        db.crear_usuario(user_id, name)
    
    peliculas = [
        (1, "El Padrino", 238, "1972-03-14", 9.2, 1972, 238, 175, ["USA"], 1500000, "https://www.imdb.com/title/tt0068646/", 245066411, "Historia de la mafia italiana", "padrino.jpg", 6000000, ["English", "Italian"]),
        (2, "The Matrix", 603, "1999-03-31", 8.7, 1999, 603, 136, ["USA"], 1700000, "https://www.imdb.com/title/tt0133093/", 463517383, "Realidad simulada", "matrix.jpg", 63000000, ["English"])
    ]
    for movie in peliculas:
        db.crear_pelicula(*movie)
    
    ratings = [
        ("user1", 1, 5, 1672531200), ("user1", 2, 4, 1672617600),
        ("user2", 1, 3, 1672704000), ("user2", 2, 5, 1672790400),
        ("user3", 1, 4, 1672876800), ("user3", 2, 3, 1672963200),
        ("user4", 1, 5, 1673049600), ("user4", 2, 5, 1673136000),
        ("user5", 1, 3, 1673222400), ("user5", 2, 4, 1673308800)
    ]
    for rating in ratings:
        db.crear_rating(*rating)
        
    personas = [
        ("Marlon Brando", 123, "1924-04-03", "2004-07-01", "USA", "https://www.imdb.com/name/nm0000008/", 8, "Legendario actor", "brando.jpg", ["Actor"], 1),
        ("Francis Ford Coppola", 456, "1939-04-07", None, "USA", "https://www.imdb.com/name/nm0000338/", 338, "Director de El Padrino", "coppola.jpg", ["Director"], 1),
        ("Keanu Reeves", 789, "1964-09-02", None, "Lebanon", "https://www.imdb.com/name/nm0000206/", 206, "Actor en Matrix", "reeves.jpg", ["Actor"], 2),
        ("Lana Wachowski", 987, "1965-06-21", None, "USA", "https://www.imdb.com/name/nm0905154/", 154, "Directora de The Matrix", "wachowski.jpg", ["Director"], 2),
        ("Quentin Tarantino", 159, "1963-03-27", None, "USA", "https://www.imdb.com/name/nm0000233/", 233, "Director y Actor", "tarantino.jpg", ["Director", "Actor"], 2)
    ]
    for person in personas:
        db.crear_persona(*person)
    
    db.cerrar()

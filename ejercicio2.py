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

    def crear_usuario(self, usuario_id, name):
        query = """
        MERGE (u:User {userId: $usuario_id})
        ON CREATE SET u.name = $name
        RETURN u
        """
        with self.driver.session() as session:
            return session.run(query, usuario_id=usuario_id, name=name).single()

    def crear_pelicula(self, pelicula_id, title, year, plot):
        query = """
        MERGE (m:Movie {movieId: $pelicula_id})
        ON CREATE SET m.title = $title, m.year = $year, m.plot = $plot
        RETURN m
        """
        with self.driver.session() as session:
            return session.run(query, pelicula_id=pelicula_id, title=title, year=year, plot=plot).single()
    
    def crear_rating(self, usuario_id, pelicula_id, rating, timestamp):
        query = """
        MATCH (u:User {userId: $usuario_id}), (m:Movie {movieId: $pelicula_id})
        MERGE (u)-[r:RATED]->(m)
        ON CREATE SET r.rating = $rating, r.timestamp = $timestamp
        RETURN u, r, m
        """
        with self.driver.session() as session:
            return session.run(query, usuario_id=usuario_id, pelicula_id=pelicula_id, rating=rating, timestamp=timestamp).single()
    
    def buscar_usuario(self, usuario_id):
        query = "MATCH (u:User {userId: $usuario_id}) RETURN u"
        with self.driver.session() as session:
            return session.run(query, usuario_id=usuario_id).single()
    
    def buscar_pelicula(self, pelicula_id):
        query = "MATCH (m:Movie {movieId: $pelicula_id}) RETURN m"
        with self.driver.session() as session:
            return session.run(query, pelicula_id=pelicula_id).single()
    
    def buscar_usuario_rating(self, usuario_id, pelicula_id):
        query = """
        MATCH (u:User {userId: $usuario_id})-[r:RATED]->(m:Movie {movieId: $pelicula_id})
        RETURN u, r, m
        """
        with self.driver.session() as session:
            return session.run(query, usuario_id=usuario_id, pelicula_id=pelicula_id).single()

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
    for usuario_id, name in usuarios:
        db.crear_usuario(usuario_id, name)
    
    peliculas = [
        (1, "El Padrino", 1972, "Historia de la mafia italiana"),
        (2, "The Matrix", 1999, "Un programador descubre que la realidad es una simulación"),
        (3, "Inception", 2010, "Un ladrón entra en los sueños de las personas"),
        (4, "Interstellar", 2014, "Un grupo de astronautas busca un nuevo hogar para la humanidad"),
        (5, "Pulp Fiction", 1994, "Historias entrelazadas de crimen y redención")
    ]
    for pelicula in peliculas:
        db.crear_pelicula(*pelicula)
    
    ratings = [
        ("user1", 1, 5, 1672531200), ("user1", 2, 4, 1672617600),
        ("user2", 3, 3, 1672704000), ("user2", 4, 5, 1672790400),
        ("user3", 1, 4, 1672876800), ("user3", 5, 3, 1672963200),
        ("user4", 2, 5, 1673049600), ("user4", 3, 4, 1673136000),
        ("user5", 4, 3, 1673222400), ("user5", 5, 5, 1673308800)
    ]
    for rating in ratings:
        db.crear_rating(*rating)
    
    while True:
        print("\nMenú:")
        print("1. Buscar usuario")
        print("2. Buscar película")
        print("3. Buscar relación RATED entre usuario y película")
        print("4. Salir")
        opcion = input("Seleccione una opción: ")

        if opcion == "1":
            usuario_id = input("Ingrese el ID del usuario: ")
            print(db.buscar_usuario(usuario_id))
        elif opcion == "2":
            pelicula_id = int(input("Ingrese el ID de la película: "))
            print(db.buscar_pelicula(pelicula_id))
        elif opcion == "3":
            usuario_id = input("Ingrese el ID del usuario: ")
            pelicula_id = int(input("Ingrese el ID de la película: "))
            print(db.buscar_usuario_rating(usuario_id, pelicula_id))
        elif opcion == "4":
            break
        else:
            print("Opción inválida. Intente nuevamente.")
    
    db.cerrar()

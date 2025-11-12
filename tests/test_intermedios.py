import psycopg2
import pytest
from psycopg2.extras import register_hstore

# Configuración de la base de datos de PRUEBA
DB_CONFIG = {
    "dbname": "test_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}

@pytest.fixture
def db_cursor():
    """
    Fixture de Pytest que gestiona la conexión Y
    ASEGURA QUE LAS TABLAS ESTÉN LIMPIAS antes de cada prueba.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        register_hstore(conn)
        cur = conn.cursor()

        # Vacía las tablas antes de que la prueba comience
        # RESTART IDENTITY reinicia los contadores SERIAL (buena práctica)
        cur.execute("TRUNCATE TABLE productos, productos2 RESTART IDENTITY;")
        
        yield cur
        
    finally:
        if conn:
            conn.rollback()  # Deshace lo que la prueba haya hecho
            conn.close()

# --- Pruebas Unitarias ---

def test_filtrar_por_existencia_de_clave(db_cursor):
    """
    Prueba el Ejercicio 1: Usar el operador ? para encontrar claves.
    """
    # 1. Preparación
    db_cursor.execute(
        "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)",
        ('Con Marca', {"marca": "Sony", "tipo": "Audio"})
    )
    db_cursor.execute(
        "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)",
        ('Sin Marca', {"tipo": "Mueble", "color": "madera"})
    )

    # 2. Ejecución
    query = "SELECT nombre FROM productos2 WHERE atributos ? 'marca'"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 1
    assert resultados[0][0] == 'Con Marca'

def test_combinar_hstore_y_columna(db_cursor):
    """
    Prueba el Ejercicio 2: Combinar HSTORE (marca) y columna (precio).
    """
    # 1. Preparación
    insert_query = "INSERT INTO productos2 (nombre, precio, atributos) VALUES (%s, %s, %s)"
    db_cursor.execute(insert_query, 
        ('Sony Caro', 550.00, {"marca": "Sony", "color": "negro"})
    )
    db_cursor.execute(insert_query, 
        ('Sony Barato', 300.00, {"marca": "Sony", "color": "blanco"})
    )
    db_cursor.execute(insert_query, 
        ('Logitech Caro', 600.00, {"marca": "Logitech", "color": "gris"})
    )

    # 2. Ejecución
    query = """
    SELECT nombre FROM productos2 
    WHERE (atributos -> 'marca' = 'Sony') AND (precio > 500)
    """
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 1
    assert resultados[0][0] == 'Sony Caro'

def test_extraer_claves_y_valores(db_cursor):
    """
    Prueba el Ejercicio 3: Usar skeys() y svals().
    """
    # 1. Preparación
    attrs = {"color": "rojo", "tipo": "Ropa", "talla": "M"}
    db_cursor.execute(
        "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)",
        ('Camisa', attrs)
    )

    # 2. Ejecución
    # Usamos array_agg para obtener un array de arrays, más fácil de probar
    query = """
    SELECT skeys(atributos), svals(atributos) 
    FROM productos2 WHERE nombre = 'Camisa'
    """
    db_cursor.execute(query)
    claves, valores = db_cursor.fetchone()

    # 3. Validación
    # Convertimos a 'set' porque el orden de HSTORE no está garantizado
    assert set(claves) == {"color", "tipo", "talla"}
    assert set(valores) == {"rojo", "Ropa", "M"}

def test_contar_productos_con_atributo(db_cursor):
    """
    Prueba el Ejercicio 4: Contar productos con una clave específica.
    """
    # 1. Preparación
    insert_query = "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)"
    db_cursor.execute(insert_query, ('Con Color', {"color": "rojo"}))
    db_cursor.execute(insert_query, ('Con Color 2', {"color": "azul", "peso": "10"}))
    db_cursor.execute(insert_query, ('Sin Color', {"peso": "5"}))

    # 2. Ejecución
    query = "SELECT count(*) FROM productos2 WHERE atributos ? 'color'"
    db_cursor.execute(query)
    conteo = db_cursor.fetchone()[0]

    # 3. Validación
    assert conteo == 2

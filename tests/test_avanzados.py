import psycopg2
import pytest
from psycopg2.extras import register_hstore
import json # Necesario para la prueba de jsonb_to_hstore

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
    Fixture de Pytest para gestionar la conexión y transacciones de la BD.
    Se conecta, registra el adaptador HSTORE y hace rollback al final.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        register_hstore(conn)  # ¡Crucial para HSTORE!
        cur = conn.cursor()
        yield cur
        
    finally:
        if conn:
            conn.rollback()  # Limpia la BD después de CADA prueba
            conn.close()

# --- Pruebas Unitarias ---

def test_agrupar_por_marca_hstore(db_cursor):
    """
    Prueba la consulta GROUP BY sobre una clave HSTORE.
    """
    # 1. Preparación
    productos_data = [
        ('Sony TV', 800.00, {"marca": "Sony", "tipo": "TV"}),
        ('Sony Audifonos', 550.00, {"marca": "Sony", "tipo": "Audio"}),
        ('Logitech Mouse', 130.00, {"marca": "Logitech", "tipo": "Mouse"}),
        ('Producto Sin Marca', 10.00, {"tipo": "Genérico"})
    ]
    insert_query = "INSERT INTO productos2 (nombre, precio, atributos) VALUES (%s, %s, %s)"
    db_cursor.executemany(insert_query, productos_data)

    # 2. Ejecución
    query = """
    SELECT atributos -> 'marca' AS marca, COUNT(*)
    FROM productos2
    GROUP BY marca
    """
    db_cursor.execute(query)
    
    # Convertimos la lista de tuplas (ej. [('Sony', 2), ('Logitech', 1), (None, 1)])
    # a un diccionario para facilitar la validación.
    resultados = dict(db_cursor.fetchall())

    # 3. Validación
    assert resultados['Sony'] == 2
    assert resultados['Logitech'] == 1
    assert resultados[None] == 1  # Productos sin clave 'marca' se agrupan como NULL

def test_hstore_to_jsonb(db_cursor):
    """
    Prueba la conversión de HSTORE a JSONB.
    """
    # 1. Preparación
    attrs_hstore = {"marca": "Logitech", "dpi": "16000", "color": "blanco"}
    db_cursor.execute(
        "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)",
        ('Logitech G Pro', attrs_hstore)
    )

    # 2. Ejecución
    query = "SELECT hstore_to_jsonb(atributos) FROM productos2 WHERE nombre = 'Logitech G Pro'"
    db_cursor.execute(query)
    
    # psycopg2 convierte automáticamente el tipo JSONB a un dict de Python
    jsonb_result = db_cursor.fetchone()[0]

    # 3. Validación
    assert jsonb_result == attrs_hstore

def test_funcion_jsonb_to_hstore(db_cursor):
    """
    Prueba la función personalizada jsonb_to_hstore.
    """
    # 1. Preparación
    test_json = {"clave_nueva": "valor_nuevo", "prioridad": "alta"}
    # El HSTORE esperado (psycopg2 lo devolverá como dict)
    expected_hstore = {"clave_nueva": "valor_nuevo", "prioridad": "alta"}

    # 2. Ejecución
    # Pasamos el JSON como un string (usando json.dumps)
    query = "SELECT jsonb_to_hstore(%s::jsonb)"
    db_cursor.execute(query, (json.dumps(test_json),))
    
    # psycopg2 convierte automáticamente el HSTORE resultante a un dict de Python
    hstore_result = db_cursor.fetchone()[0]

    # 3. Validación
    assert hstore_result == expected_hstore

def test_operador_contiene_todas_las_claves(db_cursor):
    """
    Prueba el operador ?& (contiene todas las claves).
    """
    # 1. Preparación
    insert_query = "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)"
    db_cursor.execute(insert_query, 
        ('Producto Completo', {"color": "rojo", "peso": "10", "marca": "A"})
    )
    db_cursor.execute(insert_query, 
        ('Solo Color', {"color": "azul", "marca": "B"})
    )
    db_cursor.execute(insert_query, 
        ('Solo Peso', {"peso": "5", "marca": "C"})
    )

    # 2. Ejecución
    query = "SELECT nombre FROM productos2 WHERE atributos ?& ARRAY['color', 'peso']"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 1
    assert resultados[0][0] == 'Producto Completo'

def test_funcion_resumir_producto(db_cursor):
    """
    Prueba la función personalizada resumir_producto, 
    incluyendo los casos 'N/A'.
    """
    # 1. Preparación
    insert_query = "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)"
    # Producto con todos los datos
    db_cursor.execute(insert_query, 
        ('Sony TV', {"marca": "Sony", "color": "negro", "tipo": "TV"})
    )
    # Producto sin 'marca' ni 'color'
    db_cursor.execute(insert_query, 
        ('Caja Genérica', {"tipo": "carton", "tamano": "grande"})
    )
    # Producto solo con 'marca'
    db_cursor.execute(insert_query, 
        ('Silla', {"marca": "IKEA", "material": "madera"})
    )

    # 2. Ejecución
    query = "SELECT nombre, resumir_producto(nombre, atributos) AS resumen FROM productos2"
    db_cursor.execute(query)
    
    # Convertimos a dict para fácil acceso
    resumenes = {nombre: resumen for nombre, resumen in db_cursor.fetchall()}

    # 3. Validación
    assert resumenes['Sony TV'] == "Producto Sony TV: marca=Sony, color=negro"
    assert resumenes['Caja Genérica'] == "Producto Caja Genérica: marca=N/A, color=N/A"
    assert resumenes['Silla'] == "Producto Silla: marca=IKEA, color=N/A"

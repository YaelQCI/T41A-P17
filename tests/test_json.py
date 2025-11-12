import psycopg2
import pytest
import json

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

    - Se conecta ANTES de cada prueba.
    - Inicia una transacción.
    - Entrega (yield) el cursor a la prueba.
    - Hace un ROLLBACK DESPUÉS de cada prueba para limpiar la BD.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        yield cur
    finally:
        if conn:
            conn.rollback()  # ¡La parte clave! Limpia la BD.
            conn.close()

# --- Pruebas Unitarias ---

def test_insertar_producto(db_cursor):
    """
    Prueba que podemos insertar un producto y 
    leer sus especificaciones JSONB.
    """
    # 1. Preparación (Insertar)
    sql_insert = """
    INSERT INTO productos (nombre, especificaciones) 
    VALUES (%s, %s)
    RETURNING id;
    """
    # psycopg2 puede convertir dicts de Python a JSONB automáticamente
    specs_dict = {"color": "rojo", "tamano": "M", "categoria": "test"}
    
    db_cursor.execute(sql_insert, ('producto_prueba', json.dumps(specs_dict)))
    
    new_id = db_cursor.fetchone()[0]
    assert new_id is not None

    # 2. Ejecución (Consultar)
    db_cursor.execute("SELECT nombre, especificaciones FROM productos WHERE id = %s", (new_id,))
    producto = db_cursor.fetchone()

    # 3. Validación (Assert)
    assert producto is not None
    assert producto[0] == 'producto_prueba'
    assert producto[1]['color'] == 'rojo'
    assert producto[1]['tamano'] == 'M'

def test_consultar_por_color(db_cursor):
    """
    Prueba la consulta por color (operador ->>)
    """
    # 1. Preparación
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('mouse_negro', '{"color":"negro", "categoria":"periferico"}')
    )
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('teclado_blanco', '{"color":"blanco", "categoria":"periferico"}')
    )

    # 2. Ejecución
    query = "SELECT nombre FROM productos WHERE especificaciones ->> 'color' = 'negro'"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 1
    assert resultados[0][0] == 'mouse_negro'

def test_consultar_por_tamano_con_operador_gin(db_cursor):
    """
    Prueba la consulta por tamaño usando el operador @> (contiene),
    que es el que utiliza el índice GIN.
    """
    # 1. Preparación
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('teclado_grande', '{"tamano":"grande", "color":"gris"}')
    )
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('mouse_chico', '{"tamano":"chico", "color":"negro"}')
    )

    # 2. Ejecución
    # Buscamos productos que contengan el JSON '{"tamano": "grande"}'
    query_json = '{"tamano": "grande"}' 
    
    db_cursor.execute("SELECT nombre FROM productos WHERE especificaciones @> %s", (query_json,))
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 1
    assert resultados[0][0] == 'teclado_grande'

def test_consultar_por_categoria_multiple(db_cursor):
    """
    Prueba una consulta que debe devolver múltiples resultados.
    """
    # 1. Preparación
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('GPU', '{"categoria":"componente"}')
    )
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('Procesador', '{"categoria":"componente"}')
    )
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('Monitor', '{"categoria":"monitor"}')
    )

    # 2. Ejecución
    query = "SELECT nombre FROM productos WHERE especificaciones ->> 'categoria' = 'componente'"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 2
    # Convertimos a 'set' para validar los nombres sin importar el orden
    nombres_encontrados = {row[0] for row in resultados}
    assert nombres_encontrados == {'GPU', 'Procesador'}

def test_consultar_valor_nulo_json(db_cursor):
    """
    Prueba cómo se consultan los valores 'null' dentro del JSONB.
    El operador ->> convierte 'null' (JSON) a NULL (SQL).
    """
    # 1. Preparación
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('procesador_sin_color', '{"categoria":"componente", "color": null}')
    )
    db_cursor.execute(
        "INSERT INTO productos (nombre, especificaciones) VALUES (%s, %s)",
        ('mouse_con_color', '{"categoria":"periferico", "color": "rojo"}')
    )
    
    # 2. Ejecución
    query = "SELECT nombre FROM productos WHERE (especificaciones ->> 'color') IS NULL"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación
    assert len(resultados) == 1
    assert resultados[0][0] == 'procesador_sin_color'

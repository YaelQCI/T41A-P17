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

def test_insertar_y_consultar_por_clave_rojo(db_cursor):
    """
    Prueba Req 2 (Insertar) y Req 3 (Consultar por color='rojo')
    """
    # 1. Preparación (Insertar registros)
    productos_data = [
        ('Sony WH-1000XM4', {"marca":"Sony", "peso":"2.5", "color":"negro", "tipo":"Audífonos"}),
        ('Oster Licuadora', {"marca":"Oster", "peso":"3.1", "color":"rojo", "tipo":"Licuadora"}),
        ('Logitech G Pro', {"marca":"Logitech", "peso":"0.15", "color":"blanco", "tipo":"Mouse"}),
        ('Nike Air Max', {"marca":"Nike", "peso":"0.8", "color":"rojo", "tipo":"Zapatillas"}),
        ('IKEA Micke', {"marca":"IKEA", "peso":"15.0", "color":"rojo", "tipo":"Escritorio"})
    ]
    
    # Usa la nueva tabla 'productos2'
    insert_query = "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s)"
    db_cursor.executemany(insert_query, productos_data)

    # 2. Ejecución (Consultar por color 'rojo')
    # Usa la nueva tabla 'productos2'
    query = "SELECT nombre FROM productos2 WHERE atributos -> 'color' = 'rojo'"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación (Assert)
    assert len(resultados) == 3
    nombres_encontrados = {row[0] for row in resultados}
    assert nombres_encontrados == {'Oster Licuadora', 'Nike Air Max', 'IKEA Micke'}

def test_actualizar_valor_en_hstore(db_cursor):
    """
    Prueba Req 4 (Actualizar 'peso' del escritorio IKEA)
    """
    # 1. Preparación (Insertar el producto a modificar)
    ikea_attrs = {"marca":"IKEA", "peso":"15.0", "color":"rojo", "tipo":"Escritorio"}
    db_cursor.execute(
        # Usa la nueva tabla 'productos2'
        "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s) RETURNING id",
        ('IKEA Micke', ikea_attrs)
    )
    producto_id = db_cursor.fetchone()[0]

    # 2. Ejecución (Actualizar 'peso' a '17.5' como en tu SQL)
    update_hstore_string = '"peso" => "17.5"'
    
    db_cursor.execute(
        # Usa la nueva tabla 'productos2'
        "UPDATE productos2 SET atributos = atributos || %s WHERE id = %s",
        (update_hstore_string, producto_id)
    )

    # 3. Validación (Assert)
    # Usa la nueva tabla 'productos2'
    db_cursor.execute("SELECT atributos FROM productos2 WHERE id = %s", (producto_id,))
    atributos_actualizados = db_cursor.fetchone()[0]
    
    assert atributos_actualizados['peso'] == '17.5'
    assert atributos_actualizados['color'] == 'rojo' 

def test_eliminar_clave_de_hstore(db_cursor):
    """
    Prueba Req 5 (Eliminar 'color' de la licuadora Oster)
    """
    # 1. Preparación (Insertar el producto a modificar)
    oster_attrs = {"marca":"Oster", "peso":"3.1", "color":"rojo", "tipo":"Licuadora"}
    db_cursor.execute(
        # Usa la nueva tabla 'productos2'
        "INSERT INTO productos2 (nombre, atributos) VALUES (%s, %s) RETURNING id",
        ('Oster Licuadora', oster_attrs)
    )
    producto_id = db_cursor.fetchone()[0]

    # 2. Ejecución (Eliminar el atributo 'color' como en tu SQL)
    db_cursor.execute(
        # Usa la nueva tabla 'productos2'
        "UPDATE productos2 SET atributos = delete(atributos, 'color') WHERE id = %s",
        (producto_id,)
    )

    # 3. Validación (Assert)
    # Usa la nueva tabla 'productos2'
    db_cursor.execute("SELECT atributos FROM productos2 WHERE id = %s", (producto_id,))
    atributos_actualizados = db_cursor.fetchone()[0]

    assert 'color' not in atributos_actualizados
    assert 'marca' in atributos_actualizados
    assert atributos_actualizados['tipo'] == 'Licuadora'
    assert atributos_actualizados['peso'] == '3.1'

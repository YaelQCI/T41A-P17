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

    - Se conecta ANTES de cada prueba.
    - ¡Registra el adaptador HSTORE!
    - Entrega (yield) el cursor.
    - Hace un ROLLBACK DESPUÉS de cada prueba para limpiar la BD.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # --- ¡IMPORTANTE PARA HSTORE! ---
        # Registra el adaptador para poder leer y escribir
        # HSTORE como si fueran diccionarios de Python.
        register_hstore(conn)
        
        cur = conn.cursor()
        yield cur
        
    finally:
        if conn:
            conn.rollback()  # ¡La parte clave! Limpia la BD.
            conn.close()

# --- Pruebas Unitarias ---

def test_insertar_y_consultar_por_clave_rojo(db_cursor):
    """
    Prueba la inserción de múltiples productos (Req 2) y 
    la consulta por una clave específica (Req 3).
    """
    # 1. Preparación (Insertar registros)
    # Gracias a register_hstore, podemos pasar un dict de Python
    productos_data = [
        ('Sony', 2.5, {"color": "Negro", "tipo": "Audífonos", "bluetooth": "true"}),
        ('Oster', 3.1, {"color": "Rojo", "tipo": "Licuadora", "velocidades": "5"}),
        ('Logitech', 0.15, {"color": "Blanco", "tipo": "Mouse", "dpi": "16000"}),
        ('Nike', 0.8, {"color": "Rojo", "tipo": "Zapatillas", "talla": "US 10"}),
        ('IKEA', 15.0, {"color": "Rojo", "tipo": "Escritorio", "dimensiones": "120x60cm"})
    ]
    insert_query = "INSERT INTO productos (nombre, peso, atributos_adicionales) VALUES (%s, %s, %s)"
    db_cursor.executemany(insert_query, productos_data)

    # 2. Ejecución (Consultar por color 'Rojo')
    query = "SELECT nombre FROM productos WHERE atributos_adicionales -> 'color' = 'Rojo'"
    db_cursor.execute(query)
    resultados = db_cursor.fetchall()

    # 3. Validación (Assert)
    assert len(resultados) == 3
    # Usamos un 'set' para validar los nombres sin importar el orden
    nombres_encontrados = {row[0] for row in resultados}
    assert nombres_encontrados == {'Oster', 'Nike', 'IKEA'}

def test_actualizar_valor_en_hstore(db_cursor):
    """
    Prueba la actualización de un valor dentro del HSTORE (Req 4).
    Usa la sintaxis: hstore_col = hstore_col || 'clave => valor'
    """
    # 1. Preparación (Insertar el producto a modificar)
    ikea_attrs = {"color": "Rojo", "tipo": "Escritorio", "peso": "68kg"}
    db_cursor.execute(
        "INSERT INTO productos (nombre, atributos_adicionales) VALUES (%s, %s) RETURNING id",
        ('IKEA', ikea_attrs)
    )
    producto_id = db_cursor.fetchone()[0]

    # 2. Ejecución (Actualizar el 'peso' usando la sintaxis de string HSTORE)
    # Esta sintaxis es la que usaste en tu ejemplo SQL
    update_hstore_string = 'peso => 79kg'
    
    db_cursor.execute(
        "UPDATE productos SET atributos_adicionales = atributos_adicionales || %s WHERE id = %s",
        (update_hstore_string, producto_id)
    )

    # 3. Validación (Assert)
    db_cursor.execute("SELECT atributos_adicionales FROM productos WHERE id = %s", (producto_id,))
    # El resultado se recibe como un dict de Python gracias a register_hstore
    atributos_actualizados = db_cursor.fetchone()[0]
    
    assert atributos_actualizados['peso'] == '79kg'
    assert atributos_actualizados['color'] == 'Rojo' # Verificar que otras claves no se borraron

def test_eliminar_clave_de_hstore(db_cursor):
    """
    Prueba la eliminación de una clave de un registro (Req 5).
    Usa la función: delete(hstore_col, 'clave')
    """
    # 1. Preparación (Insertar el producto a modificar)
    logitech_attrs = {"color": "Blanco", "tipo": "Mouse", "dpi": "16000"}
    db_cursor.execute(
        "INSERT INTO productos (nombre, atributos_adicionales) VALUES (%s, %s) RETURNING id",
        ('Logitech', logitech_attrs)
    )
    producto_id = db_cursor.fetchone()[0]

    # 2. Ejecución (Eliminar el atributo 'color', como pedía el requisito)
    db_cursor.execute(
        "UPDATE productos SET atributos_adicionales = delete(atributos_adicionales, 'color') WHERE id = %s",
        (producto_id,)
    )

    # 3. Validación (Assert)
    db_cursor.execute("SELECT atributos_adicionales FROM productos WHERE id = %s", (producto_id,))
    atributos_actualizados = db_cursor.fetchone()[0]

    # Verificar que 'color' ya no está en el diccionario
    assert 'color' not in atributos_actualizados
    # Verificar que las otras claves siguen presentes
    assert 'tipo' in atributos_actualizados
    assert atributos_actualizados['dpi'] == '16000'

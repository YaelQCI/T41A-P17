-- Crea el índice GIN en la columna 'atributos'
CREATE INDEX idx_gin_productos2_atributos ON productos2 USING GIN (atributos);

-- NOTA: Después de crearlo, es buena idea correr ANALYZE
-- para que el planificador de consultas lo use inmediatamente.
ANALYZE productos2;

SELECT 
    atributos -> 'marca' AS marca, 
    COUNT(*) AS total_productos
FROM productos2
GROUP BY marca
ORDER BY total_productos DESC;

SELECT 
    nombre, 
    atributos, -- HSTORE original
    hstore_to_jsonb(atributos) AS atributos_en_jsonb
FROM productos2
WHERE nombre = 'Logitech G Pro';

CREATE OR REPLACE FUNCTION jsonb_to_hstore(json_input jsonb)
RETURNS hstore LANGUAGE sql IMMUTABLE AS $$
    SELECT hstore(
        array_agg(key),
        array_agg(value)
    )
    FROM jsonb_each_text(json_input)
$$;

-- Ejemplo hipotético: Convertir un objeto JSONB literal a HSTORE
SELECT jsonb_to_hstore(
    '{"clave_nueva": "valor_nuevo", "prioridad": "alta"}'::jsonb
);

SELECT nombre, atributos
FROM productos2
WHERE atributos ?& ARRAY['color', 'peso'];

CREATE OR REPLACE FUNCTION resumir_producto(
    nombre_producto TEXT, 
    atributos_hstore HSTORE
)
RETURNS TEXT AS $$
DECLARE
    marca TEXT;
    color TEXT;
BEGIN
    -- Extraer los valores que nos interesan del HSTORE
    marca := atributos_hstore -> 'marca';
    color := atributos_hstore -> 'color';

    -- Si alguna clave no existe, será NULL. 
    -- Usamos COALESCE para darle un valor 'N/A' en ese caso.
    marca := COALESCE(marca, 'N/A');
    color := COALESCE(color, 'N/A');

    -- Usar format() para construir el string de resumen
    RETURN format('Producto %s: marca=%s, color=%s', 
                  nombre_producto, 
                  marca, 
                  color
           );
END;
$$ LANGUAGE plpgsql;

SELECT 
    nombre, 
    resumir_producto(nombre, atributos) AS resumen
FROM productos2;

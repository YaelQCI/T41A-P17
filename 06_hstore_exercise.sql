CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE productos2 (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    precio NUMERIC(10, 2), -- Columna ADICIONAL para el Ejercicio 2
    atributos HSTORE
);

-- Insertar los datos base
INSERT INTO productos2 (nombre, precio, atributos)
VALUES 
('Sony WH-1000XM4', 550.00, '"marca"=>"Sony", "peso"=>"2.5", "color"=>"negro", "tipo"=>"Audífonos", "bluetooth"=>"true"'),
('Oster Licuadora', 89.50, '"marca"=>"Oster", "peso"=>"3.1", "color"=>"rojo", "tipo"=>"Licuadora", "velocidades"=>"5"'),
('Logitech G Pro', 129.99, '"marca"=>"Logitech", "peso"=>"0.15", "color"=>"blanco", "tipo"=>"Mouse", "dpi"=>"16000"'),
('Nike Air Max', 150.00, '"marca"=>"Nike", "peso"=>"0.8", "color"=>"rojo", "tipo"=>"Zapatillas", "talla"=>"US 10"'),
('IKEA Micke', 79.00, '"marca"=>"IKEA", "peso"=>"15.0", "color"=>"rojo", "tipo"=>"Escritorio", "dimensiones"=>"120x60cm"');

-- 3. Consultar por una clave específica (color = 'rojo')
SELECT * FROM productos2 WHERE atributos -> 'color' = 'rojo';

-- 4. Actualizar un valor (Cambiar 'peso' del escritorio IKEA a '17.5')
UPDATE productos2
SET atributos = atributos || '"peso" => "17.5"'
WHERE nombre = 'IKEA Micke';

SELECT nombre, atributos FROM productos2 WHERE nombre = 'IKEA Micke';

-- 5. Eliminar una clave (Eliminar 'color' de la licuadora Oster)
UPDATE productos2
SET atributos = delete(atributos, 'color')
WHERE nombre = 'Oster Licuadora';

-- Verificamos la eliminación
SELECT nombre, atributos FROM productos2 WHERE nombre = 'Oster Licuadora';

-- Consulta final para ver el estado de todos los datos
SELECT * FROM productos2;

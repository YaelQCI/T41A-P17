DROP TABLE productos;

CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE productos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    peso NUMERIC(10, 2),
    atributos_adicionales HSTORE
);

INSERT INTO productos (nombre, peso, atributos_adicionales)
VALUES 
('Sony',2.5,'"color"=>"Negro", "tipo" => "Audífonos", "bluetooth" => "true", "cancelacion_ruido" => "activa"'),
('Oster',3.1,'"color"=>"Rojo", "tipo" => "Licuadora", "velocidades" => "5", "material_vaso" => "Vidrio"'),
('Logitech', 0.15,'"color"=>"Blanco", "tipo" => "Mouse", "dpi" => "16000", "inalambrico" => "true"'),
('Nike', 0.8, '"color"=>"Rojo", "tipo" => "Zapatillas", "talla" => "US 10", "material_suela" => "Goma"'),
('IKEA', 15.0, '"color"=>"Rojo", "tipo" => "Escritorio", "dimensiones" => "120x60cm", "peso" => "68kg"');

SELECT * FROM productos WHERE atributos_adicionales -> 'color' = 'Rojo';

UPDATE productos
SET atributos_adicionales = atributos_adicionales || 'peso => 79kg'
WHERE nombre = 'IKEA';
SELECT * FROM productos WHERE atributos_adicionales -> 'color' = 'Rojo';

UPDATE productos
SET atributos_adicionales = delete(atributos_adicionales, 'material_vaso')
WHERE nombre = 'Oster';

SELECT * FROM productos WHERE color='Rojo';

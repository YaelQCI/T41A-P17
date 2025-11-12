-- Ejercicio 1: Filtrar registros que contienen una clave (operador ?)
SELECT nombre, atributos FROM productos2 
WHERE atributos ? 'marca';

-- Ejercicio 2: Combinar HSTORE con otras columnas
SELECT nombre, precio, atributos FROM productos2 
WHERE 
    (atributos -> 'marca' = 'Sony') 
    AND 
    (precio > 500);

-- Ejercicio 3: Extraer todas las claves y valores
SELECT 
    nombre, 
    skeys(atributos) AS todas_las_claves, 
    svals(atributos) AS todos_los_valores 
FROM productos2;

-- Ejercicio 4: Contar cuántos productos tienen un atributo específico
SELECT 
    count(*) AS total_con_color
FROM productos2 
WHERE atributos ? 'color';


# NoSQL en PostgreSQL

Este documento explora cómo PostgreSQL, una base de datos relacional tradicional, ha incorporado capacidades NoSQL para manejar datos semiestructurados y no estructurados, ofreciendo flexibilidad sin sacrificar la robustez de las transacciones ACID.

---

## 🧩 ¿Qué es NoSQL en PostgreSQL?

Aunque PostgreSQL es una base de datos relacional, ofrece soporte para:

- **JSON / JSONB**: documentos semiestructurados.
- **HSTORE**: pares clave-valor.
- **Arrays**: listas de valores.
- **CTE recursivos**: para modelar grafos.

---

## 🔧 Ejemplo práctico

### 1. Crear tabla con JSONB
```sql
CREATE TABLE usuarios (
  id SERIAL PRIMARY KEY,
  data JSONB
);
```

### 2. Insertar datos
```sql
INSERT INTO usuarios (data)
VALUES 
  ('{"nombre": "Ana", "activo": true, "edad": 30}'),
  ('{"nombre": "Juan", "activo": false, "edad": 25}');
```

### 3. Consultar datos
```sql
SELECT data->>'nombre' AS nombre
FROM usuarios
WHERE data->>'activo' = 'true';
```

### 4. Índices GIN para JSONB
```sql
CREATE INDEX idx_data_gin ON usuarios USING GIN (data);
```
## 🧠 ¿Para qué sirve en JSONB?
Cuando tienes una columna JSONB con muchos datos semiestructurados, las consultas pueden volverse lentas si no hay un índice. El índice GIN permite:

Buscar claves y valores dentro del JSONB.
Usar operadores como @>, ?, ?&, ?| de forma eficiente.

Este índice permite acelerar consultas como:

```sql
SELECT * FROM usuarios
WHERE data @> '{"activo": true}';
```

---

## 🧪 Pruebas unitarias (usando pgTAP)
```sql
SELECT plan(2);

-- Verifica que el nombre sea Ana
SELECT is(
  (SELECT data->>'nombre' FROM usuarios WHERE id = 1),
  'Ana',
  'Nombre correcto'
);

-- Verifica que el usuario esté activo
SELECT is(
  (SELECT data->>'activo' FROM usuarios WHERE id = 1),
  'true',
  'Usuario activo'
);
```

---

## 🧠 Ejercicios recomendados

1. Crear una tabla de productos con especificaciones en JSONB.
2. Insertar al menos 5 productos con diferentes atributos.
3. Consultar productos por color, tamaño o categoría.
4. Crear índices GIN y medir el rendimiento.
5. Implementar pruebas unitarias con pgTAP.

---

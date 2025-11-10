DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM usuarios WHERE id = 1 AND data->>'nombre' = 'Ana'
  ) THEN
    RAISE EXCEPTION 'Fallo: nombre incorrecto para id 1';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM usuarios WHERE id = 1 AND data->>'activo' = 'true'
  ) THEN
    RAISE EXCEPTION 'Fallo: usuario no está activo para id 1';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM usuarios WHERE id = 2 AND data->>'edad' = '25'
  ) THEN
    RAISE EXCEPTION 'Fallo: edad incorrecta para id 2';
  END IF;
$$

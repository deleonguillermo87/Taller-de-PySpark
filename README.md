# Taller de Data Engineer con PySpark

ETL local de datos de clientes, productos, pedidos y líneas de pedido. El proceso conserva la trazabilidad de las filas rechazadas y escribe los resultados analíticos en Parquet.

## 1. Cómo ejecutar

Requisitos:

- Python 3.12 y el entorno virtual `.venv`.
- Java 21.
- PySpark 4.3.0.

Desde la raíz del proyecto:

```bash
.venv/bin/python -m src.etl
```

El comando inicia Spark en `local[*]`, procesa el histórico junto al lote incremental, sobrescribe las salidas Parquet, las vuelve a leer para comprobarlas y ejecuta el proceso dos veces para verificar idempotencia. La segunda ejecución se usa para la comparación y omite los reportes detallados.

## 2. Arquitectura del ETL

El flujo está implementado en `src/etl.py`:

1. **Lectura (`read_raw`)**: carga `customers.csv`, `products.csv`, `orders.csv` y `order_items.csv` como texto, conservando el esquema crudo.
2. **Perfil (`profile`)**: función de inspección que muestra esquemas de calidad, duplicados, nulos, categorías, huérfanos, rangos numéricos y timestamps inválidos. Es una etapa disponible para explorar datos crudos.
3. **Limpieza (`clean`)**: convierte tipos, corrige el typo de estado, resuelve duplicados y referencias. Las filas rechazadas se conservan en una cuarentena con tabla, clave, motivo y JSON de la fila original.
4. **Detalle (`build_sales_detail`)**: une líneas válidas con pedidos, clientes y productos, calcula `net_amount` y verifica la cardinalidad de los joins y los nulos.
5. **Agregación mensual**: agrupa las ventas no canceladas por mes, país, canal y categoría; compara los resultados de la API DataFrame y Spark SQL.
6. **Incremental (`apply_incremental`)**: une los CSV históricos crudos con los JSONL incrementales y orquesta limpieza, detalle, agregación, controles y persistencia.
7. **Escritura y controles**: sobrescribe Parquet, los relee y verifica conteos. `main` ejecuta el reprocesamiento incremental dos veces y compara las métricas para demostrar idempotencia.

El `profile` está disponible para inspección del crudo; la ejecución incremental de `main` no invoca ese reporte, y en su lugar muestra el resumen de limpieza y sus controles.

## 3. Anomalías encontradas

Conteos de la última ejecución, sobre el histórico combinado con el lote incremental:

| Anomalía | Conteo | Tratamiento |
|---|---:|---|
| Claves duplicadas de `customers` (`customer_id`) | 1 fila repetida | **Cuarentena** de la fila descartada; se conserva una fila según `signup_date` más reciente y desempate determinista. |
| Claves duplicadas de `orders` (`order_id`) | 2 filas descartadas: 1 versión reemplazada y 1 duplicado exacto | **Cuarentena** con motivo específico; se conserva la versión con `updated_at` más reciente. |
| `customers.email` nulo | 1 | **Conservar con advertencia**; no se rechaza al cliente. |
| `orders.status = 'deliverd'` | 1 | **Corregir** a `delivered`; se informa el contador de correcciones. |
| `orders.order_ts = '2026-99-31T12:00:00Z'` | 1 | El `try_cast` produce nulo y la fila se envía a **cuarentena** con motivo `order_ts inválido`. |
| Pedidos sin customer | 2 filas: 1 con `customer_id` nulo y 1 huérfana | **Cuarentena** con el motivo correspondiente. |
| Líneas con `order_id` huérfano | 9 | **Cuarentena**. Incluye líneas de pedidos inválidos o no disponibles después de la limpieza. |
| Líneas con `product_id` huérfano | 1 | **Cuarentena**. |
| `quantity <= 0` | 2 líneas | **Cuarentena**. |
| `discount_pct > 1` | 1 línea | **Cuarentena** por estar fuera de `[0, 1]`. |
| `P088`, `unit_price` negativo | 1 producto; arrastra 427 líneas | **Cuarentena** del producto; sus líneas válidas de otro modo se envían a cuarentena con motivo `producto inválido`. |
| `P089`, `unit_price` nulo | 1 producto; arrastra 400 líneas | **Cuarentena** del producto y de sus líneas con motivo `producto inválido`. |
| Productos con `active=false` | 11 | **Conservar**; son utilizables para ventas históricas. |

Los dos productos inválidos arrastran **827 líneas** en conjunto. La cuarentena total es **848 filas** en todas las tablas: 1 customer, 2 productos, 5 pedidos y 840 líneas. Cada fila retirada tiene una única entrada de cuarentena y un motivo.

## 4. Decisiones de negocio y tipos

- Los pedidos con `status = 'cancelled'` no se consideran ventas reales y se excluyen de `sales_monthly`.
- `net_amount` se almacena como `DecimalType(18,2)`: cantidad por precio unitario por (1 menos descuento), redondeado a dos decimales.
- Los importes y sus agregaciones se calculan como decimales, no como flotantes binarios.
- No se inventan ni imputan precios. Un producto sin precio válido y sus líneas afectadas se conservan en cuarentena para revisión.
- `active=false` no invalida por sí solo un producto ni una venta histórica.

En la última ejecución, el detalle contiene 39.386 líneas y suma **95.703.942,43**, incluyendo pedidos cancelados. El agregado mensual excluye cancelados y suma **72.087.211,49** en 207 grupos.

## 5. Incremental idempotente

`apply_incremental(spark)` realiza un reprocesamiento completo y determinista:

- Lee `orders_incremental.jsonl` y `order_items_incremental.jsonl`; convierte todas las columnas del lote incremental a string para igualar el esquema crudo de CSV.
- Une histórico e incremental crudos con `unionByName`, asignando `source_rank=0` al histórico y `source_rank=1` al incremental.
- Ejecuta la misma `clean()`. Los pedidos duplicados se resuelven por `updated_at` descendente. Las líneas duplicadas se resuelven por `line_id`, dando prioridad al mayor `source_rank`.
- Una actualización de pedido no elimina sus líneas históricas. En particular, el incremental actualiza `O00005`, pero no sustituye ni borra la línea histórica `L0000009`.
- Reconstruye detalle y agregado desde cero y sobrescribe las carpetas de salida; reprocesar el mismo lote no acumula datos.

Las aserciones verifican que `O00005` queda una sola vez con `updated_at = 2026-03-05T12:00:00Z`, que `L0000009` permanece y que no hay `order_id` duplicados en pedidos limpios.

La evidencia de dos ejecuciones consecutivas es idéntica:

| Métrica | Resultado en cada ejecución |
|---|---:|
| Pedidos limpios | 20.097 |
| Líneas limpias | 39.386 |
| Suma de `net_amount` del detalle | 95.703.942,43 |
| Grupos en `sales_monthly` | 207 |
| Filas totales en `quarantine` | 848 |

## 6. Controles de calidad

1. **Reconciliación monetaria:** compara la suma decimal de `net_sales` con la suma de `net_amount` del detalle filtrado para excluir `cancelled`. La diferencia debe ser exactamente `0.00`. En la última ejecución, ambos importes son **72.087.211,49**. El total bruto de detalle, que sí incluye cancelados, es **95.703.942,43**.
2. **Integridad de filas y claves:** verifica que no haya `order_id` duplicados en pedidos finales y que, por tabla, `entrada = limpias + cuarentena`. Para el histórico combinado se comprobó:

   | Tabla | Entrada | Limpias | Cuarentena |
   |---|---:|---:|---:|
   | `customers` | 1.001 | 1.000 | 1 |
   | `products` | 100 | 98 | 2 |
   | `orders` | 20.102 | 20.097 | 5 |
   | `order_items` | 40.226 | 39.386 | 840 |

Además, el detalle comprueba que los joins `inner` no multiplican ni eliminan líneas limpias y que claves, `net_amount` y `year_month` no sean nulos. La agregación mensual compara los resultados de la API DataFrame y SQL.

## 7. Preguntas frecuentes

**¿Cuál es la granularidad de cada archivo?**  
`customers.csv`: una fila por `customer_id`; `products.csv`: una fila por `product_id`; `orders.csv`: una fila por versión de `order_id`; `order_items.csv`: una fila por línea (`line_id`). Los JSONL incrementales contienen nuevas filas o versiones de pedidos y nuevas líneas.

**¿Cuándo corregir, rechazar o poner en cuarentena?**  
Se corrige una anomalía conocida y no ambigua, como `deliverd` → `delivered`. Se conservan con advertencia datos válidos para el uso, como un email nulo o un producto inactivo. Se pone en cuarentena una fila que no puede usarse de manera fiable, como una fecha inválida, una clave duplicada descartada o una relación huérfana. La fila original y el motivo quedan registrados.

**¿Qué diferencia hay entre transformaciones y acciones en Spark?**  
Las transformaciones (por ejemplo, `select`, `filter`, `join` o `groupBy`) describen un plan perezoso; se ejecutan cuando una acción como `count`, `collect`, `show` o una escritura Parquet solicita el resultado.

**¿Cómo evitar que un join multiplique totales?**  
Verificar que las tablas del lado derecho tengan una sola fila por clave antes de unirlas, deduplicar según una regla explícita y comparar el conteo antes y después. El detalle ejecuta una comprobación de cardinalidad para que los joins no multipliquen ni pierdan líneas.

**¿Qué limitaciones tiene Spark local frente a un clúster?**  
`local[*]` usa los recursos de una sola máquina; no valida distribución entre nodos, tolerancia a fallos de ejecutores, escalado horizontal ni comportamiento ante particiones y volúmenes de producción. Es útil para el taller y pruebas funcionales, pero el rendimiento y la operación deben validarse en el entorno de clúster destino.

## Estructura de `output/`

El flujo actual sobrescribe `sales_detail/`, `sales_monthly/` y `quarantine/` en cada ejecución:

- `sales_detail/`: detalle enriquecido por línea, con `net_amount` decimal; incluye pedidos cancelados para mantener el detalle completo.
- `sales_monthly/`: **entregable analítico final**, agregado mensual por `year_month`, país, canal y categoría, particionado por `year_month`; excluye pedidos cancelados.
- `quarantine/`: filas del histórico y del lote incremental que fueron retiradas, con tabla, clave, motivo y JSON original.

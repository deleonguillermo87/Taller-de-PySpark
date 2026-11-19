from pathlib import Path
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from .spark_session import get_spark


DATA_DIR = Path(__file__).resolve().parent.parent / "datos"


def read_raw(spark: SparkSession, mostrar: bool = True) -> dict[str, DataFrame]:
    """Lee los cuatro CSV como texto y muestra su esquema y cantidad de filas."""
    archivos = {
        "customers": "customers.csv",
        "products": "products.csv",
        "orders": "orders.csv",
        "order_items": "order_items.csv",
    }
    dataframes = {}

    for nombre, archivo in archivos.items():
        dataframe = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_DIR / archivo))
        )
        if mostrar:
            print(f"\nEsquema de {nombre}:")
            dataframe.printSchema()
            print(f"Cantidad de filas de {nombre}: {dataframe.count()}")
        dataframes[nombre] = dataframe

    return dataframes


def profile(dfs: dict[str, DataFrame]) -> None:
    """Imprime métricas de calidad y un resumen de anomalías de los datos."""
    claves = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "line_id",
    }
    columnas_categoricas = {
        "status": "orders",
        "channel": "orders",
        "country": "customers",
        "category": "products",
        "active": "products",
    }
    anomalías = []

    for nombre, dataframe in dfs.items():
        total = dataframe.count()
        clave = claves[nombre]
        duplicadas = (
            dataframe.groupBy(clave)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"\nPerfil de {nombre}:")
        print(f"  Filas: {total}")
        print(f"  Claves duplicadas (valores repetidos): {duplicadas}")
        if duplicadas:
            anomalías.append(f"{nombre}: {duplicadas} claves duplicadas")

        nulos = dataframe.agg(
            *[
                F.sum(F.when(F.col(columna).isNull(), 1).otherwise(0)).alias(columna)
                for columna in dataframe.columns
            ]
        ).first()
        print("  Nulos por columna:")
        for columna in dataframe.columns:
            cantidad = nulos[columna] or 0
            print(f"    {columna}: {cantidad}")
            if cantidad:
                anomalías.append(f"{nombre}.{columna}: {cantidad} valores nulos")

    print("\nValores distintos:")
    for columna, nombre_df in columnas_categoricas.items():
        valores = [
            fila[columna]
            for fila in (
                dfs[nombre_df]
                .select(columna)
                .distinct()
                .orderBy(columna)
                .collect()
            )
        ]
        print(f"  {columna} ({nombre_df}): {valores}")
        if columna == "status" and "deliverd" in valores:
            cantidad = dfs[nombre_df].filter(F.col(columna) == "deliverd").count()
            anomalías.append(f"orders.status: {cantidad} filas con el typo 'deliverd'")

    patron_timestamp = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
    print("\nTimestamps inválidos:")
    for columna in ("order_ts", "updated_at"):
        condicion_invalida = (
            F.col(columna).isNull()
            | ~F.col(columna).rlike(patron_timestamp)
            | F.expr(f"try_cast(`{columna}` AS TIMESTAMP)").isNull()
        )
        cantidad = dfs["orders"].filter(condicion_invalida).count()
        print(f"  {columna}: {cantidad}")
        if cantidad:
            anomalías.append(
                f"orders.{columna}: {cantidad} valores no válidos "
                "para yyyy-MM-ddTHH:mm:ssZ"
            )

    huérfanos = {
        "orders sin customer": dfs["orders"].join(
            dfs["customers"].select("customer_id"),
            on="customer_id",
            how="left_anti",
        ),
        "order_items sin order": dfs["order_items"].join(
            dfs["orders"].select("order_id"),
            on="order_id",
            how="left_anti",
        ),
        "order_items sin product": dfs["order_items"].join(
            dfs["products"].select("product_id"),
            on="product_id",
            how="left_anti",
        ),
    }
    print("\nRegistros huérfanos (left_anti):")
    for relacion, dataframe in huérfanos.items():
        cantidad = dataframe.count()
        print(f"  {relacion}: {cantidad}")
        if cantidad:
            anomalías.append(f"{relacion}: {cantidad} registros")

    columnas_numericas = {
        "quantity": ("order_items", "INT"),
        "discount_pct": ("order_items", "DOUBLE"),
        "unit_price": ("products", "DOUBLE"),
    }
    valores_casteados = {}
    print("\nMínimo y máximo tras castear:")
    for columna, (nombre_df, tipo) in columnas_numericas.items():
        dataframe = dfs[nombre_df].withColumn(
            "_valor_casteado", F.expr(f"try_cast(`{columna}` AS {tipo})")
        )
        minimo, maximo = dataframe.agg(
            F.min("_valor_casteado").alias("min"),
            F.max("_valor_casteado").alias("max"),
        ).first()
        valores_casteados[columna] = dataframe
        print(f"  {columna}: min={minimo}, max={maximo}")

    reglas_numericas = (
        (
            "order_items.quantity",
            valores_casteados["quantity"],
            F.col("_valor_casteado") <= 0,
            "valores menores o iguales a cero",
        ),
        (
            "order_items.discount_pct",
            valores_casteados["discount_pct"],
            (F.col("_valor_casteado") < 0) | (F.col("_valor_casteado") > 1),
            "descuentos fuera de [0, 1]",
        ),
        (
            "products.unit_price",
            valores_casteados["unit_price"],
            F.col("_valor_casteado").isNull() | (F.col("_valor_casteado") < 0),
            "precios nulos, no numéricos o negativos",
        ),
    )
    for etiqueta, dataframe, condicion, descripcion in reglas_numericas:
        cantidad = dataframe.filter(condicion).count()
        if cantidad:
            anomalías.append(f"{etiqueta}: {cantidad} {descripcion}")

    print("\nResumen de anomalías encontradas:")
    if anomalías:
        for anomalía in anomalías:
            print(f"  - {anomalía}")
    else:
        print("  No se detectaron anomalías en las reglas evaluadas.")


def clean(
    dfs: dict[str, DataFrame],
    mostrar: bool = True,
) -> tuple[dict[str, DataFrame], DataFrame]:
    """Convierte tipos, limpia registros y devuelve las filas rechazadas."""
    claves = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "line_id",
    }
    columnas_tipo = {
        "customers": {"signup_date": "date"},
        "products": {"unit_price": "double", "active": "boolean"},
        "orders": {"order_ts": "timestamp", "updated_at": "timestamp"},
        "order_items": {"quantity": "int", "discount_pct": "double"},
    }
    entradas = {tabla: dataframe.count() for tabla, dataframe in dfs.items()}
    tipados = {}

    for tabla, dataframe in dfs.items():
        original = dataframe.withColumn(
            "_fila_original",
            F.to_json(F.struct(*[F.col(columna) for columna in dataframe.columns])),
        )
        for columna, tipo in columnas_tipo[tabla].items():
            if tipo == "timestamp":
                original = original.withColumn(
                    columna, F.expr(f"try_cast(`{columna}` AS TIMESTAMP)")
                )
            else:
                original = original.withColumn(columna, F.col(columna).cast(tipo))
        tipados[tabla] = original

    def filas_cuarentena(
        dataframe: DataFrame, tabla: str, motivo: str
    ) -> DataFrame:
        return dataframe.select(
            F.lit(tabla).alias("tabla"),
            F.col(claves[tabla]).cast("string").alias("clave"),
            F.lit(motivo).alias("motivo"),
            F.col("_fila_original").alias("fila_original"),
        )

    def quitar_duplicados(
        tabla: str, fecha_orden: str | None
    ) -> tuple[DataFrame, DataFrame]:
        dataframe = tipados[tabla]
        clave = claves[tabla]
        orden = []
        if fecha_orden is not None:
            orden.append(F.col(fecha_orden).desc_nulls_last())
        if "source_rank" in dataframe.columns:
            orden.append(F.col("source_rank").desc())
        orden.append(F.col("_fila_original").asc_nulls_first())
        ventana = Window.partitionBy(clave).orderBy(*orden)
        numeradas = dataframe.withColumn("_numero_fila", F.row_number().over(ventana))
        if tabla == "orders":
            numeradas = numeradas.withColumn(
                "_updated_at_mas_reciente",
                F.max(fecha_orden).over(Window.partitionBy(clave)),
            )
        duplicadas = numeradas.filter(F.col("_numero_fila") > 1)
        conservadas = numeradas.filter(F.col("_numero_fila") == 1).drop("_numero_fila")
        if tabla == "orders":
            duplicadas = duplicadas.withColumn(
                "_motivo_duplicado",
                F.when(
                    F.col(fecha_orden) < F.col("_updated_at_mas_reciente"),
                    F.lit("versión reemplazada (updated_at más reciente)"),
                ).otherwise(F.lit("duplicado exacto")),
            )
            cuarentena_duplicados = duplicadas.select(
                F.lit(tabla).alias("tabla"),
                F.col(clave).cast("string").alias("clave"),
                F.col("_motivo_duplicado").alias("motivo"),
                F.col("_fila_original").alias("fila_original"),
            )
        else:
            cuarentena_duplicados = filas_cuarentena(
                duplicadas, tabla, "duplicado"
            )
        return conservadas, cuarentena_duplicados

    def agregar_condicion(
        dataframe: DataFrame, condicion: Column, tabla: str, motivo: str
    ) -> tuple[DataFrame, DataFrame]:
        rechazadas = dataframe.filter(condicion)
        restantes = dataframe.filter(~F.coalesce(condicion, F.lit(False)))
        return restantes, filas_cuarentena(rechazadas, tabla, motivo)

    customers, clientes_duplicados = quitar_duplicados("customers", "signup_date")
    products = tipados["products"]
    orders = tipados["orders"]
    orders, orders_order_ts_invalido = agregar_condicion(
        orders,
        F.col("order_ts").isNull(),
        "orders",
        "order_ts inválido",
    )
    orders, orders_updated_at_invalido = agregar_condicion(
        orders,
        F.col("updated_at").isNull(),
        "orders",
        "updated_at inválido",
    )
    tipados["orders"] = orders
    orders, pedidos_duplicados = quitar_duplicados("orders", "updated_at")
    items = tipados["order_items"]
    lineas_duplicadas = None
    if "source_rank" in items.columns:
        items, lineas_duplicadas = quitar_duplicados("order_items", None)

    # La corrección ocurre antes de retirar duplicados para contar todas las filas corregidas.
    correcciones = orders.filter(F.col("status") == "deliverd").count()
    orders = orders.withColumn(
        "status",
        F.when(F.col("status") == "deliverd", F.lit("delivered")).otherwise(
            F.col("status")
        ),
    )
    if mostrar:
        print(f"\nCorrecciones status 'deliverd' -> 'delivered': {correcciones}")
    email_nulo = customers.filter(F.col("email").isNull()).count()
    if mostrar:
        print(f"Advertencia: customers con email nulo conservados: {email_nulo}")

    # Los productos inválidos se separan antes de resolver las referencias de las líneas.
    condicion_producto_invalido = F.col("unit_price").isNull() | (
        F.col("unit_price") <= 0
    )
    productos_rechazados = products.filter(condicion_producto_invalido)
    products = products.filter(
        ~F.coalesce(condicion_producto_invalido, F.lit(False))
    )
    productos_invalidos = filas_cuarentena(
        productos_rechazados, "products", "unit_price nulo o no positivo"
    )
    ids_productos_invalidos = productos_rechazados.select("product_id").distinct()
    productos_inactivos = products.filter(F.col("active") == F.lit(False)).count()
    if mostrar:
        print(f"Productos con active=false conservados: {productos_inactivos}")

    # Los pedidos con customer_id nulo se distinguen de las referencias huérfanas.
    orders, orders_customer_nulo = agregar_condicion(
        orders,
        F.col("customer_id").isNull(),
        "orders",
        "customer_id nulo",
    )
    ids_customers = customers.select("customer_id").distinct()
    pedidos_huerfanos = orders.join(
        ids_customers, on="customer_id", how="left_anti"
    )
    orders = orders.join(ids_customers, on="customer_id", how="left_semi")
    orders_customer_huerfano = filas_cuarentena(
        pedidos_huerfanos, "orders", "customer_id huérfano"
    )

    # Cada línea se cuarentena una sola vez; la referencia a un producto inválido
    # tiene prioridad para identificar explícitamente la causa en cascada.
    lineas_producto_invalido = items.join(
        ids_productos_invalidos, on="product_id", how="left_semi"
    )
    items = items.join(ids_productos_invalidos, on="product_id", how="left_anti")
    lineas_producto_invalido = filas_cuarentena(
        lineas_producto_invalido, "order_items", "producto inválido"
    )
    items, lineas_cantidad_invalida = agregar_condicion(
        items,
        F.col("quantity") <= 0,
        "order_items",
        "quantity <= 0",
    )
    items, lineas_descuento_invalido = agregar_condicion(
        items,
        (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1),
        "order_items",
        "discount_pct fuera de [0,1]",
    )

    ids_orders = orders.select("order_id").distinct()
    lineas_order_huerfano = items.join(ids_orders, on="order_id", how="left_anti")
    items = items.join(ids_orders, on="order_id", how="left_semi")
    lineas_order_huerfano = filas_cuarentena(
        lineas_order_huerfano, "order_items", "order_id huérfano"
    )

    ids_products = products.select("product_id").distinct()
    lineas_product_huerfano = items.join(
        ids_products, on="product_id", how="left_anti"
    )
    items = items.join(ids_products, on="product_id", how="left_semi")
    lineas_product_huerfano = filas_cuarentena(
        lineas_product_huerfano, "order_items", "product_id huérfano"
    )

    clean_dfs = {
        "customers": customers.drop("_fila_original"),
        "products": products.drop("_fila_original"),
        "orders": orders.drop("_fila_original"),
        "order_items": items.drop("_fila_original"),
    }
    for tabla, dataframe in clean_dfs.items():
        if "source_rank" in dataframe.columns:
            clean_dfs[tabla] = dataframe.drop("source_rank")
    cuarentena = (
        clientes_duplicados.unionByName(productos_invalidos)
        .unionByName(pedidos_duplicados)
        .unionByName(orders_order_ts_invalido)
        .unionByName(orders_updated_at_invalido)
        .unionByName(orders_customer_nulo)
        .unionByName(orders_customer_huerfano)
        .unionByName(lineas_producto_invalido)
        .unionByName(lineas_cantidad_invalida)
        .unionByName(lineas_descuento_invalido)
        .unionByName(lineas_order_huerfano)
        .unionByName(lineas_product_huerfano)
    )
    if lineas_duplicadas is not None:
        cuarentena = cuarentena.unionByName(lineas_duplicadas)

    limpias = {tabla: dataframe.count() for tabla, dataframe in clean_dfs.items()}
    por_motivo = cuarentena.groupBy("tabla", "motivo").count().collect()
    cuarentena_por_tabla = {tabla: 0 for tabla in dfs}
    for fila in por_motivo:
        cuarentena_por_tabla[fila["tabla"]] += fila["count"]
    for tabla in dfs:
        total_cuarentena = cuarentena_por_tabla[tabla]
        assert entradas[tabla] == limpias[tabla] + total_cuarentena, (
            f"Conciliación inválida para {tabla}: entrada={entradas[tabla]}, "
            f"limpias={limpias[tabla]}, cuarentena={total_cuarentena}"
        )
    if mostrar:
        print("\nResumen de limpieza:")
        for tabla in dfs:
            print(
                f"  {tabla}: entrada={entradas[tabla]}, limpias={limpias[tabla]}, "
                f"cuarentena={cuarentena_por_tabla[tabla]}"
            )
            for fila in por_motivo:
                if fila["tabla"] == tabla:
                    print(f"    {fila['motivo']}: {fila['count']}")
        print("  Verificación: entrada = limpias + cuarentena para cada tabla.")

        print("\norder_items en cuarentena por motivo:")
        (
            cuarentena.filter(F.col("tabla") == "order_items")
            .groupBy("motivo")
            .count()
            .orderBy("motivo")
            .show(truncate=False)
        )

    lineas_por_producto_rechazado = (
        dfs["order_items"]
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "lineas_arrastradas")
    )
    productos_rechazados_reporte = (
        dfs["products"]
        .join(ids_productos_invalidos, on="product_id", how="left_semi")
        .select("product_id", "unit_price")
        .distinct()
        .join(lineas_por_producto_rechazado, on="product_id", how="left")
        .fillna({"lineas_arrastradas": 0})
        .orderBy("product_id")
    )
    if mostrar:
        print("\nProductos rechazados y líneas que arrastran:")
        productos_rechazados_reporte.show(truncate=False)

    return clean_dfs, cuarentena


def build_sales_detail(
    clean: dict[str, DataFrame], mostrar: bool = True
) -> DataFrame:
    """Construye el detalle de ventas a partir de las tablas limpias."""
    items = clean["order_items"].alias("items")
    orders = clean["orders"].alias("orders")
    customers = clean["customers"].alias("customers")
    products = clean["products"].alias("products")

    detalle = (
        items.join(
            orders,
            F.col("items.order_id") == F.col("orders.order_id"),
            "inner",
        )
        .join(
            customers,
            F.col("orders.customer_id") == F.col("customers.customer_id"),
            "inner",
        )
        .join(
            products,
            F.col("items.product_id") == F.col("products.product_id"),
            "inner",
        )
        .select(
            F.col("orders.order_id").alias("order_id"),
            F.col("items.line_id").alias("line_id"),
            F.date_format(F.col("orders.order_ts"), "yyyy-MM").alias("year_month"),
            F.col("customers.country").alias("country"),
            F.col("orders.channel").alias("channel"),
            F.col("products.category").alias("category"),
            F.col("orders.status").alias("status"),
            F.col("items.quantity").alias("quantity"),
            F.col("products.unit_price").alias("unit_price"),
            F.col("items.discount_pct").alias("discount_pct"),
            F.round(
                F.col("items.quantity")
                * F.col("products.unit_price")
                * (1 - F.col("items.discount_pct")),
                2,
            )
            .cast(DecimalType(18, 2))
            .alias("net_amount"),
        )
    )

    conteo_items = clean["order_items"].count()
    conteo_detalle = detalle.count()
    assert conteo_detalle == conteo_items, (
        f"Los joins alteraron la cantidad de filas: "
        f"order_items={conteo_items}, detalle={conteo_detalle}"
    )

    columnas_obligatorias = ["order_id", "line_id", "net_amount"]
    nulos = detalle.agg(
        *[
            F.sum(F.when(F.col(columna).isNull(), 1).otherwise(0)).alias(columna)
            for columna in columnas_obligatorias
        ]
    ).first()
    assert all(not nulos[columna] for columna in columnas_obligatorias), (
        f"El detalle contiene nulos en columnas clave o net_amount: "
        f"{ {columna: nulos[columna] for columna in columnas_obligatorias} }"
    )
    nulos_year_month = detalle.filter(F.col("year_month").isNull()).count()
    assert nulos_year_month == 0, (
        f"El detalle contiene {nulos_year_month} filas sin year_month"
    )

    suma_neta = detalle.agg(F.sum("net_amount").alias("suma_net_amount")).first()[
        "suma_net_amount"
    ] or Decimal("0.00")
    if mostrar:
        print(
            f"\nDetalle de ventas: filas={conteo_detalle}, "
            f"suma net_amount={suma_neta:.2f}"
        )
    return detalle


def aggregate_sales(detail: DataFrame, mostrar: bool = True) -> DataFrame:
    """Agrega ventas no canceladas con DataFrames y Spark SQL, y contrasta resultados."""
    dimensiones = ["year_month", "country", "channel", "category"]
    ventas_reales = detail.filter(F.col("status") != "cancelled")
    agregado_df = ventas_reales.groupBy(*dimensiones).agg(
        F.round(F.sum("net_amount"), 2)
        .cast(DecimalType(18, 2))
        .alias("net_sales"),
        F.countDistinct("order_id").alias("orders"),
        F.count("line_id").alias("lines"),
    )

    detail.createOrReplaceTempView("sales_detail")
    agregado_sql = detail.sparkSession.sql(
        """
        SELECT
            year_month,
            country,
            channel,
            category,
            CAST(ROUND(SUM(net_amount), 2) AS DECIMAL(18, 2)) AS net_sales,
            COUNT(DISTINCT order_id) AS orders,
            COUNT(line_id) AS lines
        FROM sales_detail
        WHERE status <> 'cancelled'
        GROUP BY year_month, country, channel, category
        """
    )

    conteo_df = agregado_df.count()
    conteo_sql = agregado_sql.count()
    total_df = agregado_df.agg(F.sum("net_sales")).first()[0] or Decimal("0.00")
    total_sql = agregado_sql.agg(F.sum("net_sales")).first()[0] or Decimal("0.00")
    assert conteo_df == conteo_sql, (
        f"Las agregaciones difieren en filas: DataFrames={conteo_df}, SQL={conteo_sql}"
    )
    assert abs(total_df - total_sql) <= Decimal("0.01"), (
        f"Las agregaciones difieren en suma: DataFrames={total_df}, SQL={total_sql}"
    )

    diferencias_df = agregado_df.exceptAll(agregado_sql).count()
    diferencias_sql = agregado_sql.exceptAll(agregado_df).count()
    assert diferencias_df == 0 and diferencias_sql == 0, (
        "Las agregaciones no producen los mismos grupos y métricas: "
        f"solo DataFrames={diferencias_df}, solo SQL={diferencias_sql}"
    )

    ventas_reales_total = ventas_reales.agg(F.sum("net_amount")).first()[0] or Decimal("0.00")
    suma_cancelled = (
        detail.filter(F.col("status") == "cancelled")
        .agg(F.sum("net_amount"))
        .first()[0]
        or Decimal("0.00")
    )
    suma_agregado = total_df
    assert suma_agregado == ventas_reales_total, (
        "No se cumple la reconciliación exacta: "
        f"agregado={suma_agregado:.2f}, detalle no cancelado={ventas_reales_total:.2f}"
    )

    if mostrar:
        suma_detalle_total = detail.agg(F.sum("net_amount")).first()[0] or Decimal("0.00")
        print("\nControl de agregación:")
        print(f"  Suma net_amount del detalle total: {suma_detalle_total:.2f}")
        print(f"  Suma net_amount de pedidos cancelled: {suma_cancelled:.2f}")
        print(f"  Suma net_sales del agregado: {suma_agregado:.2f}")
        print(
            "  Reconciliación exacta detalle no cancelado = agregado: "
            f"{ventas_reales_total:.2f} = {suma_agregado:.2f}"
        )
        print(
            f"  Resultados DataFrames y SQL idénticos: sí "
            f"({conteo_df} grupos; diferencia de suma={abs(total_df - total_sql):.2f})"
        )
        print("\nTop 10 grupos por net_sales:")
        agregado_df.orderBy(F.col("net_sales").desc()).show(10, truncate=False)
    return agregado_df


def apply_incremental(
    spark: SparkSession, mostrar: bool = True
) -> dict[str, int | Decimal]:
    """Reprocesa histórico e incremental desde crudo y sobrescribe las salidas."""
    historico = read_raw(spark, mostrar=mostrar)
    archivos_incrementales = {
        "orders": "orders_incremental.jsonl",
        "order_items": "order_items_incremental.jsonl",
    }

    for tabla, archivo in archivos_incrementales.items():
        incremental = spark.read.json(str(DATA_DIR / "incremental" / archivo))
        columnas_esperadas = historico[tabla].columns
        faltantes = set(columnas_esperadas) - set(incremental.columns)
        if faltantes:
            raise ValueError(
                f"El incremental de {tabla} no contiene columnas requeridas: "
                f"{sorted(faltantes)}"
            )
        incremental_crudo = incremental.select(
            *[
                F.col(columna).cast("string").alias(columna)
                for columna in columnas_esperadas
            ]
        )
        historico_crudo = historico[tabla].select(
            *[
                F.col(columna).cast("string").alias(columna)
                for columna in columnas_esperadas
            ]
        )
        historico_crudo = historico_crudo.withColumn("source_rank", F.lit(0))
        incremental_crudo = incremental_crudo.withColumn("source_rank", F.lit(1))
        historico[tabla] = historico_crudo.unionByName(incremental_crudo)

    entradas = {tabla: dataframe.count() for tabla, dataframe in historico.items()}
    assert entradas["orders"] == 20102, (
        f"Se esperaban 20102 orders crudos, se encontraron {entradas['orders']}"
    )
    assert entradas["order_items"] == 40226, (
        f"Se esperaban 40226 order_items crudos, se encontraron "
        f"{entradas['order_items']}"
    )

    clean_dfs, quarantine = clean(historico, mostrar=mostrar)
    detail = build_sales_detail(clean_dfs, mostrar=mostrar)
    sales_monthly = aggregate_sales(detail, mostrar=mostrar)

    fila_pedido = clean_dfs["orders"].filter(F.col("order_id") == "O00005")
    assert fila_pedido.count() == 1, "O00005 debe aparecer una sola vez en orders limpio"
    timestamp_esperado = F.expr(
        "try_cast('2026-03-05T12:00:00Z' AS TIMESTAMP)"
    )
    assert fila_pedido.filter(F.col("updated_at") == timestamp_esperado).count() == 1, (
        "O00005 no tiene updated_at = 2026-03-05T12:00:00Z"
    )
    linea_presente = (
        clean_dfs["order_items"]
        .filter(F.col("line_id") == "L0000009")
        .limit(1)
        .count()
        == 1
    )
    assert linea_presente, "La línea histórica L0000009 debe conservarse"
    pedidos_duplicados = (
        clean_dfs["orders"]
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    assert pedidos_duplicados == 0, (
        f"Quedaron {pedidos_duplicados} order_id duplicados en orders limpio"
    )
    assert entradas["orders"] == clean_dfs["orders"].count() + quarantine.filter(
        F.col("tabla") == "orders"
    ).count()
    assert entradas["order_items"] == clean_dfs["order_items"].count() + quarantine.filter(
        F.col("tabla") == "order_items"
    ).count()
    if mostrar:
        print(
            "Integridad verificada: orders 20102 = "
            f"{clean_dfs['orders'].count()} limpias + "
            f"{quarantine.filter(F.col('tabla') == 'orders').count()} cuarentena; "
            "order_items 40226 = "
            f"{clean_dfs['order_items'].count()} limpias + "
            f"{quarantine.filter(F.col('tabla') == 'order_items').count()} cuarentena."
        )
        print(
            "Integridad verificada: sin order_id duplicados; "
            "O00005 actualizado y L0000009 conservada."
        )

    output_dir = DATA_DIR.parent / "output"
    salidas = {
        "sales_detail": detail,
        "sales_monthly": sales_monthly,
        "quarantine": quarantine,
    }
    for nombre, dataframe in salidas.items():
        escritor = dataframe.write.mode("overwrite")
        if nombre == "sales_monthly":
            escritor = escritor.partitionBy("year_month")
        escritor.parquet(str(output_dir / nombre))

    detalle_disco = spark.read.parquet(str(output_dir / "sales_detail"))
    mensual_disco = spark.read.parquet(str(output_dir / "sales_monthly"))
    cuarentena_disco = spark.read.parquet(str(output_dir / "quarantine"))
    conteo_detalle_disco = detalle_disco.count()
    conteo_mensual_disco = mensual_disco.count()
    conteo_cuarentena_disco = cuarentena_disco.count()
    if mostrar:
        print("\nConteos Parquet releídos desde disco:")
        print(f"  output/sales_detail: {conteo_detalle_disco}")
        print(f"  output/sales_monthly: {conteo_mensual_disco}")
        print(f"  output/quarantine: {conteo_cuarentena_disco}")
        print("\nCinco filas de output/sales_monthly:")
        mensual_disco.show(5, truncate=False)

    metricas = {
        "orders": clean_dfs["orders"].count(),
        "order_items": clean_dfs["order_items"].count(),
        "detail_net_amount": detail.agg(F.sum("net_amount")).first()[0]
        or Decimal("0.00"),
        "aggregate_rows": sales_monthly.count(),
    }
    if mostrar:
        print(
            "Resultados de esta ejecución: "
            f"orders={metricas['orders']}, order_items={metricas['order_items']}, "
            f"suma net_amount={metricas['detail_net_amount']:.2f}, "
            f"filas sales_monthly={metricas['aggregate_rows']}"
        )
    return metricas


def main() -> None:
    spark = get_spark()
    try:
        primera_ejecucion = apply_incremental(spark)
        segunda_ejecucion = apply_incremental(spark, mostrar=False)
        assert primera_ejecucion == segunda_ejecucion, (
            "El reprocesamiento incremental no fue idempotente: "
            f"primera={primera_ejecucion}, segunda={segunda_ejecucion}"
        )
        print(
            "Idempotencia verificada: conteos, suma del detalle y filas agregadas "
            "son idénticos en ambas ejecuciones."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

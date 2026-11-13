
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, year, month, count

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("JVGYM_BigData") \
    .master("local[*]") \
    .getOrCreate()

# Definir nombres de las columnas
columnas = [
    "ID_Inscripcion",
    "ID_Cliente",
    "ID_Sede",
    "ID_Membresia",
    "Fecha_Inicio",
    "Fecha_Fin",
    "Monto_Pagado"
]

# Leer archivo CSV
df = spark.read.csv(
    "Resultados.csv",
    sep=";",
    header=False,
    inferSchema=True
)

# Asignar nombres a las columnas
df = df.toDF(*columnas)

# Mostrar los datos
print("DATOS DE JVGYM")
df.show()

# Mostrar estructura
print("ESTRUCTURA DEL DATAFRAME")
df.printSchema()

# Contar registros
print("TOTAL DE REGISTROS:", df.count())


# 1. CALCULAR LOS INGRESOS POR MES
ingresos_mensuales = df.groupBy(
    year("Fecha_Inicio").alias("Anio"),
    month("Fecha_Inicio").alias("Mes")
).agg(
    sum("Monto_Pagado").alias("Ingresos_Totales")
)

ingresos_mensuales = ingresos_mensuales.orderBy("Anio", "Mes")

print("INGRESOS MENSUALES DE JVGYM")
ingresos_mensuales.show()


# 2. CONTAR LAS INSCRIPCIONES POR SEDE
inscripciones_sede = df.groupBy(
    "ID_Sede"
).agg(
    count("ID_Inscripcion").alias("Total_Inscripciones")
)

inscripciones_sede = inscripciones_sede.orderBy(
    "Total_Inscripciones", ascending=False
)

print("INSCRIPCIONES POR SEDE")
inscripciones_sede.show()


# 3. IDENTIFICAR LAS MEMBRESIAS MAS SOLICITADAS
membresias_solicitadas = df.groupBy(
    "ID_Membresia"
).agg(
    count("ID_Inscripcion").alias("Total_Solicitudes")
)

membresias_solicitadas = membresias_solicitadas.orderBy(
    "Total_Solicitudes", ascending=False
)

print("MEMBRESIAS MAS SOLICITADAS")
membresias_solicitadas.show()


# Cerrar sesión de Spark
spark.stop()



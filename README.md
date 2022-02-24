## Proyecto
Desarrollo de arquitectura Lambda para el procesamiento de datos recolecatados desde antenas de telefonía móvil.

## Capas de desarollo

### Speed Layer
Capa de procesamiento en streaming, tecnologías de uso Kafka, Spark Structured Streaming y Google Compute Engine.

### Batch Layer
Capa de procesamiento por lotes, tecnologías de uso Spark SQL y Google Cloud SQL.

### Serving Layer
Capa de muestra de datos, servicio final, obtenidos por medio de los dos anteriores procesos, tecnologías de uso Apache Superset

## Fuentes de datos

### Datos en tiempo real
Datos recibidos en tiempo real desde antenas (simulador de Docker). La información del simulador llega al sistema de mensajes de Kafka, al topic `devices`.

### Metadatos sobre usuarios
Contamos con una base de datos con información del usuario, gestionada por operadores, almacenada en Google Clound SQL (PostgreSQL). Se trata de una infromación estática, relacionada con la fuente de datos en tiempo real. 
Implementar en Spark codigo de provisionamiento de datos y tablas (PostgreSQL), donde almacenaremos datos y métricas, `JdbcProvisioner`.

## Objetivo

### Speed Layer
* Desarrollar job de Spark Structured Streaming para recolectar la información en tiempo real, almacenar la misma en Kafka, procesar los datos y calcular las métricas agregadas cada 5 minutos, cuardadndo los datos en PostgreSQL. Métricas:
   * Total bytes recibidos por antena; 
   * Total bytes transmitidos por id de usuario; 
   * Total bytes transmitidos por aplicación;
* Almacenar los datos recolectados por AÑO/MES/DÍA/HORA, en tiempo real, en formato PARQUET, se almacena en local. 

### Speed Layer
* Desarrolar job de Spark SQL que calcula las metricas agregadas de los datos almacenados en PARQUET, almacenando dichas métricas en PostgreSQL. Métricas:
   * Total bytes recibidos por antena; 
   * Total bytes transmitidos por id de usuario; 
   * Total bytes transmitidos por aplicación; 
   * Email de usuarios que han sobrepasado la cuota por hora;

### Serving Layer
Desarrollar dashboard utilizando Apache Superset que muestre métricas y datos relevantes obtenidas por medio de los job speed y batch.  

## Implementación

### Speed Layer

* Trabajo previo y logística:
  * Configuración de VM Compute Engine, configurar Kafka y Docker (simulador de datos en tiempo real). Establecer permisos de conexión, IP, etc.
  * Crear instancia de Google Cloud Engine (PostgreSQL), donde volcaremos los metadata de usuarios y crearemos las tablas para las futuras inserciones de métricas. 
* `def readFromKafka` Leer datos desde topic `device` de Kafka.
* `def parserJsonData` Los datos obtenidos desde Kafka no tinene la estriuctura adecuada para un posterior procesamiento. Para ello debemos pre-procesar los datos, obteniendo la estructura (schema) y tipado adecuado.
* `def totalBytesAntenna` `def totalBytesUser` `def totalBytesApp` Pasamos a desarrollar el cálculo de métricas agregadas, obteniendo el schema necesario marcado previamente por la tabla que almacenará dichos datos, creada en PostgreSQL.
* `def writeToJdbc` Los tres DataFrame obtenidos anteriormente se vuelcan/escriben en las tablas de PostgreSQL: `bytes`. Al ser trabajos que deben ser ejecutados de forma asíncrona debemos devolver las tres escrituras como un `Future`, para la ejecución de trabajos en paralelo. 
* `override def writeToStorage` Guardamos los datos de entrada en local, particionándolos por año, mes, día y hora, en formato PARQUET. AL igual que anteriormete este trabajo debe ejecutarse de forma asíncrona, por lo que devuelve un `Future`.
* `def main(args: Array[String]): Unit = run(args)` Los argumentos necesarios para ejecutar Spark Structured Streaming Job son:
```
"kafkaServer" "topicName" "jdbc:Uri" "jdbcTable" "jdbcUser" "jdbcPassword" "StorageRootPath"
```

### Batch Layer

* `def readFromStorage` Obtenemos los datos almacenados en local, en formato PARQUET, por serie temporal, que se especifica en los argumentos.
* `def hourlyTotalBytesAntenna` `hourlyTotalBytesUser` `hourlyTotalBytesApp` Una vez obtenido el DataFrame temporal, calculamos las métricas agregadas por hora.
* `readDataPSQL` Desde PostgreSQL obtenemos los metadatos de los usurios.
* `readDataPSQL` Desde PostgreSQL obtenemos el total de bytes, por hora, transmitidos por id de usuario.
* `def enrichMetadata` Enriquecemos el DataFrame de total de bytes con el DataFrame de metadatos de usuario, para obtener la información encesaria para el cálculo de la siguiente métrica.
* `def usersWithExceededQuota` Obtenemos el email de usuario que ha superado su límite de cuota por hora.
* `def writeToJdbc` Insertamos los datos de las cuatro métricas en sus respectivas tablas en PostgreSQL.

* `def main(args: Array[String]): Unit = run(args)` Los argumentos necesarios para ejecutar Spark SQL Job son:
```
"StoragePath" "OffsetDateTime" "jdbc:Uri" "userMetadataTable" "bytesHourlyTable" "userQuotaLimitTable" "jdbcUser" "jdbcPassword"
```


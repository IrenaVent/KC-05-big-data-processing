## Proyecto
Desarrollo de arquitectura Lambda para el procesamiento de datos recolectados desde antenas de telefonía móvil.

## Capas de desarollo

### Speed Layer
Capa de procesamiento en streaming. Tecnologías de uso: Kafka, Spark Structured Streaming y Google Compute Engine.

### Batch Layer
Capa de procesamiento por lotes. Tecnologías de uso: Spark SQL y Google Cloud SQL.

### Serving Layer
Capa de muestra de datos, servicio final, obtenidos por medio de los dos anteriores procesos. Tecnologías de uso: Apache Superset.

## Fuentes de datos

### Datos en tiempo real
Datos recibidos en tiempo real desde antenas (simulador de Docker). La información del simulador llega al sistema de mensajes de Kafka, al topic `devices`.

### Metadatos sobre usuarios
Contamos con una base de datos con información del usuario, gestionada por operadores, almacenada en Google Clound SQL (PostgreSQL). Se trata de una infromación estática, relacionada con la fuente de datos en tiempo real. 
Se debe implementar en Spark el código de provisionamiento de datos y tablas (PostgreSQL) `JdbcProvisioner`, donde almacenaremos datos y métricas.

## Objetivo

### Speed Layer
* Desarrollar job de Spark Structured Streaming para recolectar la información en tiempo real, almacenar la misma en Kafka, procesar los datos y calcular las métricas agregadas cada 5 minutos, guardando los datos en PostgreSQL. Métricas:
   * Total bytes recibidos por antena; 
   * Total bytes transmitidos por id de usuario; 
   * Total bytes transmitidos por aplicación;
* Almacenar los datos recolectados por AÑO/MES/DÍA/HORA, en tiempo real, en formato PARQUET. Se almacena en local. 

### Speed Layer
* Desarrollar job de Spark SQL que calcula las métricas agregadas de los datos almacenados en PARQUET, almacenando dichas métricas en PostgreSQL. Métricas:
   * Total bytes recibidos por antena; 
   * Total bytes transmitidos por id de usuario; 
   * Total bytes transmitidos por aplicación; 
   * Email de usuarios que han sobrepasado la cuota por hora;

### Serving Layer
Desarrollar dashboard utilizando Apache Superset que muestre métricas y datos relevantes obtenidos por medio de los job speed y batch.  

## Implementación

### Speed Layer

* Trabajo previo y logística:
  * Configuración de VM Compute Engine, configurar Kafka y Docker (simulador de datos en tiempo real). Establecer permisos de conexión, IP, etc.
  * Crear instancia de Google Cloud SQL (PostgreSQL), donde volcaremos los metadatos de usuarios y crearemos las tablas para las futuras inserciones de las métricas. 
* `def readFromKafka`: leer datos desde topic `device` de Kafka.
* `def parserJsonData`: los datos obtenidos desde Kafka no tienen la estructura adecuada para un posterior procesamiento. Para ello debemos pre-procesar los datos, obteniendo la estructura (schema) y tipado adecuado.
* `def totalBytesAntenna` `def totalBytesUser` `def totalBytesApp`: pasamos a desarrollar el cálculo de métricas agregadas, obteniendo el schema necesario marcado previamente por la tabla que almacenará dichos datos, creada en PostgreSQL.
* `def writeToJdbc`: los tres DataFrame obtenidos anteriormente se vuelcan/escriben en la tabla `bytes` en PostgreSQL. Al ser trabajos que deben ser ejecutados de forma asíncrona debemos devolver las tres escrituras como un `Future` para la ejecución de trabajos en paralelo. 
* `def writeToStorage`: guardamos los datos de entrada en local, particionándolos por año, mes, día y hora, en formato PARQUET. Al igual que anteriormente, este trabajo debe ejecutarse de forma asíncrona, por lo que devuelve un `Future`.
* `def main(args: Array[String]): Unit = run(args)`: los argumentos necesarios para ejecutar Spark Structured Streaming Job, IDE Edit Configurations...son:
```
"kafkaServer" "topicName" "jdbc:Uri" "jdbcTable" "jdbcUser" "jdbcPassword" "StorageRootPath"
```

### Batch Layer

* `def readFromStorage`: obtener los datos almacenados en local, en formato PARQUET, por serie temporal, que se especifica en los argumentos.
* `def hourlyTotalBytesAntenna` `hourlyTotalBytesUser` `hourlyTotalBytesApp`: una vez obtenido el DataFrame temporal, calculamos las métricas agregadas por hora.
* `def writeToJdbc`: insertar los datos de las tres métricas en sus respectivas tablas en PostgreSQL.
* `readDataPSQL`: desde PostgreSQL, obtener los metadatos de los usuarios.
* `readDataPSQL`: desde PostgreSQL, obtener el total de bytes, por hora, transmitidos por id de usuario.
* `def enrichMetadata`: enriquecer el DataFrame de total de bytes con el DataFrame de metadatos de usuario, para obtener la información necesaria para el cálculo de la siguiente métrica.
* `def usersWithExceededQuota`: obtener el email de usuario que ha superado su límite de cuota por hora.
* `def writeToJdbc`: insertar los datos de la cuarta métrica su respectiva tabla en PostgreSQL.
* `def main(args: Array[String]): Unit = run(args)` los argumentos necesarios para ejecutar Spark SQL Job, IDE Edit Configurations... son:
```
"StoragePath" "OffsetDateTime" "jdbc:Uri" "userMetadataTable" "bytesHourlyTable" "userQuotaLimitTable" "jdbcUser" "jdbcPassword"
```


// Databricks notebook source
// Tarea: Realizar 3 búsquedas con comandos SQL y 3 búsquedas usando la librería de Grafos
// Alumno: Martiñón Luna Jonathan José
// Materia: Datos Masivos I
// Fecha: Abril 16, 2020

// COMMAND ----------

//Objetivo: realizar análisis de flujo de clicks con datos recolectados de Wikipedia (Clickstream Analysis).
//Data: https://old.datahub.io/dataset/wikipedia-clickstream/resource/be85cc68-d1e6-4134-804a-fd36b94dbb82
//Requerimientos: Scala
//Este notebook está basado de: WikipediaClickStream desarrollado por Raazesh Sainudiin y Sivanand Sivaram.

// COMMAND ----------

// Quería ver los posibles archivos a leer, ya vi que son del 00000 al 00009

// COMMAND ----------

// MAGIC %fs ls dbfs:/datasets/wiki-clickstream/

// COMMAND ----------

// Seleccioné el archivo 00005
//Cargamos los datos a la variable clickstream
val clickstream = sqlContext.read.parquet("/datasets/wiki-clickstream/part-00005-tid-506950732109671714-3b06b104-a7d9-4326-9887-49fab501d371-637-1-c000.snappy.parquet")

// COMMAND ----------

//Visualizamos el esquema de los datos 'clickstream'
clickstream.printSchema

// COMMAND ----------

//Visualizamos algunos registros del dataframe
display(clickstream)

// COMMAND ----------

clickstream.show(5)//Otra forma de visualizar los registros del dataframe

// COMMAND ----------

//Vamos a visualizar cuántos registros existen en el dataframe
clickstream.count()

// COMMAND ----------

// Supongo que es apartir de aquí que nosotros hacemos las consultas

// COMMAND ----------

// Consulta 1

// Quise ver las 5 páginas más visitadas en wikipedia desde Facebook
display(clickstream
        .select(clickstream("curr_title"), clickstream("prev_title"), clickstream("n"))
        .filter("prev_title = 'other-facebook'")
        .groupBy("curr_title").sum()
        .orderBy($"sum(n)".desc)
        .limit(5))
// Debo admitir que fue raro pensar que lo que más se consultaba era el Mounstro Come Galletas
// Con una notable diferencia sobre los demás.

// COMMAND ----------

// Consulta 2

// Quise ver la cantidad de veces que se llegó a Wikipedia desde google
// Pues a mi parecer es la forma más normal.
display(clickstream
        .select(clickstream("prev_title"), clickstream("n"))
        .filter("prev_title = 'other-google'")
        .groupBy("prev_title").sum())
// En un principio pensé que había un error, pues el total
// era mayor que el total de registros
// Total: 2305690
// Suma:  154047227
// Después noté que estaba sumando las 'n', no contando registros, 
// en la celda de abajo cuento registros

// COMMAND ----------

// Consulta 3

display(clickstream
        .select(clickstream("prev_title"), clickstream("n"))
        .filter("prev_title = 'other-google'")
        .groupBy("prev_title").count())
// Y como podemos ver ya habla de los registros en la tabla, SUPONGO
// que nuestra 'n' se refiere al número de veces que desde 'prev_title'
// se llegó a 'curr_title', por lo que al sumar no será igual al número de
// registros, pues puede que una página haya sido usada n veces para llegar y no solo una
// Totales:  2305690
// Contados: 254800

// COMMAND ----------

//Paso 4: creamos una tabla temporal
clickstream.createOrReplaceTempView("clicks_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC --- Consulta 1 en SQL
// MAGIC 
// MAGIC --- Nuevamente creí que existían un error, pues la consulta:
// MAGIC 
// MAGIC -- SELECT *
// MAGIC --   FROM clicks_table
// MAGIC --   WHERE 
// MAGIC --     prev_title = 'other-yahoo'  AND
// MAGIC --     prev_id IS NOT NULL AND prev_title != 'Main_Page'
// MAGIC --   ORDER BY n DESC
// MAGIC --   LIMIT 2
// MAGIC 
// MAGIC --  Sólamente arrojaba un 'OK', sin embargo, después de analizar la tabla
// MAGIC -- Display, me di cuenta que practicamente todos los prev_title que 
// MAGIC -- inicien con 'other-' tienen prev_id = 'null', por lo que no arrojaría nada
// MAGIC -- Por lo que simplemente porcedí a quitar esa restricción y funcionó
// MAGIC -- Pues ya hay datos que cumplan con lo que pido
// MAGIC -- Igual quité el 'Main_Page', pues ya le estoy diciendo que solo quiero
// MAGIC -- Aquellos que sean 'other-yahoo', así que no es necesario aclarar lo otro.
// MAGIC 
// MAGIC --- Páginas de wikipedia que llegaron desde yahoo
// MAGIC SELECT *
// MAGIC   FROM clicks_table
// MAGIC   WHERE 
// MAGIC     prev_title = 'other-yahoo'
// MAGIC   ORDER BY n DESC
// MAGIC   LIMIT 2
// MAGIC -- No son muchas :$

// COMMAND ----------

// MAGIC %sql
// MAGIC --- Consulta 2 en SQL
// MAGIC 
// MAGIC -- Teniendo en cuenta lo anterior, supuse que lo mejor sería No tomar páginas
// MAGIC -- con prev_title ='other-...'
// MAGIC -- Viendo los títulos encontré 'Stephen_King_bibliography', me dio curisidad
// MAGIC -- Saber a dónde se movía la gente desde ahí
// MAGIC 
// MAGIC --- ¿A qué páginas se mueven las personas después de ver a la bibliografía de Stephen King?
// MAGIC SELECT prev_title,curr_title, n
// MAGIC   FROM clicks_table
// MAGIC   WHERE 
// MAGIC     prev_title = 'Stephen_King_bibliography' AND
// MAGIC     prev_id IS NOT NULL
// MAGIC   ORDER BY n DESC
// MAGIC   LIMIT 30

// COMMAND ----------

// MAGIC %sql
// MAGIC --- Consulta 3 en SQL
// MAGIC 
// MAGIC -- Finalmente pensé en tomar en cuenta algún curr_title, pero ninguno llamó
// MAGIC -- mi atención, así que decidí continuar con pre_title
// MAGIC 
// MAGIC --- ¿Cuántas páginas de wikipedia se visitan partiendo de ver Iraq?
// MAGIC SELECT prev_title,curr_title,n 
// MAGIC   FROM clicks_table
// MAGIC   WHERE 
// MAGIC     prev_title = 'Iraq' AND
// MAGIC     prev_id IS NOT NULL
// MAGIC   ORDER BY n DESC
// MAGIC   LIMIT 30

// COMMAND ----------

//Paso 5: visualización de consultas

// COMMAND ----------

// MAGIC %scala
// MAGIC package d3
// MAGIC  // We use a package object so that we can define top level classes like Edge that need to be used in other cells
// MAGIC //Este código fue desarrollado por "Michael Armbrust at Spark Summit East February 2016"
// MAGIC  
// MAGIC  import org.apache.spark.sql._
// MAGIC  import com.databricks.backend.daemon.driver.EnhancedRDDFunctions.displayHTML
// MAGIC  
// MAGIC  case class Edge(src: String, dest: String, count: Long)
// MAGIC  
// MAGIC  case class Node(name: String)
// MAGIC  case class Link(source: Int, target: Int, value: Long)
// MAGIC  case class Graph(nodes: Seq[Node], links: Seq[Link])
// MAGIC  
// MAGIC  object graphs {
// MAGIC  val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate())  
// MAGIC  import sqlContext.implicits._
// MAGIC    
// MAGIC  def force(clicks: Dataset[Edge], height: Int = 100, width: Int = 960): Unit = {
// MAGIC    val data = clicks.collect()
// MAGIC    val nodes = (data.map(_.src) ++ data.map(_.dest)).map(_.replaceAll("_", " ")).toSet.toSeq.map(Node)
// MAGIC    val links = data.map { t =>
// MAGIC      Link(nodes.indexWhere(_.name == t.src.replaceAll("_", " ")), nodes.indexWhere(_.name == t.dest.replaceAll("_", " ")), t.count / 20 + 1)
// MAGIC    }
// MAGIC    showGraph(height, width, Seq(Graph(nodes, links)).toDF().toJSON.first())
// MAGIC  }
// MAGIC  
// MAGIC  /**
// MAGIC   * Displays a force directed graph using d3
// MAGIC   * input: {"nodes": [{"name": "..."}], "links": [{"source": 1, "target": 2, "value": 0}]}
// MAGIC   */
// MAGIC  def showGraph(height: Int, width: Int, graph: String): Unit = {
// MAGIC  
// MAGIC  displayHTML(s"""
// MAGIC  <!DOCTYPE html>
// MAGIC  <html>
// MAGIC  <head>
// MAGIC    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
// MAGIC    <title>Polish Books Themes - an Interactive Map</title>
// MAGIC    <meta charset="utf-8">
// MAGIC  <style>
// MAGIC  
// MAGIC  .node_circle {
// MAGIC    stroke: #777;
// MAGIC    stroke-width: 1.3px;
// MAGIC  }
// MAGIC  
// MAGIC  .node_label {
// MAGIC    pointer-events: none;
// MAGIC  }
// MAGIC  
// MAGIC  .link {
// MAGIC    stroke: #777;
// MAGIC    stroke-opacity: .2;
// MAGIC  }
// MAGIC  
// MAGIC  .node_count {
// MAGIC    stroke: #777;
// MAGIC    stroke-width: 1.0px;
// MAGIC    fill: #999;
// MAGIC  }
// MAGIC  
// MAGIC  text.legend {
// MAGIC    font-family: Verdana;
// MAGIC    font-size: 13px;
// MAGIC    fill: #000;
// MAGIC  }
// MAGIC  
// MAGIC  .node text {
// MAGIC    font-family: "Helvetica Neue","Helvetica","Arial",sans-serif;
// MAGIC    font-size: 17px;
// MAGIC    font-weight: 200;
// MAGIC  }
// MAGIC  
// MAGIC  </style>
// MAGIC  </head>
// MAGIC  
// MAGIC  <body>
// MAGIC  <script src="//d3js.org/d3.v3.min.js"></script>
// MAGIC  <script>
// MAGIC  
// MAGIC  var graph = $graph;
// MAGIC  
// MAGIC  var width = $width,
// MAGIC      height = $height;
// MAGIC  
// MAGIC  var color = d3.scale.category20();
// MAGIC  
// MAGIC  var force = d3.layout.force()
// MAGIC      .charge(-700)
// MAGIC      .linkDistance(180)
// MAGIC      .size([width, height]);
// MAGIC  
// MAGIC  var svg = d3.select("body").append("svg")
// MAGIC      .attr("width", width)
// MAGIC      .attr("height", height);
// MAGIC      
// MAGIC  force
// MAGIC      .nodes(graph.nodes)
// MAGIC      .links(graph.links)
// MAGIC      .start();
// MAGIC  
// MAGIC  var link = svg.selectAll(".link")
// MAGIC      .data(graph.links)
// MAGIC      .enter().append("line")
// MAGIC      .attr("class", "link")
// MAGIC      .style("stroke-width", function(d) { return Math.sqrt(d.value); });
// MAGIC  
// MAGIC  var node = svg.selectAll(".node")
// MAGIC      .data(graph.nodes)
// MAGIC      .enter().append("g")
// MAGIC      .attr("class", "node")
// MAGIC      .call(force.drag);
// MAGIC  
// MAGIC  node.append("circle")
// MAGIC      .attr("r", 10)
// MAGIC      .style("fill", function (d) {
// MAGIC      if (d.name.startsWith("other")) { return color(1); } else { return color(2); };
// MAGIC  })
// MAGIC  
// MAGIC  node.append("text")
// MAGIC        .attr("dx", 10)
// MAGIC        .attr("dy", ".35em")
// MAGIC        .text(function(d) { return d.name });
// MAGIC        
// MAGIC  //Now we are giving the SVGs co-ordinates - the force layout is generating the co-ordinates which this code is using to update the attributes of the SVG elements
// MAGIC  force.on("tick", function () {
// MAGIC      link.attr("x1", function (d) {
// MAGIC          return d.source.x;
// MAGIC      })
// MAGIC          .attr("y1", function (d) {
// MAGIC          return d.source.y;
// MAGIC      })
// MAGIC          .attr("x2", function (d) {
// MAGIC          return d.target.x;
// MAGIC      })
// MAGIC          .attr("y2", function (d) {
// MAGIC          return d.target.y;
// MAGIC      });
// MAGIC      d3.selectAll("circle").attr("cx", function (d) {
// MAGIC          return d.x;
// MAGIC      })
// MAGIC          .attr("cy", function (d) {
// MAGIC          return d.y;
// MAGIC      });
// MAGIC      d3.selectAll("text").attr("x", function (d) {
// MAGIC          return d.x;
// MAGIC      })
// MAGIC          .attr("y", function (d) {
// MAGIC          return d.y;
// MAGIC      });
// MAGIC  });
// MAGIC  </script>
// MAGIC  </html>
// MAGIC  """)
// MAGIC  }
// MAGIC    
// MAGIC    def help() = {
// MAGIC  displayHTML("""
// MAGIC  <p>
// MAGIC  Produces a force-directed graph given a collection of edges of the following form:</br>
// MAGIC  <tt><font color="#a71d5d">case class</font> <font color="#795da3">Edge</font>(<font color="#ed6a43">src</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">dest</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">count</font>: <font color="#a71d5d">Long</font>)</tt>
// MAGIC  </p>
// MAGIC  <p>Usage:<br/>
// MAGIC  <tt>%scala</tt></br>
// MAGIC  <tt><font color="#a71d5d">import</font> <font color="#ed6a43">d3._</font></tt><br/>
// MAGIC  <tt><font color="#795da3">graphs.force</font>(</br>
// MAGIC  &nbsp;&nbsp;<font color="#ed6a43">height</font> = <font color="#795da3">500</font>,<br/>
// MAGIC  &nbsp;&nbsp;<font color="#ed6a43">width</font> = <font color="#795da3">500</font>,<br/>
// MAGIC  &nbsp;&nbsp;<font color="#ed6a43">clicks</font>: <font color="#795da3">Dataset</font>[<font color="#795da3">Edge</font>])</tt>
// MAGIC  </p>""")
// MAGIC    }
// MAGIC  }

// COMMAND ----------

// MAGIC %scala
// MAGIC import d3._
// MAGIC 
// MAGIC // Como prueba quise ver los resultados de Stephen king
// MAGIC graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    prev_title IN ('Stephen_King_bibliography') AND
// MAGIC                                    prev_id IS NOT NULL
// MAGIC                              ORDER BY n DESC
// MAGIC                              """).as[Edge])

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // Consulta 1
// MAGIC // En primera instancia busqué 'Soldier_Field', quise buscar a Obama
// MAGIC // Pero no se encontraba en este set de datos
// MAGIC 
// MAGIC graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    prev_title IN ('Soldier_Field') AND
// MAGIC                                    prev_id IS NOT NULL
// MAGIC                              ORDER BY n DESC
// MAGIC                              """).as[Edge])

// COMMAND ----------

// MAGIC 
// MAGIC %scala
// MAGIC 
// MAGIC // Consulta 2
// MAGIC // En segunda instancia busqué 'Vladimir_Sedov'
// MAGIC 
// MAGIC graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    prev_title IN ('Vladimir_Sedov') AND
// MAGIC                                    prev_id IS NOT NULL
// MAGIC                              ORDER BY n DESC
// MAGIC                              """).as[Edge])
// MAGIC // No es muy usado al parecer

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // Consulta 3
// MAGIC // Finalmente busqué 'World_Club_Series'
// MAGIC 
// MAGIC graphs.force(height = 800,width = 1000,
// MAGIC               clicks = sql("""SELECT 
// MAGIC                                    prev_title AS src,
// MAGIC                                    curr_title AS dest,
// MAGIC                                    n AS count FROM clicks_table
// MAGIC                              WHERE 
// MAGIC                                    prev_title IN ('World_Club_Series') AND
// MAGIC                                    prev_id IS NOT NULL
// MAGIC                              ORDER BY n DESC
// MAGIC                              """).as[Edge])

# lineagraph

Macro Scala qui produit le graphe de lineage (table **et** colonne) de n'importe quel code Spark SQL /
DataFrame, exporté en [OpenLineage](https://openlineage.io) et en Mermaid.

```scala
import dev.lapoule.lineagraph.Lineage._

val revenue = lineage {
  val paid      = orders.filter(col("status") === "PAID")
  val customers = spark.table("customers")
  paid.join(customers, "customer_id")
    .groupBy("customer_id", "country")
    .agg(sum("amount").as("total_amount"))
}

revenue.df     // la DataFrame, inchangée
revenue.graph  // LineageGraph : datasets, opérations, lineage par colonne
OpenLineageExporter.write(revenue.graph, Paths.get("target/lineage"))
println(MermaidExporter.render(revenue.graph))

// Lineage des écritures (save, saveAsTable, insertInto…) faites dans un bloc
val (_, writes) = lineageWrites(spark) { revenue.df.write.parquet("out/revenue") }
```

## Architecture : macro + plan Spark

Une macro seule ne peut pas faire le travail : à la compilation elle ne voit que l'AST Scala, ni les
schémas, ni le contenu d'un `select("*")`, ni le SQL construit dynamiquement. Le plan logique **analysé**
de Spark (`df.queryExecution.analyzed`) sait tout cela, mais il a oublié le code. lineagraph combine
les deux :

| Compilation (`macros/LineageMacros.scala`) | Exécution (`core/PlanLineage.scala`) |
|---|---|
| Fichier, ligne, classe/méthode et `val` englobants → nom du job | Feuilles du plan → datasets d'entrée (fichiers, tables du catalogue, V2, vues sur données en mémoire) |
| Texte source du bloc | Lineage colonne par `ExprId`, de bas en haut : IDENTITY / TRANSFORMATION / AGGREGATION |
| Noms des `val` DataFrame (internes et externes au bloc) → labels des nœuds | Dépendances INDIRECT : clés de jointure, filtres, group by, tri, partition de fenêtre |
| Littéraux passés à `spark.sql(...)`, y compris via un `val` local | CTE, union, sous-requêtes scalaires, self-join, commandes d'écriture |

Le SQL passé à `spark.sql` et l'API DataFrame produisent le même plan, donc le même lineage
(c'est vérifié par les tests).

## Modules

- `core` : modèle neutre (`model/`), extraction depuis le plan (`PlanLineage`), capture des écritures
  (`WriteCapture`, via un `QueryExecutionListener`), exporters `OpenLineageExporter` et `MermaidExporter`.
- `macros` : `lineage { … }` et `lineageWrites(spark) { … }`. Il est séparé de `core` parce qu'une def
  macro Scala 2 doit être compilée avant le code qui l'utilise.
- `example` : job de jointure commandes × clients, écrit en DataFrame API puis en SQL.

## Lancer

```bash
sbt test                 # 15 tests, Spark 4.2.0 sur JDK 25
sbt example/run          # affiche le Mermaid et écrit target/lineage/*.json (+ .mmd)
```

La version de Spark est un paramètre du build :

```bash
sbt -DsparkVersion=4.2.0 test
# Spark 4.0.x / 4.1.x passent par Hadoop, qui appelle Subject.getSubject (non supporté à partir du JDK 24) :
sbt -DsparkVersion=4.1.3 -DjavaHome=$(/usr/libexec/java_home -v 23) test
```

| Spark | JDK de la JVM forkée | Tests |
|---|---|---|
| 4.2.0 (défaut) | 25 | 15/15 |
| 4.1.3 | 23 (`-DjavaHome`) | 15/15 |
| 4.0.4 | 23 (`-DjavaHome`) | 15/15 |

## Pourquoi OpenLineage

Voir [docs/BENCHMARK.md](docs/BENCHMARK.md) : comparaison d'OpenLineage, Spline, Apache Atlas, DataHub,
OpenMetadata et W3C PROV-O, et correspondance entre notre modèle et OpenLineage.

## Limites connues

- Scala 2.13 uniquement : les def macros n'existent pas en Scala 3, il faudrait une réécriture en `inline` / quotes.
- Les noms de `val` servent de labels quand un nœud du plan a le même résultat (`sameResult`) que la
  DataFrame tracée. Un `val` réécrit ensuite par l'optimiseur peut rester sans label.
- Les sous-types CONDITIONAL et le flag `masking` d'OpenLineage ne sont pas encore calculés.
- Streaming, Hive SerDe et sources custom non couvertes : elles apparaissent comme dataset
  `spark:<NomDuNœud>`.
- `WriteCapture` utilise une API `private[spark]` (vidage du listener bus) via `org.apache.spark.lineagraph`.

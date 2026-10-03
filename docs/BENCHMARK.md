# Standards de lineage : comparatif

Objectif : choisir le format d'export du graphe produit par `lineagraph` (lineage table **et** colonne
extrait d'un plan Spark). Doc comparative, pas un benchmark exécuté : les jugements viennent des
spécifications et de l'outillage publics, pas de mesures.

## Candidats

| | Nature | Gouvernance |
|---|---|---|
| **OpenLineage** | Spécification d'événements (JSON Schema) : `RunEvent` = run + job + datasets d'entrée/sortie, enrichis par des *facets* | LF AI & Data Foundation |
| **Spline** | Outil complet (agent Spark + serveur + UI) avec son propre modèle « execution plan / execution event » | Projet open source ABSA |
| **Apache Atlas** | Catalogue de gouvernance Hadoop, système de types (entités `DataSet` / `Process`) + hooks | Apache Software Foundation |
| **DataHub** | Plateforme de métadonnées ; modèle entités/aspects (PDL), ingestion par `MetadataChangeProposal` | Open source (LinkedIn → Acryl) |
| **OpenMetadata** | Plateforme de métadonnées ; API JSON Schema, entité `lineage` avec détails colonne | Open source (Collate) |
| **W3C PROV-O** | Ontologie générique de provenance (Entity / Activity / Agent), RDF / JSON-LD / PROV-N | Recommandation W3C |

## Critères

| Critère | OpenLineage | Spline | Atlas | DataHub | OpenMetadata | PROV-O |
|---|---|---|---|---|---|---|
| Lineage colonne | ✅ facet `columnLineage`, avec type de transformation (DIRECT/INDIRECT + sous-type) | ✅ niveau attribut, natif | ⚠️ `hive_column_lineage` ; côté Spark dépend du connecteur | ✅ `fineGrainedLineages` dans l'aspect `UpstreamLineage` | ✅ `columnsLineage` sur l'arête | ⚠️ possible en modélisant chaque colonne comme `Entity`, rien de standard |
| Typage de la transformation | ✅ IDENTITY / TRANSFORMATION / AGGREGATION, JOIN / FILTER / GROUP_BY / SORT / WINDOW… | ⚠️ expressions brutes du plan | ❌ | ⚠️ champ `transformOperation` libre | ⚠️ `function` libre | ❌ |
| Intégration Spark existante | ✅ `openlineage-spark` (SparkListener officiel) | ✅ agent Spark (raison d'être du projet) | ⚠️ `spark-atlas-connector`, plus maintenu | ⚠️ son agent Spark repose sur OpenLineage | ⚠️ ingestion OpenLineage | ❌ |
| Indépendant d'un backend | ✅ format seulement ; transports HTTP, Kafka, fichier, console… | ❌ couplé au serveur Spline | ❌ couplé à Atlas | ❌ couplé à DataHub | ❌ couplé à OpenMetadata | ✅ |
| Backends qui consomment le format | Marquez (référence), DataHub, OpenMetadata, Egeria, plusieurs catalogues cloud | Spline UI | Atlas UI | DataHub UI | OpenMetadata UI | Triple stores, outils PROV |
| Extensibilité | ✅ facets custom versionnées par schéma | ⚠️ champs `extra` | ✅ types custom | ✅ aspects custom | ⚠️ `extension` | ✅ (RDF) |
| Notion de run / exécution | ✅ `Run` (+ START/COMPLETE/FAIL) | ✅ execution event | ⚠️ entité `Process` sans run natif | ✅ `DataProcessInstance` | ⚠️ pipeline status | ✅ `Activity` |
| Verbosité / simplicité | Moyenne : un JSON par run | Moyenne | Élevée (types + relations) | Élevée (un MCP par aspect) | Moyenne | Élevée (triplets) |
| Validation | JSON Schema publié + client Java/Python officiels | Interne | Typedefs Atlas | Schémas PDL | JSON Schema | SHACL / OWL |

✅ natif · ⚠️ partiel ou indirect · ❌ absent

## Choix : OpenLineage

1. **Seul standard d'échange** du lot : les autres sont des modèles internes d'outils, et plusieurs
   (DataHub, OpenMetadata, Egeria) *ingèrent* déjà de l'OpenLineage. Exporter OpenLineage, c'est être
   lisible par tous sans s'enfermer dans un backend.
2. Le facet `columnLineage` correspond exactement à ce que le plan Spark permet de calculer : sources
   par colonne + nature de la transformation + dépendances indirectes (clés de jointure, filtres, group by).
3. Client Java officiel (`openlineage-java`) : construction typée de l'événement et validation par
   aller-retour JSON dans les tests.
4. Même vocabulaire que `openlineage-spark` : nos événements sont comparables à ceux du listener officiel.

Écartés : **Spline** (bon outil, mais format propriétaire couplé à son serveur), **Atlas** (lourd, Spark mal
supporté aujourd'hui), **PROV-O** (générique, aucun outillage data et pas de vocabulaire colonne),
**DataHub / OpenMetadata** (modèles de plateforme ; ils acceptent déjà OpenLineage en entrée).

Mermaid reste fourni à côté, pour la lecture humaine (README, PR, Notion), pas comme format d'échange.

## Correspondance modèle lineagraph → OpenLineage

| lineagraph | OpenLineage |
|---|---|
| `SourceInfo.jobName` (classe.méthode[.val], fourni par la macro) | `job.name` (namespace `lineagraph`) |
| `SourceInfo.code` / `file:line` | job facets `sourceCode`, `sourceCodeLocation` |
| `SourceInfo.sql` (littéraux SQL capturés) | job facet `sql` |
| `LineageGraph.inputs` (feuilles du plan) | `inputs[]` + facet `schema` |
| `LineageGraph.output` (DataFrame tracée, ou cible d'écriture) | `outputs[0]` + facets `schema`, `columnLineage` |
| `ColumnLineage` | `columnLineage.fields.<colonne>.inputFields[]` |
| `Transformation(kind, subtype)` | `inputFields[].transformations[]` (`type`, `subtype`) |
| `datasetDeps` (jointure, filtre, group by, tri, fenêtre) | `columnLineage.dataset[]` (INDIRECT) |
| `operations` / `edges` (graphe des opérateurs) | — pas d'équivalent standard ; exporté en Mermaid |

---
title: "SahlPay — Plateforme Big Data pour un portefeuille mobile"
subtitle: "Écosystème Big Data 2 — 5ème année EMSI — 2026-2027"
author: "Binôme : NOM Prénom — NOM Prénom"
---

> **À compléter avant export PDF** : remplacer les blocs `[CAPTURE …]` par les captures d'écran. Tous les chiffres viennent de l'exécution sur le cluster Docker du 01/11/2026 (sorties dans `outputs/`). Export : `pandoc rapport/rapport.md -o rapport.pdf`.

# 1. Architecture et positionnement (Partie 1)

**Déploiement.** `docker-compose.yml` démarre Kafka 3.7 (KRaft, topic `transactions` à 3 partitions créé par `kafka-init`), MinIO (bucket `lake` créé par le service `mc`), un Spark Master, deux Workers (4 cœurs / 3 Go chacun), un History Server et JupyterLab. Une seule image Spark 3.5.1 sert au Master, aux Workers et à Jupyter : le driver et les executors ont donc la même version de Spark, de Scala (2.12) et de Python (3.10), et les connecteurs Kafka, S3A, Delta et GraphFrames sont préinstallés dans `/opt/spark/jars`.

`[CAPTURE 1 — Spark Master UI : 2 Workers ALIVE]` `[CAPTURE 2 — console MinIO : bucket lake]`

Sortie de `part1_cluster.py` : version `3.5.1`, master `spark://spark-master:7077`, appName `SahlPay-Partie1-Cluster`, `defaultParallelism = 4` (`spark.cores.max = 4` par application, 2 executors de 2 cœurs), tâches exécutées sur `spark-worker-1` et `spark-worker-2`.

**Rôles dans notre déploiement.**

- **Driver** : le processus Python lancé par `spark-submit` dans le conteneur `jupyter`. Il exécute `main()`, construit le DAG, le découpe en stages (DAGScheduler), envoie les tâches (TaskScheduler) et reçoit les résultats des actions (`collect`, `show`). En streaming, c'est lui qui planifie chaque micro-batch et écrit le checkpoint.
- **Cluster Manager** : le Master standalone (`spark-master:7077`). Il connaît les Workers et leurs ressources et attribue à chaque application des executors dans la limite de `spark.cores.max`.
- **Workers / Executors** : chaque Worker lance des JVM executors (2 cœurs, 1 Go) qui exécutent les tâches, conservent les partitions en cache et écrivent les fichiers de shuffle. Pour le code PySpark (RDD, UDF), ils lancent aussi des workers Python.
- **SparkContext** : point d'entrée créé par le driver (encapsulé dans la `SparkSession`). Il établit la connexion au Master, crée les RDD et les variables broadcast, et porte la configuration (S3A, Delta, event log).

**Deux limites de MapReduce corrigées par Spark.**

1. *Matérialisation sur disque entre chaque étape.* Une chaîne « dédoublonner → filtrer → joindre → agréger » coûterait plusieurs jobs MapReduce, chacun écrivant son résultat dans HDFS. Les algorithmes de graphe itératifs (PageRank, composantes connexes) relisent le graphe à chaque itération. Spark enchaîne ces étapes dans un même DAG, garde les données intermédiaires en mémoire (`cache`) et n'écrit sur disque qu'aux shuffles.
2. *Traitement batch uniquement, avec une API bas niveau.* SahlPay veut des indicateurs presque en temps réel depuis Kafka. MapReduce n'offre pas de streaming, il faudrait ajouter un autre moteur (Storm…). Spark propose une seule API (DataFrame/SQL) pour le batch (Parties 2-3), le streaming (Parties 4-5) et les graphes (Partie 6), avec l'optimiseur Catalyst.

# 2. Traitement batch avec les RDD (Partie 2)

`producer.py --mode file --n 50000` produit 50 000 événements (16 517 transactions, environ 2 % de doublons, environ 5 % d'arrivées en retard), copiés dans `lake/bronze/history/`. Les transactions `COMPLETED` sont dédoublonnées par `tx_id` (`reduceByKey`), ce qui donne 15 317 transactions.

| Ville | Montant total (MAD) | Nb tx | Montant moyen (MAD) |
|---|---:|---:|---:|
| Casablanca | 1 308 314,11 | 4 295 | 304,61 |
| Marrakech | 656 756,94 | 2 027 | 324,00 |
| Rabat | 595 785,97 | 2 238 | 266,21 |
| Tanger | 468 740,75 | 1 482 | 316,29 |
| Fes | 408 946,62 | 1 512 | 270,47 |
| Agadir | 357 804,94 | 1 193 | 299,92 |
| Meknes | 254 665,38 | 774 | 329,03 |
| Oujda | 230 431,36 | 712 | 323,64 |
| Kenitra | 187 611,58 | 644 | 291,32 |
| Tetouan | 112 682,75 | 440 | 256,10 |

Top 5 des commerçants (paiements validés) : M-229 (71), M-088 (70), M-122 (70), M-027 (69), M-038 (68). La moyenne est calculée en un seul passage avec `aggregateByKey((0.0, 0), seqOp, combOp)`, qui produit le couple (somme, nombre) par ville.

**Transformations ou actions.** Les transformations (lazy) sont `textFile`, `map(json.loads)`, `filter`, `reduceByKey`, `values`, `cache`, `aggregateByKey` et `mapValues`. Les actions, qui déclenchent chacune un job, sont `count`, `collect` et `takeOrdered`. `toDebugString()` montre deux `ShuffledRDD`, l'un pour le dédoublonnage et l'autre pour l'agrégation, ainsi que les `CachedPartitions` du RDD mis en cache.

`[CAPTURE 3 — DAG Spark UI du job « montant moyen par ville » + onglet Stages]`

**Stages et shuffle.** Le job de la moyenne comporte 3 stages : (1) lecture, parsing, filtre et pré-agrégation pour le dédoublonnage ; (2) lecture du shuffle, `map` et pré-agrégation par ville ; (3) fusion finale et `collect`. Les jobs suivants marquent le stage 1 comme *skipped* grâce au cache. `reduceByKey` combine d'abord localement dans chaque partition (combiner), puis redistribue les paires selon le hash de la clé : les map tasks écrivent des fichiers de shuffle sur disque et les reduce tasks les lisent via le réseau. Ce shuffle est une dépendance large, donc une frontière de stage.

**Tolérance aux pannes.** Un RDD ne stocke pas ses données mais leur recette (lineage). Si un executor disparaît, Spark recalcule seulement les partitions perdues en rejouant les transformations depuis la source ou depuis les fichiers de shuffle encore disponibles. Avec une dépendance étroite, une partition perdue n'exige de recalculer qu'une partition parente. Pour un lineage très long, comme dans les algorithmes itératifs, `checkpoint()` le tronque.

# 3. Spark SQL et DataFrames (Partie 3)

Les deux sources sont lues avec un `StructType` explicite (sans inférence, donc sans passe de lecture supplémentaire) et `event_time` est directement typé `timestamp`. Le calcul par ville donne les mêmes résultats en API DataFrame et en SQL (`createOrReplaceTempView` + `spark.sql`). La jointure P2M × merchants fournit le montant encaissé par catégorie et par ville ; 25 paiements visent un commerçant absent du référentiel (`M-9xx`, injectés volontairement) et sont écartés par la jointure interne.

**UDF ou `when/otherwise`** (2 M de montants, cluster) : UDF Python **1,02 s**, expression native **0,22 s**, soit environ 5 fois plus rapide. Le plan de l'UDF contient `BatchEvalPython` : chaque ligne est sérialisée vers un processus Python puis renvoyée, et Catalyst ne peut ni l'analyser ni la fusionner. `CASE WHEN` est une expression Catalyst compilée en bytecode par le *whole-stage codegen* (`*(1) Project [CASE WHEN …]`), sans quitter la JVM.

**Silver.** Les données sont dédoublonnées sur `(tx_id, status)`, débarrassées des lignes invalides, enrichies (`merchant_name`, `merchant_category`, `amount_band`, `date`) puis écrites en Parquet dans `lake/silver/transactions_history/city=…/` (48 987 lignes).

`[CAPTURE 4 — mc ls : partitions city= de silver/transactions_history]`

**`explain(True)`** (texte complet dans `outputs/part3_explain_join.txt`) :

- *Parsed / Analyzed Logical Plan* : `Join Inner (payee_id = merchant_id)` au-dessus de `Filter(type = P2M)`, `Deduplicate [tx_id]`, `Filter(status = COMPLETED)` et des deux `Relation json`. L'analyse résout les colonnes et les types.
- *Optimized Logical Plan* (Catalyst) : les filtres sont combinés et poussés vers la source (`isnotnull(type) AND type = P2M AND isnotnull(payee_id)`), un filtre `isnotnull(merchant_id)` est ajouté côté référentiel, et les colonnes inutiles sont élaguées (`ReadSchema` réduit).
- *Physical Plan* : **`BroadcastHashJoin … BuildRight`**. Le référentiel (300 lignes) est sous le seuil `autoBroadcastJoinThreshold` de 10 Mo, il est donc diffusé à chaque executor et la table des transactions n'est pas shufflée. Avec le seuil à -1, le même plan devient **`SortMergeJoin`** avec deux `Exchange hashpartitioning` et deux `Sort`. Le plan est encapsulé dans `AdaptiveSparkPlan` (AQE).

`[CAPTURE 5 — sortie explain(True) + DAG Spark UI de la jointure]`

**RDD, DataFrame ou Dataset.**

| | RDD (Python) | DataFrame | Dataset (Scala/Java) |
|---|---|---|---|
| Typage | objets Python, aucun schéma | schéma vérifié à l'exécution (`Row`) | typé à la compilation (`case class`) |
| Optimisation | aucune : le code utilisateur est opaque | Catalyst (pushdown, pruning, choix de jointure) + Tungsten (format binaire, codegen) | Catalyst + Tungsten, avec des lambdas opaques |
| Coût en PySpark | chaque enregistrement passe par un worker Python (pickle) | exécution entièrement dans la JVM | non disponible en Python |

*Temps mesurés sur le cluster* :

| Opération | RDD | DataFrame | SQL |
|---|---:|---:|---:|
| Comptage des événements (lecture S3A) | 4,45 s | 4,35 s | — |
| Filtre COMPLETED + dédoublonnage | 2,50 s | 3,47 s | — |
| Montant total par ville | 0,34 s | 1,96 s | 0,42 s |

Sur 50 000 lignes, ces temps ne départagent pas les API. La lecture S3A et la planification des jobs dominent. Le RDD agrège un RDD déjà en cache, et la première requête DataFrame inclut la compilation Catalyst, que la requête SQL suivante réutilise (0,42 s pour le même plan physique). L'avantage de l'API structurée apparaît dès que le calcul domine : la même logique est 5 fois plus rapide en expression native qu'en code Python sur 2 M de lignes.

# 4. Structured Streaming depuis Kafka (Partie 4)

`part4_streaming.py` lit `transactions` (`readStream.format("kafka")`), parse `value` avec `from_json` et le schéma de la Partie 3, puis démarre cinq requêtes : flux typé vers la console (`append`), flux brut vers `lake/bronze/transactions_raw/` en JSON (avec clé, partition, offset et timestamp Kafka, partitionné par `ingest_date`, checkpoint dédié), comptage par ville et statut vers la console (`complete`), fenêtres de 5 min avec un watermark de 10 min (`update`), et un détecteur des événements très en retard.

`[CAPTURE 6 — console : plusieurs micro-batches successifs (raw_console et city_status)]` `[CAPTURE 7 — mc ls : bronze/transactions_raw/ingest_date=…/*.json]`

**Modèle source → transformation → sink.** Le topic est vu comme une *table infinie* à laquelle chaque message ajoute une ligne. La requête s'écrit comme une requête batch sur cette table. À chaque déclenchement, Spark calcule de façon incrémentale le résultat sur les nouvelles lignes, en conservant un état pour les agrégations, et l'écrit dans le sink.

**Déclencheurs.** `trigger(processingTime="10 seconds")` fonctionne en micro-batch : un job toutes les 10 s, une latence de l'ordre de la seconde, toutes les opérations sont supportées et la garantie est exactly-once avec un sink adapté. `trigger(continuous="1 second")` maintient des tâches longues qui traitent les lignes une par une (latence d'environ 1 ms, checkpoint asynchrone toutes les secondes). Il ne supporte que les projections et filtres (pas d'agrégation ni de dédoublonnage) et n'offre que l'at-least-once, ce qui ne convient pas à nos KPI.

**Modes de sortie.** `append` n'émet que les nouvelles lignes définitives (pour une agrégation, une fenêtre n'est émise qu'une fois close par le watermark). `update` n'émet que les lignes modifiées depuis le dernier batch. `complete` réémet toute la table résultat à chaque batch, ce qui n'est possible que pour les agrégations dont l'état reste borné, comme le comptage par ville et statut.

**Watermark et événement en retard de 15 min.** Watermark = max(`event_time` observé) − 10 min. Sur 18 minutes d'exécution (327 micro-batches en console), 123 événements sont arrivés avec plus de 10 min de retard. Exemple tiré des logs (`outputs/part4_late_events.log`, `outputs/part4_console.txt`) :

```
[PROGRESS watermark] batch=84 lignes=224 max_event_time=2026-11-01T15:13:39.000Z
                     watermark=2026-11-01T15:03:29.000Z lignes ignorees (retard > watermark)=1
[batch 84] TX-50000068 INITIATED Casablanca event_time=2026-11-01 14:59:21 arrivee=2026-11-01 15:13:35
           retard=14.2 min | watermark=2026-11-01 15:03:19 < watermark -> IGNORE par l'agregation fenetree
```

L'événement daté de 14:59:21 arrive à 15:13:35, alors que le watermark vaut déjà 15:03:19. Sa fenêtre [14:55, 15:00) est considérée comme close : son état a été purgé et le résultat déjà émis. L'événement est donc **ignoré par l'agrégation fenêtrée** et compté dans `numRowsDroppedByWatermark`. Il n'est en revanche **pas perdu** : il figure dans la couche Bronze et dans le sink console `append`, qui n'ont pas d'état. Un événement en retard de moins de 10 min aurait mis à jour sa fenêtre normalement. Le watermark est un compromis entre exactitude et mémoire : 10 min couvrent la majorité des retards sans conserver un état illimité.

# 5. Pipeline de production Spark–Kafka (Partie 5)

```
Kafka → from_json → withWatermark(event_time, 10 min) → dropDuplicatesWithinWatermark([tx_id, status])
      ├─ jointure stream-static merchants + amount_band  → silver/transactions_clean   (Parquet, city/date, append)
      ├─ window 10 min (tumbling) × city                  → gold/kpi_city_10min          (Delta, MERGE)
      ├─ window 10 min / 5 min (sliding) × city           → gold/kpi_city_reject_rate_sliding (Delta, MERGE)
      └─ session_window 5 min × device_id                 → gold/device_sessions         (Delta, append)
```

- **Dédoublonnage.** `dropDuplicatesWithinWatermark(["tx_id","status"])` (Spark 3.5) est la forme de `dropDuplicates` combinée au watermark : la clé reste `(tx_id, status)` et chaque clé est oubliée après le passage du watermark. Un `dropDuplicates` sans `event_time` dans la clé conserverait toutes les clés indéfiniment (état non borné). Sur le cluster, 404 doublons ont été envoyés volontairement dans Kafka ; aucun ne se retrouve dans Silver (voir le tableau de reprise plus bas).
- **Définition des KPI.** Une transaction compte dans `nb_transactions` lorsqu'elle atteint un état final (`COMPLETED` ou `REJECTED`). Le montant ne compte que les `COMPLETED`. Le taux de rejet vaut `REJECTED / (COMPLETED + REJECTED)` sur chaque fenêtre glissante. Pour les sessions, `nb_transactions` est le nombre d'`INITIATED` et `duree_activite_s` l'écart entre le premier et le dernier événement de la session.
- **Gold en Delta.** Les fenêtres tumbling et glissantes sont émises en mode `update` et fusionnées par `foreachBatch` avec un `MERGE` sur `(city, window_start)` : le tableau de bord voit les fenêtres en cours sans attendre 20 min. Les sessions, dont les bornes peuvent encore changer, sont écrites en `append`, une fois closes.

`[CAPTURE 8 — table Delta gold relue en batch (part5_verify.py §1)]` `[CAPTURE 9 — DESCRIBE HISTORY : versions WRITE puis MERGE par micro-batch]`

**Offsets et consumer groups.** Le checkpoint `checkpoints/silver_transactions_clean/` contient `offsets/<n>`, journal d'écriture anticipée écrit **avant** le batch n : version, métadonnées (`batchWatermarkMs`…) et `{"transactions":{"0":…,"1":…,"2":…}}`. Il contient aussi `commits/<n>`, écrit **après** l'écriture du sink, ainsi que `state/` pour le dédoublonnage et les agrégations. Au redémarrage, Spark relit le dernier offset validé et reprend au batch suivant ; un batch planifié mais non validé est rejoué avec exactement les mêmes offsets.

Spark lit les partitions avec `assign()` (consumers préfixés `sahlpay-pipeline-…` par `groupIdPrefix`) et **ne commite jamais ses offsets dans Kafka** : la seule source de vérité est le checkpoint. Pour la supervision, un `StreamingQueryListener` recopie après chaque batch les offsets de fin de la requête Silver dans le groupe `sahlpay-pipeline-silver`. `kafka-consumer-groups.sh --describe --group sahlpay-pipeline-silver` affiche alors `CURRENT-OFFSET`, identique au fichier `offsets/<n>` du checkpoint, ainsi que `LOG-END-OFFSET` et `LAG`. Mesuré à l'arrêt : `CURRENT-OFFSET` = 4192 / 4299 / 4171, exactement les offsets de `checkpoints/silver_transactions_clean/offsets/31`, avec un LAG de 470 / 460 / 490 qui augmente pendant l'arrêt. Au redémarrage, le premier batch (n° 32) repart de ces offsets. Le pipeline rattrape alors son retard à environ 170 lignes/s, contre environ 20 en régime normal, puis termine au batch 47 (`CURRENT-OFFSET` = 7359 / 7388 / 7132, LAG résiduel d'environ 250 par partition, correspondant au trafic des derniers secondes avant l'arrêt). Ce groupe n'est jamais relu par le pipeline : supprimer le groupe ne change rien à la reprise.

`[CAPTURE 10 — mc cat …/offsets/<n> avant arrêt et après reprise]` `[CAPTURE 11 — kafka-consumer-groups.sh --describe]`

| Vérification (`outputs/part5_counts.json`) | avant arrêt | après reprise (2 min de production en plus) |
|---|---:|---:|
| Batch du checkpoint | 31 | 47 |
| Lignes Silver | 12 429 | 21 386 |
| Doublons Silver | 0 | 0 |
| Messages Kafka jusqu'aux offsets du checkpoint | 12 662 (12 429 couples distincts) | 21 879 (21 475 couples distincts) |
| Absents de Silver (tous arrivés avec > 10 min de retard, écartés par le watermark) | 0 | 89 |
| Pertes non expliquées / lignes Silver absentes de Kafka | 0 / 0 | 0 / 0 |

La table Delta Gold `kpi_city_10min` contient 47 versions (création puis un `MERGE` par micro-batch), visibles dans `DESCRIBE HISTORY`. La table des fenêtres glissantes contient 60 lignes et celle des sessions 782.

**Exactly-once.** (1) La **source est rejouable** : Kafka conserve les messages et Spark peut relire n'importe quelle plage d'offsets. (2) Le **checkpoint** fixe la plage de chaque batch avant son exécution (`offsets/`) et sa validation après (`commits/`), avec l'état associé. Un batch interrompu est donc rejoué à l'identique. (3) Les **sinks** sont idempotents ou transactionnels. Le sink fichier Parquet consigne les fichiers de chaque batch dans `_spark_metadata/` et un lecteur Spark ignore les fichiers d'un batch non validé. Delta enregistre chaque écriture comme une transaction atomique dans `_delta_log`, et le `MERGE` par clé réécrit les mêmes valeurs si un batch est rejoué. Le **sink console** n'a ni journal ni transaction : un batch rejoué est réaffiché, et ce qui a été imprimé avant une panne ne peut pas être annulé. Il n'offre donc qu'un affichage at-least-once, réservé au débogage.

# 6. Analyse de graphes avec GraphFrames et GraphX (Partie 6)

**Modèle du graphe** (construit depuis Silver et exporté dans `lake/gold/graph_model/`) :

| Sommets (`id`, `type`, `label`, `city`, `category`) | Arêtes (`src`, `dst`, `relationship`, `tx_count`, `total_amount`) |
|---|---|
| `wallet` (`W-…`), `device` (`D-…`), `merchant` (`M-…`) | `USES_DEVICE` wallet→device (toutes tentatives), `PAYS` wallet→merchant (P2M COMPLETED), `TRANSFERS_TO` wallet→wallet (P2P COMPLETED) |

Les préfixes rendent les identifiants uniques et stables d'un type à l'autre. Les arêtes sont agrégées par `(src, dst, relationship)`, et les arêtes orphelines (extrémité absente des sommets) sont exclues par deux semi-jointures, puis contrôlées dans le GraphFrame (0 orpheline restante).

Graphe construit depuis `silver/transactions_clean` (flux Kafka de la Partie 5, 21 386 événements) : 8 475 sommets (4 242 wallets, 3 933 devices, 300 merchants), 10 530 arêtes (3 987 `USES_DEVICE`, 4 541 `PAYS`, 2 002 `TRANSFERS_TO`) ; 6 arêtes `PAYS` orphelines exclues (commerçants `M-9xx` absents du référentiel).

`[CAPTURE 12 — schéma du graphe + comptages par type]`

**Motifs GraphFrames (`find`).**

- `(a)-[]->(d); (b)-[]->(d)` avec `a.id < b.id` sur le sous-graphe `USES_DEVICE` : D-4a3670 est utilisé par 25 comptes (300 paires) et D-ef0b67 par 12. Les 19 autres appareils partagés ne le sont que par 2 comptes (appareils familiaux).
- `(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)` avec `a.id < b.id AND a.id < c.id`, ce qui garde une seule rotation par cycle : 9 cycles au total, dont 4 avec chaque arête ≥ 8 000 MAD (les anneaux injectés, de 8 196 à 17 412 MAD) et 5 cycles « naturels » entre proches, de petits montants.
- `(a)-[e1]->(b); (b)-[e2]->(m)` (`TRANSFERS_TO` puis `PAYS`) : 1 946 chaînes compte → compte → commerçant, dont 3 passent par un compte collecteur suivi d'un paiement e-commerce d'environ 1 100 MAD (sortie des fonds).
- Degrés : le maximum d'`inDegree` naturel vaut 4, contre 48, 45 et 44 pour les trois comptes collecteurs (montant moyen reçu de 27,6 à 35,4 MAD).

`[CAPTURE 13 — résultat du motif appareil partagé ou cycle]`

**GraphX (Scala).** `GraphXAnalysis.scala` relit les Parquet et attribue à chaque sommet un `VertexId` Long : le rang de l'identifiant texte trié (`orderBy("id").rdd.zipWithIndex`), unique et reproductible, sauvegardé dans `graph_model/vertex_ids`. Il construit `Graph(verticesRDD, edgesRDD)` avec des attributs `EdgeAttr(relationship, txCount, totalAmount)`, puis le sous-graphe des transferts par `subgraph`. Il calcule ensuite `degrees` / `inDegrees` / `outDegrees`, `connectedComponents` (762 composantes, la plus grande de 366 comptes, toutes les autres de 2 à 14 comptes) et `pageRank(0.0001)`.

`[CAPTURE 14 — top pageRank et tailles de composantes GraphX]`

**Comparaison** (`part6_compare.py`, même sous-graphe `TRANSFERS_TO`) : 2 716 comptes, **0 différence** de degrés, in-degrés et out-degrés entre les deux bibliothèques, et des tailles de composantes identiques.

| Opération (cluster) | GraphFrames | GraphX |
|---|---:|---:|
| Construction du graphe | 108,3 s (lecture de la couche Silver + agrégation + export) | 0,9 s (+ 5,4 s d'attribution des VertexId) |
| Degrés | 0,79 s | 1,45 s |
| Composantes connexes | 9,1 s | 1,7 s |
| PageRank | — | 6,7 s |

La construction côté GraphFrames est dominée par la lecture de la couche Silver issue du streaming : des centaines de petits fichiers Parquet (un par partition `city/date` et par micro-batch), qu'il faudrait compacter en production (`OPTIMIZE` Delta ou réécriture périodique). GraphX part de l'export Parquet déjà agrégé. Sur l'historique batch de 50 000 événements, les composantes connexes ont pris 150 s avec GraphFrames contre 2,2 s avec GraphX (exécution de contrôle en local).

GraphFrames repose sur des DataFrames : il s'utilise depuis Python, bénéficie de Catalyst et offre `find()` pour les motifs (inexistant en GraphX). En revanche, ses algorithmes itératifs enchaînent des jointures de DataFrames : les composantes connexes ont exigé de désactiver AQE, dont la journalisation d'un plan qui grossit à chaque itération saturait le driver, et sont beaucoup plus lentes. GraphX (RDD, Scala uniquement) impose de gérer des identifiants Long et des attributs typés, mais son moteur Pregel est nettement plus rapide pour CC et PageRank.

**Centralité.** Les 3 premiers comptes au PageRank sont les collecteurs (W-12339, W-14969, W-10791). Le 4e (W-11848) n'a qu'**un** virement entrant, mais il provient du collecteur W-14969, ce qui lui transmet beaucoup de score. De même, 11 des 15 comptes du top PageRank ne déclenchent aucune règle. La centralité décrit donc une **position structurelle** : elle sert à prioriser une revue humaine, pas à conclure à une fraude.

**Anomalies.** Les seuils sont explicites et paramétrables. La liste motivée est écrite dans `lake/gold/graph_analysis/anomaly_type=…/`.

| Type | Règle | Cas (cluster) |
|---|---|---|
| `shared_device` | ≥ 5 comptes distincts sur un appareil | 2 |
| `transfer_cycle` | cycle de 3 comptes dont chaque arête ≥ 1 000 MAD | 4 |
| `collector_account` | ≥ 20 émetteurs distincts et montant moyen reçu ≤ 100 MAD | 3 |
| `collector_cash_out` | collecteur → paiement commerçant ≥ 500 MAD | 3 |
| `large_component` | composante de transferts ≥ 100 comptes | 1 (366 comptes) |

**Vérification séparée** contre `data/fraud_ground_truth.json` (scénarios injectés par `producer.py`) : appareils partagés 2/2, cycles 4/4, collecteurs 3/3, et **0 faux positif** avec ces seuils. Le même résultat est obtenu sur l'historique batch de 50 000 événements. La sensibilité aux seuils est réelle : sans seuil de montant, les 5 cycles naturels seraient des faux positifs (50 sur l'historique). À 2 comptes par appareil, les 19 appareils familiaux le seraient aussi. La grande composante est un signal peu spécifique : les collecteurs, en recevant de 44 à 48 comptes de cercles sociaux différents, fusionnent de nombreuses petites communautés, si bien que 366 comptes majoritairement légitimes s'y trouvent.

`[CAPTURE 15 — outputs/part6_subgraph_suspect.png]` `[CAPTURE 16 — mc ls gold/graph_analysis/ + extrait de la liste des cas suspects]`

Métriques pour le tableau de bord (`lake/gold/graph_metrics`, `outputs/part6_metrics.json`, `outputs/dashboard_conformite.html`) : sommets et arêtes par type, nombre d'anomalies par type, nombre maximal de comptes par appareil, in-degré maximal, taille de la plus grande composante, rappel et faux positifs sur les scénarios.

**Limites.** Les données sont synthétiques et les seuils ont été choisis en connaissant les scénarios : il faudrait les recalibrer sur des données réelles étiquetées. Les arêtes agrégées perdent la chronologie, si bien qu'un « cycle » n'est pas forcément un aller-retour d'argent dans le temps. Seuls les cycles de longueur 3 sont recherchés. Le graphe est recalculé en batch, il n'est pas mis à jour en continu. Enfin, l'analyse signale des structures à examiner, pas des fraudes avérées.

**Versions et lancement.** Spark 3.5.1 / Scala 2.12.18 / Java 17 ; GraphFrames `0.8.3-spark3.5-s_2.12` (jar dans `/opt/spark/jars`, module Python lu directement depuis le jar via `PYTHONPATH`, ou `--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 --repositories https://repos.spark-packages.org`). Lancement de GraphX : `docker compose exec jupyter spark-shell -i graphx/GraphXAnalysis.scala`.

# 7. Conclusion

La preuve de concept couvre toute la chaîne : collecte (Kafka), traitement distribué batch et streaming (Spark), stockage en couches Bronze/Silver/Gold sur MinIO (JSON, Parquet, Delta), analyse de graphes (GraphFrames et GraphX) et restitution (tableau de bord conformité), sans solliciter la base PostgreSQL transactionnelle. Débit, latence par micro-batch, temps de requêtes et reprise après panne sont mesurés par les scripts (`outputs/`).

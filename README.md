# SahlPay — Plateforme Big Data pour portefeuille mobile

Plateforme d'analyse quasi temps réel des transactions d'un portefeuille mobile (paiements marchands par QR code et transferts entre particuliers) : ingestion Kafka, traitements distribués Apache Spark (batch et streaming), lac de données Bronze / Silver / Gold sur stockage objet S3, et analyse de graphes avec GraphFrames et GraphX pour la détection de structures inhabituelles.

Projet réalisé dans le cadre du module *Écosystème Big Data 2* (5ème année, EMSI, 2026-2027). SahlPay est une entreprise fictive.

---

## Sommaire

1. [Présentation](#présentation)
2. [Architecture](#architecture)
3. [Stack technique](#stack-technique)
4. [Structure du dépôt](#structure-du-dépôt)
5. [Installation](#installation)
6. [Exécution](#exécution)
7. [Démonstration : suivi d'un paiement](#démonstration--suivi-dun-paiement)
8. [Résultats](#résultats)
9. [Choix techniques](#choix-techniques)
10. [Limites](#limites)
11. [Auteur](#auteur)

---

## Présentation

L'application de paiement SahlPay (API Spring Boot + PostgreSQL) émet un événement à chaque changement d'état d'une transaction : `INITIATED`, `AUTHORIZED`, `COMPLETED`, `REJECTED`, `REVERSED`. La plateforme exploite ce flux sans solliciter la base transactionnelle pour :

- calculer des indicateurs par ville et par catégorie de commerçant (volumes, montants, taux de rejet) sur des fenêtres temporelles ;
- garantir des résultats fiables malgré les doublons, les événements en retard et les redémarrages (traitement exactly-once) ;
- modéliser les relations comptes / appareils / bénéficiaires sous forme de graphe et repérer les structures atypiques : appareils partagés, transferts circulaires, comptes collecteurs.

Les structures détectées sont des signaux à examiner par une équipe conformité, et non des conclusions automatiques de fraude.

## Architecture

```
                 +-------------+      +-----------------------+      +-----------------------------+
 producer.py --> |    Kafka    | ---> |   Spark Structured    | ---> |      MinIO (bucket lake)    |
 (simulateur)    | transactions|      |      Streaming        |      |                             |
                 | 3 partitions|      |  Master + 2 Workers   |      |  bronze/  JSON brut         |
                 +-------------+      +-----------------------+      |  silver/  Parquet nettoyé   |
                                                 |                   |  gold/    Delta Lake (KPI)  |
                                                 v                   |  checkpoints/               |
                                      +-----------------------+      +-----------------------------+
                                      | GraphFrames (PySpark) |                    |
                                      | GraphX (Scala)        | -------------------+
                                      +-----------------------+                    v
                                                                       Tableau de bord conformité
```

| Couche | Emplacement | Format | Contenu |
|---|---|---|---|
| Bronze | `lake/bronze/transactions_raw/` | JSON | Messages Kafka bruts avec partition, offset et horodatage |
| Bronze | `lake/bronze/history/` | JSON Lines | Historique de 50 000 événements pour le batch |
| Silver | `lake/silver/transactions_clean/` | Parquet (`city`, `date`) | Flux dédoublonné et enrichi avec le référentiel commerçants |
| Silver | `lake/silver/transactions_history/` | Parquet (`city`) | Historique nettoyé et enrichi |
| Gold | `lake/gold/kpi_city_10min/` | Delta Lake | KPI par ville et fenêtre tumbling de 10 minutes |
| Gold | `lake/gold/kpi_city_reject_rate_sliding/` | Delta Lake | Taux de rejet sur fenêtres glissantes (10 min / 5 min) |
| Gold | `lake/gold/device_sessions/` | Delta Lake | Sessions d'activité par appareil (inactivité de 5 min) |
| Gold | `lake/gold/graph_analysis/` | Parquet (`anomaly_type`) | Structures inhabituelles motivées |

## Stack technique

| Composant | Version | Rôle |
|---|---|---|
| Apache Spark | 3.5.1 (Scala 2.12, Java 17, Python 3.10) | Calcul distribué : RDD, SQL, Structured Streaming, GraphX |
| Apache Kafka | 3.7.0, mode KRaft | Bus d'événements |
| MinIO | `pgsty/minio` RELEASE.2026-08-04 | Stockage objet compatible S3 |
| Delta Lake | 3.2.0 | Tables Gold transactionnelles |
| GraphFrames | 0.8.3-spark3.5-s_2.12 | Motifs et algorithmes de graphe sur DataFrames |
| Connecteurs | `spark-sql-kafka-0-10` 3.5.1, `hadoop-aws` 3.3.4 | Accès Kafka et S3A |
| JupyterLab | 4.2.5 | Environnement interactif PySpark |
| Docker Compose | v2 ou plus | Orchestration de l'environnement |

Une image Docker unique (`docker/spark/Dockerfile`) est partagée par le Master, les Workers, le History Server et Jupyter. Le driver et les executors utilisent ainsi exactement les mêmes versions de Spark, Scala et Python. Tous les connecteurs sont intégrés à l'image : aucun `--packages` n'est nécessaire à l'exécution.

Les images officielles `minio/minio` n'étant plus publiées, la plateforme utilise le fork communautaire maintenu par Pigsty (console « SILO »), compatible avec l'API S3 et les commandes `mc`. Une autre image peut être choisie avec les variables `MINIO_IMAGE` et `MC_IMAGE`.

## Structure du dépôt

```
docker-compose.yml               Kafka, MinIO + client mc, Spark Master, 2 Workers, History Server, Jupyter
docker/spark/                    Dockerfile, spark-defaults.conf, dépendances Python
producer/
  generate_merchants.py          Référentiel statique des 300 commerçants
  producer.py                    Générateur de transactions (modes file et kafka)
  send_test_payment.py           Envoi d'un paiement de test identifiable
data/
  merchants.json                 Référentiel commerçants (JSON Lines)
  fraud_ground_truth.json        Vérité terrain des scénarios injectés
src/
  common.py                      Session Spark, chemins du lac, schémas, enrichissement
  part1_cluster.py               Partie 1 : cluster et SparkSession
  part2_rdd.py                   Partie 2 : traitement batch avec les RDD
  part3_sql.py                   Partie 3 : DataFrames, SQL, UDF, Parquet, plans Catalyst
  part4_streaming.py             Partie 4 : Structured Streaming, Bronze, watermark
  part5_pipeline.py              Partie 5 : pipeline Silver / Gold, fenêtres, Delta Lake
  part5_verify.py                Partie 5 : relecture Gold, historique Delta, preuve de reprise
  part6_graphframes.py           Partie 6 : graphe, motifs, degrés, anomalies, visualisation
  part6_compare.py               Partie 6 : comparaison GraphFrames / GraphX
  track_payment.py               Suivi d'un paiement à travers toutes les couches
  dashboard.py                   Génération du tableau de bord conformité
graphx/GraphXAnalysis.scala      Partie 6 : degrees, connectedComponents, pageRank (GraphX)
notebooks/partie1_cluster.ipynb  Partie 1 en notebook
rapport/rapport.md               Rapport du projet
```

## Installation

### Prérequis

- Docker Desktop (Windows / macOS) ou Docker Engine avec le plugin Compose (Linux), avec au moins 8 Go de mémoire allouée.
- Ports libres : 7077, 8080-8082, 8888, 9000-9001, 9094, 18080, 4040-4045.

### Démarrage

```bash
git clone https://github.com/AtmaniAnas1/SahlPay.git
cd SahlPay
docker compose up -d --build
docker compose ps
```

La première construction de l'image prend quelques minutes. Au démarrage, le service `kafka-init` crée le topic `transactions` (3 partitions) puis s'arrête ; le service `mc` crée le bucket `lake` et y dépose le référentiel commerçants.

### Interfaces

| Interface | Adresse | Identifiants |
|---|---|---|
| Spark Master | http://localhost:8080 | — |
| Spark UI (application en cours) | http://localhost:4040 | — |
| Spark History Server | http://localhost:18080 | — |
| Console MinIO | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| JupyterLab | http://localhost:8888 | — |

Les identifiants et l'absence d'authentification sur Jupyter conviennent à un environnement local de démonstration uniquement.

## Exécution

Toutes les commandes Spark s'exécutent dans le conteneur `jupyter`, dont le répertoire de travail `/opt/project` est le dépôt monté. Le master `spark://spark-master:7077` et la configuration S3A / Delta proviennent de `spark-defaults.conf`.

### Partie 1 — Cluster

```bash
docker compose exec jupyter spark-submit src/part1_cluster.py
```

Affiche la version de Spark, le master, l'appName, `defaultParallelism`, les hôtes des executors, et vérifie l'accès au bucket et au topic.

### Partie 2 — Traitement batch avec les RDD

```bash
docker compose exec jupyter python3 producer/producer.py --mode file --n 50000
docker compose exec mc mc cp /data/transactions_history.jsonl local/lake/bronze/history/
docker compose exec jupyter spark-submit src/part2_rdd.py --hold 600
```

Montant total et moyen par ville (`reduceByKey`, `aggregateByKey`), top 5 des commerçants (`takeOrdered`) et lineage (`toDebugString`). L'option `--hold` maintient l'application active pour consulter le DAG dans la Spark UI.

### Partie 3 — Spark SQL et DataFrames

```bash
docker compose exec jupyter spark-submit src/part3_sql.py --hold 600
```

Lecture avec schéma explicite, calculs en API DataFrame et en SQL, jointure avec le référentiel, comparaison UDF Python / `when-otherwise`, écriture Parquet partitionnée et plans `explain(True)` (BroadcastHashJoin puis SortMergeJoin).

### Partie 4 — Structured Streaming

Dans un premier terminal, lancer le producteur et le laisser actif :

```bash
docker compose exec jupyter python3 producer/producer.py --mode kafka --rate 20
```

Dans un second terminal :

```bash
docker compose exec jupyter spark-submit src/part4_streaming.py
```

Sinks console (`append`, `complete`, `update`), écriture du flux brut dans Bronze et démonstration du watermark de 10 minutes sur les événements en retard.

### Partie 5 — Pipeline de production

```bash
docker compose exec jupyter spark-submit src/part5_pipeline.py
docker compose exec jupyter spark-submit src/part5_verify.py --label avant_arret
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group sahlpay-pipeline-silver
docker compose exec jupyter spark-submit src/part5_pipeline.py
docker compose exec jupyter spark-submit src/part5_verify.py --label apres_reprise
docker compose exec mc mc ls --recursive local/lake/
```

Arrêter le pipeline (Ctrl-C) avant chaque vérification. Pour démontrer la reprise, attendre 2 minutes avant le second lancement, producteur actif. `part5_verify.py` relit la table Delta Gold, affiche `DESCRIBE HISTORY` et les offsets du checkpoint, puis compare Silver au contenu de Kafka.

Spark ne commite pas ses offsets dans Kafka : le checkpoint fait foi. Pour la supervision, le pipeline recopie après chaque micro-batch les offsets de la requête Silver dans le consumer group `sahlpay-pipeline-silver`, qui n'est jamais relu par le pipeline.

### Partie 6 — Analyse de graphes

```bash
docker compose exec jupyter spark-submit src/part6_graphframes.py
docker compose exec jupyter spark-shell -i graphx/GraphXAnalysis.scala
docker compose exec jupyter spark-submit src/part6_compare.py
docker compose exec jupyter spark-submit src/dashboard.py
```

Le graphe est construit à partir de `silver/transactions_clean`. Pour utiliser l'historique batch : `--source s3a://lake/silver/transactions_history`. Les seuils de détection sont paramétrables (`--device-min-accounts`, `--collector-min-senders`, `--cycle-min-amount`, `--component-min-size`). Le tableau de bord est généré dans `outputs/dashboard_conformite.html`.

### Arrêt

```bash
docker compose down        # conserve le lac de données
docker compose down -v     # supprime également les volumes
```

## Démonstration : suivi d'un paiement

Avec le pipeline de la Partie 5 et l'ingestion Bronze (`part4_streaming.py --queries bronze_raw`) en cours :

```bash
docker compose exec jupyter python3 producer/send_test_payment.py --tx-id TX-TEST-001 --city Rabat --amount 1234.56
docker compose exec jupyter spark-submit src/track_payment.py TX-TEST-001
```

Le paiement est retrouvé dans Kafka (partition et offsets), dans Bronze (JSON brut), dans Silver (enrichi avec le nom et la catégorie du commerçant) et dans le KPI Gold de sa fenêtre de 10 minutes. Délai mesuré entre l'envoi et la mise à jour du KPI : environ 11 secondes.

## Résultats

Résultats obtenus sur le cluster Docker (2 Workers, 4 cœurs par application).

| Domaine | Résultat |
|---|---|
| Batch | 50 000 événements, 16 517 transactions, 15 317 transactions `COMPLETED` distinctes |
| UDF vs expression native | 1,02 s contre 0,22 s sur 2 millions de lignes |
| Jointure | BroadcastHashJoin (référentiel de 300 lignes) ; SortMergeJoin lorsque le broadcast est désactivé |
| Streaming | Événements arrivant au-delà du watermark de 10 minutes écartés des agrégations fenêtrées et comptés par Spark |
| Reprise après arrêt | 21 386 lignes Silver, 0 doublon (404 doublons injectés retirés), 0 perte non expliquée ; reprise exacte aux offsets du checkpoint |
| Delta Lake | 47 versions dans l'historique de la table `kpi_city_10min` (un `MERGE` par micro-batch) |
| Détection | 9 scénarios injectés sur 9 retrouvés (2 appareils partagés, 4 cycles, 3 comptes collecteurs), 0 faux positif aux seuils retenus |
| GraphFrames / GraphX | Degrés identiques sur les 2 716 comptes du sous-graphe des transferts |

## Choix techniques

- **Dédoublonnage borné** : `dropDuplicatesWithinWatermark(["tx_id", "status"])` (Spark 3.5) conserve la clé métier tout en purgeant l'état une fois le watermark dépassé.
- **Sinks Gold idempotents** : les KPI fenêtrés sont émis en mode `update` et fusionnés par `MERGE` Delta sur `(city, window_start)` ; un micro-batch rejoué après une panne réécrit les mêmes valeurs.
- **Garantie exactly-once** : source Kafka rejouable, checkpoint (`offsets/` avant traitement, `commits/` après) et sinks transactionnels (journal `_spark_metadata` pour Parquet, `_delta_log` pour Delta).
- **Identifiants de graphe stables** : préfixes de type (`W-`, `D-`, `M-`) côté GraphFrames, et `VertexId` dérivé du rang de l'identifiant trié côté GraphX.
- **Vérité terrain** : le générateur écrit les scénarios injectés dans `data/fraud_ground_truth.json`, ce qui permet de mesurer le rappel et les faux positifs.

## Limites

- Données synthétiques ; les seuils de détection ont été calibrés en connaissant les scénarios injectés.
- Les arêtes du graphe sont agrégées : la chronologie des transferts n'est pas prise en compte dans la détection des cycles, limitée à la longueur 3.
- L'analyse de graphe est recalculée en batch et n'est pas mise à jour en continu.
- La couche Silver issue du streaming produit de nombreux petits fichiers ; un compactage périodique serait nécessaire en production.
- La configuration (identifiants, absence d'authentification sur Jupyter) est destinée à un usage local uniquement.

## Auteur

**Atmani Anas** — [github.com/AtmaniAnas1](https://github.com/AtmaniAnas1)

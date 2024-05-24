# Tp-Architecture-BI-SORNET-THUILLER-MACHEDA

## Introduction
Dans ce projet, nous explorons un jeu de données comprenant plus de 33 000 offres d'emploi provenant de LinkedIn. L'objectif est de traiter ces données de manière à les rendre exploitables pour une analyse approfondie . Nous allons charger les données dans une base de données Snowflake, effectuer des transformations si nécessaires, puis les analyser pour en extraire des insights pertinents. Enfin, nous utiliserons Streamlit pour visualiser nos résultats de manière interactive.

## 1. Chargement des Données

### Étape 1 : Création de la Base de Données
Pour stocker nos données, nous avons créé une nouvelle base de données Snowflake nommée "linkedin". Cette base de données servira de conteneur pour toutes les tables de données liées aux offres d'emploi.

<p align="center">
<img  width="600"  alt="MicrosoftTeams-image (6)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/50cf8f72-2779-48f5-a552-8940b3907882">
</p>

### Étape 2 : Configuration du Stage
Un stage a été configuré pour spécifier l'emplacement du bucket S3 contenant nos données. Ce stage permettra de charger les données directement depuis le bucket S3 dans Snowflake, facilitant ainsi le processus de chargement.

<p align="center">
<img  width="600"  alt="MicrosoftTeams-image (7)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/319dcb40-679f-49b7-9564-8646a7b05484">
</p>

### Étape 3 : Création des Formats de Fichier
Nous avons créé des formats de fichier adaptés à la structure de nos données. Étant donné que nos données sont fournies sous forme de fichiers CSV et sous forme de JSON, nous avons créé deux files format de fichier CSV et JSON dans Snowflake.

<p align="center">
<img align="center" width="600"  alt="MicrosoftTeams-image (8)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/103a3828-cc10-4345-990e-2e9b63949ea1">
</p>

### Étape 4 : Création des Tables de Données
Nous avons créé les différentes tables de données dans Snowflake en respectant la description des colonnes de chaque fichier. Chaque table a été définie avec les colonnes appropriées, mais initialement, nous avons utilisé des types de données génériques, tels que VARCHAR, car les types de données réels n'étaient pas clairement définis dans les fichiers source.

<p align="center">
<img  align="left" width="365" alt="MicrosoftTeams-image (15)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/25fbeed1-5bf7-4434-8e96-653b18380896">
<img width="500" alt="MicrosoftTeams-image (14)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/14c3f967-d41c-4bb9-9e38-0070e6ea4d2e">
<img width="500"  alt="MicrosoftTeams-image (11)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/41517cd5-fdf6-441b-8f7c-ceeb4e7c83e9">
</p>

### Étape 5 : Chargement des Données dans les Tables
Une fois les tables créées, nous avons procédé au chargement des données. Nous avons utilisé les commandes COPY INTO pour charger les données depuis les fichiers CSV et JSON dans les tables correspondantes. Nous avons pu déterminé les type de fichier depuis le stage que nous avons creer dans Snowflake avec la commande `list @linkdin_jobs`. Cela nous a permis de stocker les données dans notre entrepôt de données Snowflake pour une analyse ultérieure.

- List des types
<p align="center">
  <img align="center" width="600" alt="MicrosoftTeams-image (13)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/d585d6e9-d70d-442f-99af-59c15ba6da6e">
</p>

- Type **CSV**
<p align="center">
<img width="600" alt="MicrosoftTeams-image (20)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/1477df2c-b332-4f2d-a7e7-da1707e47a6a">
</p>

- Type **JSON**
<p align="center">
<img width="600" alt="MicrosoftTeams-image (16)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/a7db49e0-c30e-4718-8486-ea67a2968901">
</p>

### Étape 6 : Transformation des Données
Après le chargement initial des données, nous avons rencontré plusieurs défis pour les rendre exploitables. Outre la gestion des valeurs manquantes et la normalisation des données, nous avons dû faire face à un autre problème : les éléments étaient stockés dans une colonne au format JSON. Pour obtenir une table exploitable, nous avons créé des tables vides avec la structure appropriée, puis nous avons utilisé des fonctionnalités avancées de Snowflake telles que LATERAL FLATTEN et JSON_VALUE pour extraire les valeurs de la colonne JSON et les insérer dans les tables normales.

- Transformation Données **JSON**
  1. Creation Table vide pour charge
    <p align="center">
      <img width="220" alt="MicrosoftTeams-image (10)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/4e9c4409-58df-4850-a440-fbfa3f5b25a4">
    </p>
    
  2. Insertion des données
     <p align="center">
       <img  align="center" width="600" alt="MicrosoftTeams-image (17)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/7507f466-f1da-49ee-b9ed-4e9471a11a65">
    </p>
    
  3. Visualisation
    <p align="center">
      <img align="center" width="600" alt="MicrosoftTeams-image (18)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/405f75b2-9c70-44e4-a0cb-77b5c453b82b">
    </p>


- Transformation Données **CSV**
     <p align="center">
       <img width="600" alt="MicrosoftTeams-image (21)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/a0db9285-1804-4608-ab84-253d481c7d4a">
    </p>

## Problèmes Rencontrés et Solutions Apportées
Au cours du processus de chargement et de transformation des données, nous avons rencontré des difficultés lors du mappage des colonnes des fichiers avec les types de données Snowflake. Pour résoudre ce problème, nous avons consulté la documentation Snowflake pour comprendre les types de données appropriés à utiliser pour chaque colonne. Une fois les données chargées et les types de données analysés, nous avons pu procéder à la mise à jour des tables pour les aligner avec les types de données réels. Cette approche nous a permis de garantir la cohérence et l'intégrité des données dans notre entrepôt de données Snowflake.

## 2. Analyse des données

Avec un volume aussi important de données, l'exploration de cet ensemble offre un potentiel significatif. Il s'agit notamment d'explorer les titres d'emploi, les entreprises et les lieux offrant les rémunérations les plus élevées, ainsi que d'examiner les variations entre industries et entreprises en ce qui concerne leurs offres d'emploi/stages et leurs avantages.

Une fois que les fichiers ont été correctement chargés dans la base de données, l'analyse peut commencer, permettant de répondre à plusieurs questions clés :

**1. Top 10 des titres d'emploi les plus postés :**
   - Cette analyse permettra d'identifier les titres d'emploi les plus demandés dans l'ensemble de données, offrant ainsi un aperçu des besoins du marché du travail.
```sql
SELECT
  title, COUNT(*) AS job_count
FROM
  jobs_posting
GROUP BY
  title
ORDER BY
  job_count DESC
LIMIT 10;

```

**2. Les titres d'emploi les mieux rémunérés (en tenant compte de la devise) :**
   - Cette analyse mettra en lumière les emplois qui offrent les salaires les plus élevés, ce qui peut être crucial pour les candidats à la recherche d'opportunités bien rémunérées. Pour cette requête, nous avons rencontré quelques difficultés liées au jeu de données. En effet, nous avons observé la présence de plusieurs valeurs NULL dans les champs relatifs aux salaires. Afin de contourner ce problème, nous avons utilisé la fonction COALESCE, qui nous a permis de remplacer les valeurs NULL par zéro. De plus, nous avons restreint notre analyse aux offres d'emploi dont la devise était en USD, car les autres devises présentaient également des valeurs NULL.
```sql
SELECT
  title,
  COALESCE(MAX(max_salary),0) AS highest_salary
FROM
  jobs_posting
WHERE
  currency = 'USD'
GROUP BY
  title
ORDER BY
  highest_salary DESC
LIMIT 1;
```

**3. Répartition des offres d'emploi par taille d'entreprise :**
   - Cette analyse permettra de comprendre comment les opportunités d'emploi sont distribuées selon la taille des entreprises, ce qui peut avoir des implications sur les perspectives d'emploi dans différentes catégories d'entreprises.
```sql
SELECT
  companies.company_size,
  COUNT(*) AS job_count
FROM
  jobs_posting
JOIN
  companies ON jobs_posting.company_id = companies.company_id
GROUP BY companies.company_size;
```

**4. Répartition des offres d'emploi par type d'industrie :**
   - Cette analyse mettra en évidence les industries qui offrent le plus d'opportunités d'emploi, permettant ainsi de mieux comprendre les tendances du marché du travail dans différents secteurs.
```sql
SELECT
  industries.industry_name,
  COUNT(*) AS job_count
FROM
  jobs_posting
JOIN
  job_industries
ON
  jobs_posting.job_id = job_industries.job_id
JOIN
  industries
ON
  job_industries.industry_id = industries.industry_id
GROUP BY
  industries.industry_name;
```

**5. Répartition des offres d'emploi par type d'emploi (temps plein, stage, temps partiel) :**
   - Cette analyse permettra de déterminer la proportion d'offres d'emploi pour différents types d'emplois, ce qui peut aider à comprendre les préférences des employeurs en matière de recrutement.
```sql
SELECT
  formatted_work_type,
  COUNT(*) AS job_count
FROM
  jobs_posting
GROUP BY
  formatted_work_type;
```

## Visualisation des données :

Dans le cadre de cette section, nous avons utilisé Streamlit pour visualiser les résultats de chaque question sous forme de graphiques interactifs. Chaque graphique est accompagné d'un titre situé au-dessus, décrivant la nature de l'analyse effectuée, suivi des données utilisées pour générer le graphique, placées en dessous.

Voici un exemple de la mise en page de chaque graphique dans notre application Streamlit :

<p align="center">
<img width="600" alt="MicrosoftTeams-image (22)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/8a340a23-4414-4b73-806c-d427ccb1731c">
<img width="600" alt="MicrosoftTeams-image (23)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/c6a4be07-7ba6-4761-9b23-060cf60fedf1">
<img width="600" alt="MicrosoftTeams-image (24)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/99607d5a-b0cb-4f9f-9ef5-255ff146cbba">
<img width="600" alt="MicrosoftTeams-image (25)" src="https://github.com/DumeM2b/Tp-Architecture-BI-SORNET-THUILLER-MACHEDA/assets/163656850/0c94c6c2-378f-4485-97a6-76a1a5afeeb2">
</p>


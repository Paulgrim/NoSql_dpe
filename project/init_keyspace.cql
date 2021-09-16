DROP KEYSPACE IF EXISTS grimalpa_projet;

-- Creating a keyspace
CREATE KEYSPACE "grimalpa_projet"
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

-- Using it
USE grimalpa_projet;

--- Table to hold all the DPE from the 68
-- Source of complete truth
CREATE TABLE dpe
(
    dpe_id                                         int,
    departement                                    int,
    tr001_modele_dpe_id                            int,
    nom_methode_dpe                                varchar,
    date_visite_diagnostiqueur                     varchar,
    date_etablissement_dpe                         varchar,
    consommation_energie                           float,
    classe_consommation_energie                    varchar,
    estimation_ges                                 float,
    classe_estimation_ges                          varchar,
    secteur_activite                               varchar,
    annee_construction                             int,
    surface_habitable                              float,
    surface_thermique_lot                          float,
    commune                                        varchar,
    arrondissement                                 varchar,
    nom_rue                                        varchar,
    code_postal                                    int,
    code_insee_commune_actualise                   int,
    shon                                           float,
    surface_utile                                  float,
    surface_thermique_parties_communes             float,
    en_souterrain                                  boolean,
    en_surface                                     boolean,
    nombre_niveaux                                 int,
    surface_baies_orientees_nord                   float,
    surface_baies_orientees_est_ouest              float,
    surface_baies_orientees_sud                    float,
    surface_planchers_hauts_deperditifs            float,
    surface_planchers_bas_deperditifs              float,
    surface_parois_verticales_opaques_deperditives float,
    organisme_certificateur                        varchar,
    longitude                                      float,
    latitude                                       float,
    geo_adresse                                    varchar,
    tr001_modele_dpe_code                          varchar,
    tr002_type_batiment_code                       varchar,
    tr002_type_batiment_description                varchar,
    tr002_type_batiment_libelle                    varchar,
    tr012_categorie_erp_code                       varchar,
    tr012_categorie_erp_categorie                  varchar,
    tr012_categorie_erp_groupe                     varchar,
    tr013_type_erp_code                            varchar,
    tr013_type_erp_type                            varchar,
    PRIMARY KEY (departement, dpe_id)
);

---------------------------------------------------
-- Tables with cleaned information for correlation
CREATE TABLE surface_habitable_by_departement
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    surface_habitable           float,
    consommation_energie        float,

    PRIMARY KEY (departement, dpe_id, surface_habitable)
);

CREATE TABLE annee_construction_by_departement
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    annee_construction          int,
    consommation_energie        float,

    PRIMARY KEY (departement, dpe_id)
);

CREATE TABLE surface_thermique_lot_by_departement
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    surface_thermique_lot       float,
    consommation_energie        float,

    PRIMARY KEY (departement, dpe_id)
);


CREATE TABLE shon_by_departement
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    shon                        float,
    consommation_energie        float,

    PRIMARY KEY (departement, dpe_id)
);



CREATE TABLE surface_utile_by_departement
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    surface_utile               float,
    consommation_energie        float,

    PRIMARY KEY (departement, dpe_id)
);



CREATE TABLE surfThermiquePartiesCommunes_by_departement
(
    dpe_id                             int,
    departement                        int,
    classe_consommation_energie        varchar,
    estimation_ges                     float,
    classe_estimation_ges              varchar,
    surface_thermique_parties_communes float,
    consommation_energie               float,

    PRIMARY KEY (departement, dpe_id)
);

CREATE TABLE nombre_niveaux
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    nombre_niveaux              int,
    consommation_energie        float,

    PRIMARY KEY (departement, dpe_id)
);

CREATE TABLE surface_baies_orientees_nord_by_departement
(
    dpe_id                       int,
    departement                  int,
    classe_consommation_energie  varchar,
    estimation_ges               float,
    classe_estimation_ges        varchar,
    surface_baies_orientees_nord float,
    consommation_energie         float,

    PRIMARY KEY (departement, dpe_id)
);

CREATE TABLE surface_baies_orientees_est_ouest_by_departement
(
    dpe_id                            int,
    departement                       int,
    classe_consommation_energie       varchar,
    estimation_ges                    float,
    classe_estimation_ges             varchar,
    surface_baies_orientees_est_ouest float,
    consommation_energie              float,

    PRIMARY KEY (departement, dpe_id )
);

CREATE TABLE surface_baies_orientees_sud_by_departement
(
    dpe_id                      int,
    departement                 int,
    classe_consommation_energie varchar,
    estimation_ges              float,
    classe_estimation_ges       varchar,
    surface_baies_orientees_sud float,
    consommation_energie        float,

    PRIMARY KEY ( departement, dpe_id )
);

CREATE TABLE surfPlanchersHautsDeperd_by_departement
(
    dpe_id                              int,
    departement                         int,
    classe_consommation_energie         varchar,
    estimation_ges                      float,
    classe_estimation_ges               varchar,
    surface_planchers_hauts_deperditifs float,
    consommation_energie                float,

    PRIMARY KEY ( departement, dpe_id )
);

CREATE TABLE surfPlanchersBasDeperd_by_departement
(
    dpe_id                            int,
    departement                       int,
    classe_consommation_energie       varchar,
    estimation_ges                    float,
    classe_estimation_ges             varchar,
    surface_planchers_bas_deperditifs float,
    consommation_energie              float,

    PRIMARY KEY (departement, dpe_id)
);

CREATE TABLE surfParoisVertOpaquesDeperd_by_departement
(
    dpe_id                                         int,
    departement                                    int,
    classe_consommation_energie                    varchar,
    estimation_ges                                 float,
    classe_estimation_ges                          varchar,
    surface_parois_verticales_opaques_deperditives float,
    consommation_energie                           float,


    PRIMARY KEY (departement, dpe_id)
);
-------------------------------------------------------------
--- Table to classify houses according to their energy consumption
CREATE TABLE logement_by_consommation_energetique
(
    dpe_id                                         int,
    departement                                    int,
    tr001_modele_dpe_id                            int,
    nom_methode_dpe                                varchar,
    date_visite_diagnostiqueur                     varchar,
    date_etablissement_dpe                         varchar,
    consommation_energie                           float,
    classe_consommation_energie                    varchar,
    estimation_ges                                 float,
    classe_estimation_ges                          varchar,
    secteur_activite                               varchar,
    annee_construction                             int,
    surface_habitable                              float,
    surface_thermique_lot                          float,
    commune                                        varchar,
    arrondissement                                 varchar,
    nom_rue                                        varchar,
    code_postal                                    int,
    code_insee_commune_actualise                   int,
    shon                                           float,
    surface_utile                                  float,
    surface_thermique_parties_communes             float,
    en_souterrain                                  boolean,
    en_surface                                     boolean,
    nombre_niveaux                                 int,
    surface_baies_orientees_nord                   float,
    surface_baies_orientees_est_ouest              float,
    surface_baies_orientees_sud                    float,
    surface_planchers_hauts_deperditifs            float,
    surface_planchers_bas_deperditifs              float,
    surface_parois_verticales_opaques_deperditives float,
    organisme_certificateur                        varchar,
    longitude                                      float,
    latitude                                       float,
    geo_adresse                                    varchar,
    tr001_modele_dpe_code                          varchar,
    tr002_type_batiment_code                       varchar,
    tr002_type_batiment_description                varchar,
    tr002_type_batiment_libelle                    varchar,
    tr012_categorie_erp_code                       varchar,
    tr012_categorie_erp_categorie                  varchar,
    tr012_categorie_erp_groupe                     varchar,
    tr013_type_erp_code                            varchar,
    tr013_type_erp_type                            varchar,
    PRIMARY KEY ((departement, classe_consommation_energie), consommation_energie, dpe_id)
) WITH CLUSTERING ORDER BY (consommation_energie ASC);

-------------------------------------
------density 100km²-----------------
-------------------------------------
-- Table of homes by x y and department (10km * 10km)
CREATE TABLE logement_by_space_and_departement
(
    x                                              int,
    y                                              int,
    dpe_id                                         int,
    departement                                    int,
    tr001_modele_dpe_id                            int,
    nom_methode_dpe                                varchar,
    date_visite_diagnostiqueur                     varchar,
    date_etablissement_dpe                         varchar,
    consommation_energie                           float,
    classe_consommation_energie                    varchar,
    estimation_ges                                 float,
    classe_estimation_ges                          varchar,
    secteur_activite                               varchar,
    annee_construction                             int,
    surface_habitable                              float,
    surface_thermique_lot                          float,
    commune                                        varchar,
    arrondissement                                 varchar,
    nom_rue                                        varchar,
    code_postal                                    int,
    code_insee_commune_actualise                   int,
    shon                                           float,
    surface_utile                                  float,
    surface_thermique_parties_communes             float,
    en_souterrain                                  boolean,
    en_surface                                     boolean,
    nombre_niveaux                                 int,
    surface_baies_orientees_nord                   float,
    surface_baies_orientees_est_ouest              float,
    surface_baies_orientees_sud                    float,
    surface_planchers_hauts_deperditifs            float,
    surface_planchers_bas_deperditifs              float,
    surface_parois_verticales_opaques_deperditives float,
    organisme_certificateur                        varchar,
    longitude                                      float,
    latitude                                       float,
    geo_adresse                                    varchar,
    tr001_modele_dpe_code                          varchar,
    tr002_type_batiment_code                       varchar,
    tr002_type_batiment_description                varchar,
    tr002_type_batiment_libelle                    varchar,
    tr012_categorie_erp_code                       varchar,
    tr012_categorie_erp_categorie                  varchar,
    tr012_categorie_erp_groupe                     varchar,
    tr013_type_erp_code                            varchar,
    tr013_type_erp_type                            varchar,
    PRIMARY KEY ((departement, x, y), consommation_energie, dpe_id)
) WITH CLUSTERING ORDER BY (consommation_energie ASC);

--------------------------------------------
-- Table to count homes by x y and departement (10km * 10km)
CREATE TABLE density_by_departement
(
    x           int,
    y           int,
    departement int,
    nbLogement  counter,
    PRIMARY KEY ((departement), x, y)
);

-------------------------------------
------density 25 km²-----------------
-------------------------------------
-- Table of homes by x y and departement (5km * 5km)

CREATE TABLE logement_by_space_and_departement_5km
(
    x                                              int,
    y                                              int,
    dpe_id                                         int,
    departement                                    int,
    tr001_modele_dpe_id                            int,
    nom_methode_dpe                                varchar,
    date_visite_diagnostiqueur                     varchar,
    date_etablissement_dpe                         varchar,
    consommation_energie                           float,
    classe_consommation_energie                    varchar,
    estimation_ges                                 float,
    classe_estimation_ges                          varchar,
    secteur_activite                               varchar,
    annee_construction                             int,
    surface_habitable                              float,
    surface_thermique_lot                          float,
    commune                                        varchar,
    arrondissement                                 varchar,
    nom_rue                                        varchar,
    code_postal                                    int,
    code_insee_commune_actualise                   int,
    shon                                           float,
    surface_utile                                  float,
    surface_thermique_parties_communes             float,
    en_souterrain                                  boolean,
    en_surface                                     boolean,
    nombre_niveaux                                 int,
    surface_baies_orientees_nord                   float,
    surface_baies_orientees_est_ouest              float,
    surface_baies_orientees_sud                    float,
    surface_planchers_hauts_deperditifs            float,
    surface_planchers_bas_deperditifs              float,
    surface_parois_verticales_opaques_deperditives float,
    organisme_certificateur                        varchar,
    longitude                                      float,
    latitude                                       float,
    geo_adresse                                    varchar,
    tr001_modele_dpe_code                          varchar,
    tr002_type_batiment_code                       varchar,
    tr002_type_batiment_description                varchar,
    tr002_type_batiment_libelle                    varchar,
    tr012_categorie_erp_code                       varchar,
    tr012_categorie_erp_categorie                  varchar,
    tr012_categorie_erp_groupe                     varchar,
    tr013_type_erp_code                            varchar,
    tr013_type_erp_type                            varchar,
    PRIMARY KEY ((departement, x, y), consommation_energie, dpe_id)
) WITH CLUSTERING ORDER BY (consommation_energie ASC);

--------------------------------------------
-- Table to count homes according to x y and departement (5km * 5km)
CREATE TABLE density_by_departement_5km
(
    x           int,
    y           int,
    departement int,
    nbLogement  counter,
    PRIMARY KEY ((departement), x, y)
);


------------------------------------------------
-- Table to count homes according to their type
CREATE TABLE type_logement_by_departement
(
    tr002_type_batiment_description varchar,
    nbLogement                      counter,
    departement                     int,
    classe_consommation_energie     varchar,
    PRIMARY KEY ((departement), classe_consommation_energie, tr002_type_batiment_description)
);


-----------------------------------------------------------
-- Table to count energy consumption according to their type
CREATE TABLE energie_type_logement_by_departement
(
    tr002_type_batiment_description varchar,
    departement                     int,
    consommation_energie            float,
    dpe_id                          int,
    PRIMARY KEY ((departement, tr002_type_batiment_description), dpe_id, consommation_energie)
);





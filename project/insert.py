import argparse
import glob
import os
from typing import List

from convertorDistance import convertMtoKM, wgs84_to_epsg3035
from utils import CassandraWrapper, DPE, stream_csv


# list to clean up method names
naming_for_3CL = [
    "3CL",
    "3CL - DPE",
    "3CL-DPE",
    "Méthode 3CL",
    "TROIS_CL_DPE",
]
naming_for_facture = [
    "facture",
    "FACTURE",
    "FACTURE - DPE",
    "FACTURE SEULE",
    "Facture",
    "FACTURES",
    "Méthode Facture",
]
naming_for_TH = ["TH_BCE", "TH_C_E", "Th-BCE", "th-C-E", "th-CE"]
naming_for_mix = [
    "MIXTE FACTURE+3CL - DPE pour ECS",
]

# list to clean up energy class
classe_energetique_ges = ["A", "B", "C", "D", "E", "F", "G"]


class CreateWrapper(CassandraWrapper):
    def create_dpe(self, dpe: DPE):
        """
        Insert a DPE in the Cassandra Cluster.

        :param dpe: DPE object
        :return:
        """
        self._session.execute(
            """
            INSERT INTO dpe(
                dpe_id,
                departement,
                tr001_modele_dpe_id,
                nom_methode_dpe,
                date_visite_diagnostiqueur,
                date_etablissement_dpe,
                consommation_energie,
                classe_consommation_energie,
                estimation_ges,
                classe_estimation_ges,
                secteur_activite,
                annee_construction,
                surface_habitable,
                surface_thermique_lot,
                commune,
                arrondissement,
                nom_rue,
                code_postal,
                code_insee_commune_actualise,
                shon,
                surface_utile,
                surface_thermique_parties_communes,
                en_souterrain,
                en_surface,
                nombre_niveaux,
                surface_baies_orientees_nord,
                surface_baies_orientees_est_ouest,
                surface_baies_orientees_sud,
                surface_planchers_hauts_deperditifs,
                surface_planchers_bas_deperditifs,
                surface_parois_verticales_opaques_deperditives,
                organisme_certificateur,
                longitude,
                latitude,
                geo_adresse,
                tr001_modele_dpe_code,
                tr002_type_batiment_code,
                tr002_type_batiment_description,
                tr002_type_batiment_libelle,
                tr012_categorie_erp_code,
                tr012_categorie_erp_categorie,
                tr012_categorie_erp_groupe,
                tr013_type_erp_code,
                tr013_type_erp_type)
            VALUES(
                %(dpe_id)s,
                %(departement)s,
                %(tr001_modele_dpe_id)s,
                %(nom_methode_dpe)s,
                %(date_visite_diagnostiqueur)s,
                %(date_etablissement_dpe)s,
                %(consommation_energie)s,
                %(classe_consommation_energie)s,
                %(estimation_ges)s,
                %(classe_estimation_ges)s,
                %(secteur_activite)s,
                %(annee_construction)s,
                %(surface_habitable)s,
                %(surface_thermique_lot)s,
                %(commune)s,
                %(arrondissement)s,
                %(nom_rue)s,
                %(code_postal)s,
                %(code_insee_commune_actualise)s,
                %(shon)s,
                %(surface_utile)s,
                %(surface_thermique_parties_communes)s,
                %(en_souterrain)s,
                %(en_surface)s,
                %(nombre_niveaux)s,
                %(surface_baies_orientees_nord)s,
                %(surface_baies_orientees_est_ouest)s,
                %(surface_baies_orientees_sud)s,
                %(surface_planchers_hauts_deperditifs)s,
                %(surface_planchers_bas_deperditifs)s,
                %(surface_parois_verticales_opaques_deperditives)s,
                %(organisme_certificateur)s,
                %(longitude)s,
                %(latitude)s,
                %(geo_adresse)s,
                %(tr001_modele_dpe_code)s,
                %(tr002_type_batiment_code)s,
                %(tr002_type_batiment_description)s,
                %(tr002_type_batiment_libelle)s,
                %(tr012_categorie_erp_code)s,
                %(tr012_categorie_erp_categorie)s,
                %(tr012_categorie_erp_groupe)s,
                %(tr013_type_erp_code)s,
                %(tr013_type_erp_type)s
                );
            """,
            dpe.__dict__,
        )

    # insertion for correlation
    def create_surface_habitable(self, dpe: DPE):
        """
        insert data in surface_habitable_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_habitable == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surface_habitable_by_departement(
                    dpe_id,
                    departement,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    surface_habitable           ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_habitable)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def annee_construction(self, dpe: DPE):
        """
        insert data in annee_construction_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.annee_construction == -1:
            return False
        self._session.execute(
            """
                INSERT INTO annee_construction_by_departement(
                    dpe_id                      ,
                    departement                 ,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    annee_construction          ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(annee_construction)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_thermique_lot(self, dpe: DPE):
        """
        insert data in surface_thermique_lot_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_thermique_lot == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surface_thermique_lot_by_departement(
                    dpe_id                      ,
                    departement                 ,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    surface_thermique_lot       ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_thermique_lot)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def shon(self, dpe: DPE):
        """
        insert data in shon_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.shon == -1:
            return False
        self._session.execute(
            """
                INSERT INTO shon_by_departement(
                    dpe_id                      ,
                    departement                 ,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    shon                        ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(shon)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_utile(self, dpe: DPE):
        """
        insert data in surface_utile_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_utile == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surface_utile_by_departement(
                    dpe_id                      ,
                    departement                 ,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    surface_utile               ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_utile)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_thermique_parties_communes(self, dpe: DPE):
        """
        insert data in surfThermiquePartiesCommunes_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_thermique_parties_communes == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surfThermiquePartiesCommunes_by_departement(
                    dpe_id                             ,
                    departement                        ,
                    classe_consommation_energie        ,
                    estimation_ges                     ,
                    classe_estimation_ges              ,
                    surface_thermique_parties_communes ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_thermique_parties_communes)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def nombre_niveaux(self, dpe: DPE):
        """
        insert data in nombre_niveaux
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.nombre_niveaux == -1:
            return False
        self._session.execute(
            """
                INSERT INTO nombre_niveaux(
                    dpe_id                      ,
                    departement                 ,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    nombre_niveaux              ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(nombre_niveaux)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_baies_orientees_nord(self, dpe: DPE):
        """
        insert data in surface_baies_orientees_nord_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_baies_orientees_nord == -1:
            return True
        self._session.execute(
            """
                INSERT INTO surface_baies_orientees_nord_by_departement(
                    dpe_id                       ,
                    departement                  ,
                    classe_consommation_energie  ,
                    estimation_ges               ,
                    classe_estimation_ges        ,
                    surface_baies_orientees_nord ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_baies_orientees_nord)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return False

    def surface_baies_orientees_est_ouest(self, dpe: DPE):
        """
        insert data in surface_baies_orientees_est_ouest_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_baies_orientees_est_ouest == -1:
            return True
        self._session.execute(
            """
                INSERT INTO surface_baies_orientees_est_ouest_by_departement(
                    dpe_id                            ,
                    departement                       ,
                    classe_consommation_energie       ,
                    estimation_ges                    ,
                    classe_estimation_ges             ,
                    surface_baies_orientees_est_ouest ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_baies_orientees_est_ouest)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return False

    def surface_baies_orientees_sud(self, dpe: DPE):
        """
        insert data in surface_baies_orientees_sud_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_baies_orientees_sud == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surface_baies_orientees_sud_by_departement(
                    dpe_id                      ,
                    departement                 ,
                    classe_consommation_energie ,
                    estimation_ges              ,
                    classe_estimation_ges       ,
                    surface_baies_orientees_sud ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_baies_orientees_sud)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_planchers_hauts_deperditifs(self, dpe: DPE):
        """
        insert data in surfPlanchersHautsDeperd_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_planchers_hauts_deperditifs == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surfPlanchersHautsDeperd_by_departement(
                    dpe_id                              ,
                    departement                         ,
                    classe_consommation_energie         ,
                    estimation_ges                      ,
                    classe_estimation_ges               ,
                    surface_planchers_hauts_deperditifs ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_planchers_hauts_deperditifs)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_planchers_bas_deperditifs(self, dpe: DPE):
        """
        insert data in surfPlanchersBasDeperd_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_planchers_bas_deperditifs == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surfPlanchersBasDeperd_by_departement(
                    dpe_id                            ,
                    departement                       ,
                    classe_consommation_energie       ,
                    estimation_ges                    ,
                    classe_estimation_ges             ,
                    surface_planchers_bas_deperditifs ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_planchers_bas_deperditifs)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def surface_parois_verticales_opaques_deperditives(self, dpe: DPE):
        """
        insert data in surfParoisVertOpaquesDeperd_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.surface_parois_verticales_opaques_deperditives == -1:
            return False
        self._session.execute(
            """
                INSERT INTO surfParoisVertOpaquesDeperd_by_departement(
                    dpe_id                                         ,
                    departement                                    ,
                    classe_consommation_energie                    ,
                    estimation_ges                                 ,
                    classe_estimation_ges                          ,
                    surface_parois_verticales_opaques_deperditives ,
                    consommation_energie
                )VALUES(
                    %(dpe_id)s,
                    %(departement)s,
                    %(classe_consommation_energie)s,
                    %(estimation_ges)s,
                    %(classe_estimation_ges)s,
                    %(surface_parois_verticales_opaques_deperditives)s,
                    %(consommation_energie)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def logement_consommation_energie(self, dpe: DPE):
        """
        insert data in logement_by_consommation_energetique
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        self._session.execute(
            """
            INSERT INTO logement_by_consommation_energetique(
                dpe_id,
                departement,
                tr001_modele_dpe_id,
                nom_methode_dpe,
                date_visite_diagnostiqueur,
                date_etablissement_dpe,
                consommation_energie,
                classe_consommation_energie,
                estimation_ges,
                classe_estimation_ges,
                secteur_activite,
                annee_construction,
                surface_habitable,
                surface_thermique_lot,
                commune,
                arrondissement,
                nom_rue,
                code_postal,
                code_insee_commune_actualise,
                shon,
                surface_utile,
                surface_thermique_parties_communes,
                en_souterrain,
                en_surface,
                nombre_niveaux,
                surface_baies_orientees_nord,
                surface_baies_orientees_est_ouest,
                surface_baies_orientees_sud,
                surface_planchers_hauts_deperditifs,
                surface_planchers_bas_deperditifs,
                surface_parois_verticales_opaques_deperditives,
                organisme_certificateur,
                longitude,
                latitude,
                geo_adresse,
                tr001_modele_dpe_code,
                tr002_type_batiment_code,
                tr002_type_batiment_description,
                tr002_type_batiment_libelle,
                tr012_categorie_erp_code,
                tr012_categorie_erp_categorie,
                tr012_categorie_erp_groupe,
                tr013_type_erp_code,
                tr013_type_erp_type)
            VALUES(
                %(dpe_id)s,
                %(departement)s,
                %(tr001_modele_dpe_id)s,
                %(nom_methode_dpe)s,
                %(date_visite_diagnostiqueur)s,
                %(date_etablissement_dpe)s,
                %(consommation_energie)s,
                %(classe_consommation_energie)s,
                %(estimation_ges)s,
                %(classe_estimation_ges)s,
                %(secteur_activite)s,
                %(annee_construction)s,
                %(surface_habitable)s,
                %(surface_thermique_lot)s,
                %(commune)s,
                %(arrondissement)s,
                %(nom_rue)s,
                %(code_postal)s,
                %(code_insee_commune_actualise)s,
                %(shon)s,
                %(surface_utile)s,
                %(surface_thermique_parties_communes)s,
                %(en_souterrain)s,
                %(en_surface)s,
                %(nombre_niveaux)s,
                %(surface_baies_orientees_nord)s,
                %(surface_baies_orientees_est_ouest)s,
                %(surface_baies_orientees_sud)s,
                %(surface_planchers_hauts_deperditifs)s,
                %(surface_planchers_bas_deperditifs)s,
                %(surface_parois_verticales_opaques_deperditives)s,
                %(organisme_certificateur)s,
                %(longitude)s,
                %(latitude)s,
                %(geo_adresse)s,
                %(tr001_modele_dpe_code)s,
                %(tr002_type_batiment_code)s,
                %(tr002_type_batiment_description)s,
                %(tr002_type_batiment_libelle)s,
                %(tr012_categorie_erp_code)s,
                %(tr012_categorie_erp_categorie)s,
                %(tr012_categorie_erp_groupe)s,
                %(tr013_type_erp_code)s,
                %(tr013_type_erp_type)s
                );
            """,
            dpe.__dict__,
        )
        return True

    def logement_by_space_10km(self, dpe: DPE):
        """
        insert data in logement_by_space_and_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.longitude == -1 or dpe.latitude == -1:
            return False
        x, y = wgs84_to_epsg3035(dpe.longitude, dpe.latitude)
        # coordonnee du point haut
        x = convertMtoKM(x, 10)
        y = convertMtoKM(y, 10)
        self._session.execute(
            """
            INSERT INTO logement_by_space_and_departement(
                x,
                y,
                dpe_id,
                departement,
                tr001_modele_dpe_id,
                nom_methode_dpe,
                date_visite_diagnostiqueur,
                date_etablissement_dpe,
                consommation_energie,
                classe_consommation_energie,
                estimation_ges,
                classe_estimation_ges,
                secteur_activite,
                annee_construction,
                surface_habitable,
                surface_thermique_lot,
                commune,
                arrondissement,
                nom_rue,
                code_postal,
                code_insee_commune_actualise,
                shon,
                surface_utile,
                surface_thermique_parties_communes,
                en_souterrain,
                en_surface,
                nombre_niveaux,
                surface_baies_orientees_nord,
                surface_baies_orientees_est_ouest,
                surface_baies_orientees_sud,
                surface_planchers_hauts_deperditifs,
                surface_planchers_bas_deperditifs,
                surface_parois_verticales_opaques_deperditives,
                organisme_certificateur,
                longitude,
                latitude,
                geo_adresse,
                tr001_modele_dpe_code,
                tr002_type_batiment_code,
                tr002_type_batiment_description,
                tr002_type_batiment_libelle,
                tr012_categorie_erp_code,
                tr012_categorie_erp_categorie,
                tr012_categorie_erp_groupe,
                tr013_type_erp_code,
                tr013_type_erp_type)
            VALUES(
                %(x)s,
                %(y)s,
                %(dpe_id)s,
                %(departement)s,
                %(tr001_modele_dpe_id)s,
                %(nom_methode_dpe)s,
                %(date_visite_diagnostiqueur)s,
                %(date_etablissement_dpe)s,
                %(consommation_energie)s,
                %(classe_consommation_energie)s,
                %(estimation_ges)s,
                %(classe_estimation_ges)s,
                %(secteur_activite)s,
                %(annee_construction)s,
                %(surface_habitable)s,
                %(surface_thermique_lot)s,
                %(commune)s,
                %(arrondissement)s,
                %(nom_rue)s,
                %(code_postal)s,
                %(code_insee_commune_actualise)s,
                %(shon)s,
                %(surface_utile)s,
                %(surface_thermique_parties_communes)s,
                %(en_souterrain)s,
                %(en_surface)s,
                %(nombre_niveaux)s,
                %(surface_baies_orientees_nord)s,
                %(surface_baies_orientees_est_ouest)s,
                %(surface_baies_orientees_sud)s,
                %(surface_planchers_hauts_deperditifs)s,
                %(surface_planchers_bas_deperditifs)s,
                %(surface_parois_verticales_opaques_deperditives)s,
                %(organisme_certificateur)s,
                %(longitude)s,
                %(latitude)s,
                %(geo_adresse)s,
                %(tr001_modele_dpe_code)s,
                %(tr002_type_batiment_code)s,
                %(tr002_type_batiment_description)s,
                %(tr002_type_batiment_libelle)s,
                %(tr012_categorie_erp_code)s,
                %(tr012_categorie_erp_categorie)s,
                %(tr012_categorie_erp_groupe)s,
                %(tr013_type_erp_code)s,
                %(tr013_type_erp_type)s
                );
            """,
            {**dpe.__dict__, "x": x, "y": y},
        )
        return True

    def density_by_departement_10km(self, dpe: DPE):
        """
        insert data in density_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.longitude == -1 or dpe.latitude == -1:
            return False
        x, y = wgs84_to_epsg3035(dpe.longitude, dpe.latitude)
        x = convertMtoKM(x, 10)
        y = convertMtoKM(y, 10)
        self._session.execute(
            """
             UPDATE density_by_departement
                SET nbLogement = nbLogement + 1
                WHERE departement = %(departement)s
                AND x = %(x)s
                AND y = %(y)s;
            """,
            {**dpe.__dict__, "x": x, "y": y},
        )
        return True

    def logement_by_space_5km(self, dpe: DPE):
        """
        insert data in logement_by_space_and_departement_5km
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.longitude == -1 or dpe.latitude == -1:
            return False
        x, y = wgs84_to_epsg3035(dpe.longitude, dpe.latitude)
        x = convertMtoKM(x, 5)
        y = convertMtoKM(y, 5)
        self._session.execute(
            """
            INSERT INTO logement_by_space_and_departement_5km(
                x,
                y,
                dpe_id,
                departement,
                tr001_modele_dpe_id,
                nom_methode_dpe,
                date_visite_diagnostiqueur,
                date_etablissement_dpe,
                consommation_energie,
                classe_consommation_energie,
                estimation_ges,
                classe_estimation_ges,
                secteur_activite,
                annee_construction,
                surface_habitable,
                surface_thermique_lot,
                commune,
                arrondissement,
                nom_rue,
                code_postal,
                code_insee_commune_actualise,
                shon,
                surface_utile,
                surface_thermique_parties_communes,
                en_souterrain,
                en_surface,
                nombre_niveaux,
                surface_baies_orientees_nord,
                surface_baies_orientees_est_ouest,
                surface_baies_orientees_sud,
                surface_planchers_hauts_deperditifs,
                surface_planchers_bas_deperditifs,
                surface_parois_verticales_opaques_deperditives,
                organisme_certificateur,
                longitude,
                latitude,
                geo_adresse,
                tr001_modele_dpe_code,
                tr002_type_batiment_code,
                tr002_type_batiment_description,
                tr002_type_batiment_libelle,
                tr012_categorie_erp_code,
                tr012_categorie_erp_categorie,
                tr012_categorie_erp_groupe,
                tr013_type_erp_code,
                tr013_type_erp_type)
            VALUES(
                %(x)s,
                %(y)s,
                %(dpe_id)s,
                %(departement)s,
                %(tr001_modele_dpe_id)s,
                %(nom_methode_dpe)s,
                %(date_visite_diagnostiqueur)s,
                %(date_etablissement_dpe)s,
                %(consommation_energie)s,
                %(classe_consommation_energie)s,
                %(estimation_ges)s,
                %(classe_estimation_ges)s,
                %(secteur_activite)s,
                %(annee_construction)s,
                %(surface_habitable)s,
                %(surface_thermique_lot)s,
                %(commune)s,
                %(arrondissement)s,
                %(nom_rue)s,
                %(code_postal)s,
                %(code_insee_commune_actualise)s,
                %(shon)s,
                %(surface_utile)s,
                %(surface_thermique_parties_communes)s,
                %(en_souterrain)s,
                %(en_surface)s,
                %(nombre_niveaux)s,
                %(surface_baies_orientees_nord)s,
                %(surface_baies_orientees_est_ouest)s,
                %(surface_baies_orientees_sud)s,
                %(surface_planchers_hauts_deperditifs)s,
                %(surface_planchers_bas_deperditifs)s,
                %(surface_parois_verticales_opaques_deperditives)s,
                %(organisme_certificateur)s,
                %(longitude)s,
                %(latitude)s,
                %(geo_adresse)s,
                %(tr001_modele_dpe_code)s,
                %(tr002_type_batiment_code)s,
                %(tr002_type_batiment_description)s,
                %(tr002_type_batiment_libelle)s,
                %(tr012_categorie_erp_code)s,
                %(tr012_categorie_erp_categorie)s,
                %(tr012_categorie_erp_groupe)s,
                %(tr013_type_erp_code)s,
                %(tr013_type_erp_type)s
                );
            """,
            {**dpe.__dict__, "x": x, "y": y},
        )
        return True

    def density_by_departement_5km(self, dpe: DPE):
        """
        insert data in density_by_departement_5km
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if dpe.longitude == -1 or dpe.latitude == -1:
            return False
        x, y = wgs84_to_epsg3035(dpe.longitude, dpe.latitude)
        x = convertMtoKM(x, 5)
        y = convertMtoKM(y, 5)
        self._session.execute(
            """
             UPDATE density_by_departement_5km
                SET nbLogement = nbLogement + 1
                WHERE departement = %(departement)s
                AND x = %(x)s
                AND y = %(y)s;
            """,
            {**dpe.__dict__, "x": x, "y": y},
        )
        return True

    def type_logement(self, dpe: DPE):
        """
        insert data in type_logement_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if len(dpe.tr002_type_batiment_description) == 0:
            return False
        self._session.execute(
            """
             UPDATE type_logement_by_departement
                SET nbLogement = nbLogement + 1
                WHERE departement = %(departement)s
                AND classe_consommation_energie = %(classe_consommation_energie)s
                AND tr002_type_batiment_description = %(type)s;
            """,
            {
                "departement": dpe.departement,
                "classe_consommation_energie": dpe.classe_consommation_energie,
                "type": dpe.tr002_type_batiment_description,
            },
        )
        return True

    def energie_type_logement_by_departement(self, dpe: DPE):
        """
        insert data in energie_type_logement_by_departement
        :param dpe: DPE object
        :return: true if data is inserted else false
        """
        if len(dpe.tr002_type_batiment_description) == 0:
            return False
        self._session.execute(
            """
                INSERT INTO energie_type_logement_by_departement(
                tr002_type_batiment_description,
                departement                    ,
                consommation_energie           ,
                dpe_id                         )
                VALUES(
                %(type)s,
                %(departement)s,
                %(consommation_energie)s,
                %(dpe_id)s
                );
            """,
            {
                "departement": dpe.departement,
                "consommation_energie": dpe.consommation_energie,
                "type": dpe.tr002_type_batiment_description,
                "dpe_id": dpe.dpe_id,
            },
        )
        return True


def skip_dpe(dpe: DPE):
    """
    test if dpe is correct
    :param dpe: DPE object
    :return: boolean
    """
    if (
        (dpe.classe_consommation_energie not in classe_energetique_ges)
        or (dpe.classe_estimation_ges not in classe_energetique_ges)
        or conso_energie_is_null(dpe)
        or ges_is_null(dpe)
    ):
        return True
    else:
        return False


def conso_energie_is_null(dpe: DPE):
    """
    test if dpe.consommation_energie is correct
    :param dpe: DPE object
    :return: boolean
    """
    return dpe.consommation_energie <= 0 or dpe.consommation_energie == -1


def ges_is_null(dpe: DPE):
    """
    test if dpe.estimation_ges is incorrect
    :param dpe: DPE object
    :return: boolean
    """
    return dpe.estimation_ges <= 0 or dpe.estimation_ges == -1


def adjust_method(method: str) -> str:
    """
    returns the correct method name
    :param method: string
    :return: string
    """
    if method in naming_for_3CL:
        return "3CL"
    elif method in naming_for_TH:
        return "TH"
    elif method in naming_for_mix:
        return "MIX"
    elif method in naming_for_facture:
        return "FACTURE"
    else:
        return "UNKNOWN"


def soft_int(s: str) -> int:
    """
    convert string to integer
    :param s: string
    :return: integer
    """
    return int(s) if s.isdigit() else -1


def soft_float(s: str) -> float:
    """
    convert string to float
    :param s: string
    :return: float
    """
    if s.isdigit():
        return float(s)
    elif s.replace(".", "", 1).isdigit():
        return float(s)
    elif s.replace(".", "", 1).replace("-", "", 1).isdigit():
        return float(s)
    else:
        return -1


def row_to_dpe(row: List[str]) -> DPE:
    """
    Convert a CSV file line to a DPE object

    Used priorly to insertion

    :param row:
    :return: DPE object
    """
    (
        id,
        _,
        _,
        _,
        tr001_modele_dpe_id,
        nom_methode_dpe,
        _,
        _,
        _,
        date_visite_diagnostiqueur,
        date_etablissement_dpe,
        _,
        _,
        _,
        consommation_energie,
        classe_consommation_energie,
        estimation_ges,
        classe_estimation_ges,
        _,
        secteur_activite,
        _,
        _,
        annee_construction,
        surface_habitable,
        surface_thermique_lot,
        departement,
        commune,
        arrondissement,
        _,
        nom_rue,
        _,
        _,
        _,
        _,
        _,
        code_postal,
        _,
        code_insee_commune_actualise,
        _,
        _,
        _,
        _,
        _,
        _,
        shon,
        surface_utile,
        surface_thermique_parties_communes,
        en_souterrain,
        en_surface,
        nombre_niveaux,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        surface_baies_orientees_nord,
        surface_baies_orientees_est_ouest,
        surface_baies_orientees_sud,
        surface_planchers_hauts_deperditifs,
        surface_planchers_bas_deperditifs,
        surface_parois_verticales_opaques_deperditives,
        _,
        organisme_certificateur,
        _,
        _,
        _,
        _,
        longitude,
        latitude,
        _,
        _,
        geo_adresse,
        _,
        _,
        _,
        tr001_modele_dpe_code,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        tr002_type_batiment_code,
        tr002_type_batiment_description,
        tr002_type_batiment_libelle,
        _,
        _,
        _,
        tr012_categorie_erp_code,
        tr012_categorie_erp_categorie,
        tr012_categorie_erp_groupe,
        _,
        tr013_type_erp_code,
        tr013_type_erp_type,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = row
    dpe = DPE(
        dpe_id=int(id),
        departement=int(departement),
        tr001_modele_dpe_id=soft_int(tr001_modele_dpe_id),
        nom_methode_dpe=adjust_method(nom_methode_dpe),
        date_visite_diagnostiqueur=date_visite_diagnostiqueur,
        date_etablissement_dpe=date_etablissement_dpe,
        consommation_energie=soft_float(consommation_energie),
        classe_consommation_energie=classe_consommation_energie,
        estimation_ges=soft_float(estimation_ges),
        classe_estimation_ges=classe_estimation_ges,
        secteur_activite=secteur_activite,
        annee_construction=soft_int(annee_construction),
        surface_habitable=soft_float(surface_habitable),
        surface_thermique_lot=soft_float(surface_thermique_lot),
        commune=commune,
        arrondissement=arrondissement,
        nom_rue=nom_rue,
        code_postal=soft_int(code_postal),
        code_insee_commune_actualise=soft_int(code_insee_commune_actualise),
        shon=soft_float(shon),
        surface_utile=soft_float(surface_utile),
        surface_thermique_parties_communes=soft_float(
            surface_thermique_parties_communes
        ),
        en_souterrain=bool(en_souterrain),
        en_surface=bool(en_surface),
        nombre_niveaux=soft_int(nombre_niveaux),
        surface_baies_orientees_nord=soft_float(surface_baies_orientees_nord),
        surface_baies_orientees_est_ouest=soft_float(surface_baies_orientees_est_ouest),
        surface_baies_orientees_sud=soft_float(surface_baies_orientees_sud),
        surface_planchers_hauts_deperditifs=soft_float(
            surface_planchers_hauts_deperditifs
        ),
        surface_planchers_bas_deperditifs=soft_float(surface_planchers_bas_deperditifs),
        surface_parois_verticales_opaques_deperditives=soft_float(
            surface_parois_verticales_opaques_deperditives
        ),
        organisme_certificateur=organisme_certificateur,
        longitude=soft_float(longitude),
        latitude=soft_float(latitude),
        geo_adresse=geo_adresse,
        tr001_modele_dpe_code=tr001_modele_dpe_code,
        tr002_type_batiment_code=tr002_type_batiment_code,
        tr002_type_batiment_description=tr002_type_batiment_description,
        tr002_type_batiment_libelle=tr002_type_batiment_libelle,
        tr012_categorie_erp_code=tr012_categorie_erp_code,
        tr012_categorie_erp_categorie=tr012_categorie_erp_categorie,
        tr012_categorie_erp_groupe=tr012_categorie_erp_groupe,
        tr013_type_erp_code=tr013_type_erp_code,
        tr013_type_erp_type=tr013_type_erp_type,
    )
    return dpe


def insert_data_from_csv(file_csv: str):
    """
    insert data from a csv file in database
    :param file_csv: string
    :return: tuple(file_csv, passed, failed, insertion)
    """
    failed = 0
    passed = 0
    wrapper = CreateWrapper(user="grimalpa")
    insertion = [
        [wrapper.create_surface_habitable, "surface_habitable", 0],
        [wrapper.annee_construction, "annee construction", 0],
        [wrapper.surface_thermique_lot, "surface thermique lot", 0],
        [wrapper.shon, "shon", 0],
        [wrapper.surface_utile, "surface_utile", 0],
        [
            wrapper.surface_thermique_parties_communes,
            "surface_thermique_parties_communes",
            0,
        ],
        [wrapper.nombre_niveaux, "nombre_niveaux", 0],
        [wrapper.surface_baies_orientees_nord, "surface_baies_orientees_nord", 0],
        [
            wrapper.surface_baies_orientees_est_ouest,
            "surface_baies_orientees_est_ouest",
            0,
        ],
        [wrapper.surface_baies_orientees_sud, "surface_baies_orientees_sud", 0],
        [
            wrapper.surface_planchers_hauts_deperditifs,
            "surface_planchers_hauts_deperditifs",
            0,
        ],
        [
            wrapper.surface_planchers_bas_deperditifs,
            "surface_planchers_bas_deperditifs",
            0,
        ],
        [
            wrapper.surface_parois_verticales_opaques_deperditives,
            "surface_parois_verticales_opaques_deperditives",
            0,
        ],
        [wrapper.logement_by_space_10km, "logement by space 10km", 0],
        [wrapper.density_by_departement_10km, "table pour densité 10km", 0],
        [wrapper.logement_by_space_5km, "logement by space 5km", 0],
        [wrapper.density_by_departement_5km, "table pour densité 5km", 0],
        [
            wrapper.energie_type_logement_by_departement,
            "table type logement energie consommée",
            0,
        ],
        [wrapper.type_logement, "table type logement par classe", 0],
    ]
    for index, row in stream_csv(file_csv):
        if index == 0:
            continue
        try:
            dpe = row_to_dpe(row)
            if not skip_dpe(dpe):
                wrapper.create_dpe(dpe)
                wrapper.logement_consommation_energie(dpe)
                for i, value in enumerate(insertion):
                    if value[0](dpe):
                        insertion[i][2] += 1
                passed += 1
            else:
                failed += 1
        except ValueError:
            failed += 1
    return file_csv, passed, failed, insertion


def insert_data():
    """
    Insert Data in Cassandra
    :return:
    """
    parser = argparse.ArgumentParser(
        prog="insert", description="Insert data in the Cassandra cluster"
    )

    parser.add_argument(
        "--pattern",
        type=str,
        default="dpe-68.csv",
        help="Pattern for the CSV file to import",
    )

    args = parser.parse_args()

    data_folder = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir, "data"))

    csv_files = sorted(glob.glob(f"{data_folder}/{args.pattern}"))
    if len(csv_files) == 0:
        raise RuntimeError(
            f"No file to import. "
            f"Is the pattern correctly specified? "
            f"Current pattern: {args.pattern}"
        )

    print("The following files will be imported:")
    for csv in csv_files:
        print(csv)

    if input("Confirm with 'y': ").lower() != "y":
        print("Aborted")
        exit(0)

    res = (insert_data_from_csv(csv) for csv in csv_files)

    for csv, passed, failed, insertion in res:
        print(f"{csv} Passed: {passed}, Failed: {failed}")
        for correlation in insertion:
            print(f"{correlation[1]} Passed: {correlation[2]}")


if __name__ == "__main__":
    insert_data()

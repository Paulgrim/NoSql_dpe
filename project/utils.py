import csv
import os
from dataclasses import dataclass

from typing import List
from cassandra.cluster import Cluster

class CassandraWrapper:
    def __init__(self, user: str = None) -> None:
        self._cluster = Cluster(["00.000.000.000", "00.000.000.000"])
        keyspace = f"{user if user is not None else os.getenv('USER')}_projet"
        self._session = self._cluster.connect(keyspace=keyspace)

@dataclass(frozen=True)
class DPE:
    dpe_id: int
    departement: int
    tr001_modele_dpe_id: int
    nom_methode_dpe: str
    date_visite_diagnostiqueur: str
    date_etablissement_dpe: str
    consommation_energie: float
    classe_consommation_energie: str
    estimation_ges: float
    classe_estimation_ges: str
    secteur_activite: str
    annee_construction: int
    surface_habitable: float
    surface_thermique_lot: float
    commune: str
    arrondissement: str
    nom_rue: str
    code_postal: int
    code_insee_commune_actualise: int
    shon: float
    surface_utile: float
    surface_thermique_parties_communes: float
    en_souterrain: bool
    en_surface: bool
    nombre_niveaux: int
    surface_baies_orientees_nord: float
    surface_baies_orientees_est_ouest: float
    surface_baies_orientees_sud: float
    surface_planchers_hauts_deperditifs: float
    surface_planchers_bas_deperditifs: float
    surface_parois_verticales_opaques_deperditives: float
    organisme_certificateur: str
    longitude: float
    latitude: float
    geo_adresse: str
    tr001_modele_dpe_code: str
    tr002_type_batiment_code: str
    tr002_type_batiment_description: str
    tr002_type_batiment_libelle: str
    tr012_categorie_erp_code: str
    tr012_categorie_erp_categorie: str
    tr012_categorie_erp_groupe: str
    tr013_type_erp_code: str
    tr013_type_erp_type: str

def stream_csv(csv_file: str) -> (int, List[str]):
    """
    Return lines of CSV files one by one as a list of strings.

    :param csv_file: the path to the
    :return: an index and its row of a csv file as a list of strings
    """
    with open(csv_file, "r") as csv_f:
        reader = csv.reader(csv_f)
        for i, row in enumerate(reader):
            yield i, row


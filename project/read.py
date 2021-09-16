from typing import Generator
import pandas as pd
from utils import CassandraWrapper
import pandas as pd
from cassandra.query import tuple_factory

def pandas_factory(colnames, rows):
    """
    enable to get dataframe from the request
    used to initialize session.row_factory of an Cassandra wrapper object
    :param colnames:
    :param rows:
    :return:
    """
    return pd.DataFrame(rows, columns=colnames)


class ReadWrapper(CassandraWrapper):
    def get_correlation(self, table: str, departement: int):
        """
        Method to get elements from table used for correlation
        :param table: name of the table to get all elements
        :return: response Generator
        """
        self._session.row_factory = pandas_factory
        self._session.default_fetch_size = None
        response = self._session.execute(
            """
                SELECT *
                FROM {table}
                WHERE departement = %(departement)s;
            """.format(table=table),  # doesnt succeed with %(table)s.
            {"table": table, "departement": departement},
        )
        return response._current_rows

    def get_logement_econome(self, departement: int, classe_energetique: str) -> Generator:
        """
        Method to get the most energy-efficient homes
        :param departement: integer
        :param classe_energetique: string
        :return: response Generator
        """
        self._session.row_factory = tuple_factory
        self._session.default_fetch_size = 5000
        response = self._session.execute(
            """
                SELECT
                departement,
                classe_consommation_energie,
                consommation_energie,
                annee_construction,
                commune,
                nom_rue,
                code_postal,
                tr002_type_batiment_description,
                longitude,
                latitude
                FROM logement_by_consommation_energetique
                WHERE departement = %(departement)s
                AND classe_consommation_energie = %(classe_energetique)s;
            """,
            {"departement": departement, "classe_energetique": classe_energetique},
        )
        for row in response:
            yield row

    def get_density_10km(self, departement: int) -> Generator:
        """
        Method to get houses along x and y
        :param departement: integer
        :return: response Generator
        """
        self._session.row_factory = pandas_factory
        self._session.default_fetch_size = None
        response = self._session.execute(
            """
                SELECT
                departement,
                x,
                y,
                nbLogement
                FROM density_by_departement
                WHERE departement = %(departement)s;
            """,
            {"departement": departement},
        )
        return response._current_rows

    def get_lien_energie_10km(self, departement: int, x: int, y: int):
        """
        Method to get energie consumption along x and y
        :param departement: integer
        :param x: integer
        :param y: integer
        :return: energie consumption
        """
        # pour chaque x et y je dois récupérer la somme
        # d'énergie et ajouter dans le data frame
        self._session.row_factory = tuple_factory
        self._session.default_fetch_size = 5000
        response = self._session.execute(
            """
                SELECT
                SUM(consommation_energie) as conso
                FROM logement_by_space_and_departement
                WHERE departement = %(departement)s
                AND x = %(x)s
                AND y = %(y)s;
            """,
            {"departement": departement, "x": x, "y": y},
        )
        return response.one()[0]

    def get_density_5km(self, departement: int) -> Generator:
        """
        Method to get houses along x and y
        :param departement: integer
        :return: response Generator
        """
        self._session.row_factory = pandas_factory
        self._session.default_fetch_size = None
        response = self._session.execute(
            """
                SELECT
                departement,
                x,
                y,
                nbLogement
                FROM density_by_departement_5km
                WHERE departement = %(departement)s;
            """,
            {"departement": departement},
        )
        return response._current_rows

    def get_lien_energie_5km(self, departement: int, x: int, y: int):
        """
        Method to get energie consumption along x and y

        :param departement: integer
        :param x: integer
        :param y: integer
        :return: energie consumption
        """
        # pour chaque x et y je dois récupérer la somme
        # d'énergie et ajouter dans le data frame
        self._session.row_factory = tuple_factory
        self._session.default_fetch_size = 5000
        response = self._session.execute(
            """
                SELECT
                SUM(consommation_energie) as conso
                FROM logement_by_space_and_departement_5km
                WHERE departement = %(departement)s
                AND x = %(x)s
                AND y = %(y)s;
            """,
            {"departement": departement, "x": x, "y": y},
        )
        return response.one()[0]

    def get_distribution(self, departement: int):
        """
        Method to get distribution
        :param departement:
        :return:
        """
        classe_energetique_ges = ["A", "B", "C", "D", "E", "F", "G"]
        distribution = list()
        self._session.row_factory = tuple_factory
        self._session.default_fetch_size = 5000
        for classe in classe_energetique_ges:
            response = self._session.execute(
                """
                    SELECT
                    COUNT(*) as number
                    FROM logement_by_consommation_energetique
                    WHERE departement = %(departement)s
                    AND classe_consommation_energie = %(classe_conso)s;
                """,
                {"departement": departement, "classe_conso": classe},
            )
            distribution.append(response.one()[0])
        return pd.DataFrame(
            {"classe": classe_energetique_ges,
             "effectif": distribution})

    def get_logement_by_type(self, departement: int):
        """
        get homes by energy classe
        :param departement: integer
        :return:
        """
        self._session.row_factory = pandas_factory
        self._session.default_fetch_size = None
        response = self._session.execute(
            """
                SELECT
                tr002_type_batiment_description as type,
                nbLogement,
                classe_consommation_energie
                FROM type_logement_by_departement
                WHERE departement = %(departement)s;
            """,
            {"departement": departement},
        )
        return response._current_rows

    def get_logement_consumption_type(self, departement: int):
        """
        get homes with their consumption
        :param departement: integer
        :return:
        """
        type_batiment = [
            "Bâtiment collectif à usage principal d'habitation",
            "Logement",
            "Maison Individuelle"
        ]
        consommation = list()
        self._session.row_factory = tuple_factory
        self._session.default_fetch_size = 5000
        for type in type_batiment:
            response = self._session.execute(
                """
                    SELECT
                    SUM(consommation_energie)
                    FROM energie_type_logement_by_departement
                    WHERE departement = %(departement)s
                    AND tr002_type_batiment_description = %(type)s;
                    """,
                {
                    "departement": departement,
                    "type": type
                },
            )
            consommation.append(response.one()[0])
        return pd.DataFrame({
            "type": type_batiment,
            "conso": consommation}
        )

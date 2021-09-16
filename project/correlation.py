from read import ReadWrapper


correlation = [
    "surface_habitable_by_departement",
    "annee_construction_by_departement",
    "surface_thermique_lot_by_departement",
    "shon_by_departement",
    "surface_utile_by_departement",
    "surfThermiquePartiesCommunes_by_departement",
    "nombre_niveaux",
    "surface_baies_orientees_nord_by_departement",
    "surface_baies_orientees_est_ouest_by_departement",
    "surface_baies_orientees_sud_by_departement",
    "surfPlanchersHautsDeperd_by_departement",
    "surfPlanchersBasDeperd_by_departement",
    "surfParoisVertOpaquesDeperd_by_departement"
]

method_corr = [
    "pearson",
    "kendall",
    "spearman"
]

if __name__ == "__main__":

    wrapper = ReadWrapper()

    for table in correlation:
        df = wrapper.get_correlation(table, 68)
        df = df.drop(
            columns=[
                "dpe_id",
                "departement",
                "classe_consommation_energie",
                "classe_estimation_ges"
            ])
        print(f"----------{table}-------------")
        for method in method_corr:
            print(f"ooooo data ooooo method {method} oooooo")
            print(df.corr(method=method))

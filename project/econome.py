from read import ReadWrapper
import pandas as pd

if __name__ == "__main__":

    wrapper = ReadWrapper()

    gen = wrapper.get_logement_econome(departement=68, classe_energetique="A")
    print("----Les 5 logements les plus économes du Haut rhin-------")
    cpt = 0
    infoMaison = []
    for _, _, dpe, annee, ville, adresse, code_postale, type_maison, lo, lat in gen:
        if dpe <= 0:
            continue
        else:
            cpt += 1
            print(f"consomation énergétique {dpe} construit l'année {annee}"
                  f", adresse : {adresse}, {ville}, {code_postale},"
                  f"type de batiment {type_maison}\n")
            infoMaison.append((dpe, lo, lat, adresse, ville, code_postale, type_maison))
            if cpt == 5:
                break
    df_maison = pd.DataFrame(infoMaison, columns=["dpe",
                                                  "longitude",
                                                  "latitude",
                                                  "adresse",
                                                  "ville",
                                                  "code_postale",
                                                  "type_maison"])
    df_maison.to_csv("data/infoMaison.csv")

    logement_classe = wrapper.get_logement_by_type(departement=68)
    # print(logement_classe)
    logement_classe.to_csv("data/type_by_classe.csv")
    consommation_type = wrapper.get_logement_consumption_type(departement=68)
    # print(consommation_type)
    consommation_type.to_csv("data/consommation_by_type.csv")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from read import ReadWrapper


if __name__ == "__main__":

    wrapper = ReadWrapper()

    distribution = wrapper.get_distribution(68)
    print(distribution)
    distribution.to_csv("data/distribution.csv")

    distrib = pd.read_csv("data/distribution.csv", index_col=0)
    sns.barplot(x="classe", y="effectif", data=distrib, color="green")
    plt.savefig("data/distribution.pdf")

    type_batiment = [
        "Bâtiment collectif à usage principal d'habitation",
        "Logement",
        "Maison Individuelle",
    ]

    type_classe = pd.read_csv("data/type_by_classe.csv", index_col=0)
    consommation_type = pd.read_csv("data/consommation_by_type.csv", index_col=0)
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x="type",
        y="nblogement",
        hue="classe_consommation_energie",
        data=type_classe,
        color="green",
    )
    plt.savefig("data/distribution_conso.pdf")

    log = [
        type_classe.loc[type_classe.type == log].nblogement.sum()
        for log in type_batiment
    ]
    consommation_type["moyenne"] = consommation_type.conso / log
    plt.figure(figsize=(10, 6))

    sns.barplot(x="type", y="moyenne", data=consommation_type, color="green")
    plt.savefig("data/moyenne_conso.pdf")

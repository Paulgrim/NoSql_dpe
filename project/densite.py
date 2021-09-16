from read import ReadWrapper
from convertorDistance import createGeospace

if __name__ == '__main__':

    wrapper = ReadWrapper()
    method_corr = [
        "pearson",
        "kendall",
        "spearman"
    ]
    print("-------------------100km²-----------------")
    density = wrapper.get_density_10km(departement=68)
    density = density.drop(columns=["departement"])
    energie = list()

    for row in density.itertuples():
        energie.append(
            wrapper.get_lien_energie_10km(departement=68, x=row.x, y=row.y)
        )
    density["energie"] = energie
    # calcul de la densite de logement sur 100km²
    density["densite"] = density.nblogement / 100
    density["MoyenneEnergieZone"] = density.energie / density.nblogement
    density["value"] = density.x.apply(str) + density.y.apply(str)
    density.to_csv("data/densite10km.csv")
    createGeospace(density, 10)
    for m in method_corr:
        print(density.drop(columns=["x", "y", "nblogement", "energie"]).corr(method = m))

    print("-------------------25km²-----------------")
    density = wrapper.get_density_5km(departement=68)
    density = density.drop(columns=["departement"])
    energie = list()

    for row in density.itertuples():
        energie.append(
            wrapper.get_lien_energie_5km(departement=68, x=row.x, y=row.y)
        )
    density["energie"] = energie
    # calcul de la densite de logement sur 25km²
    density["densite"] = density.nblogement / 25
    density["MoyenneEnergieZone"] = density.energie / density.nblogement
    density["value"] = density.x.apply(str) + density.y.apply(str)
    density.to_csv("data/densite5km.csv")
    createGeospace(density, 5)
    for m in method_corr:
        print(density.drop(columns=["x", "y", "nblogement", "energie"]).corr(method=m))


#_*_ coding: utf-8 _*_

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats.kde import gaussian_kde
from scipy.stats import norm
from Test_MariaDB import WrapDB

if __name__ == '__main__':

    # Wrap운용팀 DB Connect
    db = WrapDB()
    db.connet(host="127.0.0.1", port=3306, database="WrapDB_2", user="root", password="maria")
    data_infos = db.get_data_info()

    for nm in data_infos.columns:
        data = db.get_data()


    plt.style.use("ggplot")

    d1 = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.1, 0.2, 0.3, 0.4, 0.1, 0.2, 0.3, 0.4, 0.3, 0.4, 0.3, 0.4, 0.4]
    d1_np = np.array(d1)

    # Estimating the pdf and plotting
    KDEpdf = gaussian_kde(d1_np)
    x = np.linspace(-1.5, 1.5, 1500)

    plt.plot(x, KDEpdf(x), 'r', label="KDE estimation", color="blue")
    plt.hist(d1_np, normed=1, color="cyan", alpha=.8)
    # plt.plot(x, norm.pdf(x, 0, 0.1), label="parametric distribution", color="red")
    plt.legend()
    plt.title("Returns: parametric and estimated pdf")
    plt.show()















    db.disconnect()



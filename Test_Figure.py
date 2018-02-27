#_*_ coding: utf-8 _*_

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def Figure_2D(data_frame, title='2 component', input_name = None, output_name = None, target_name = None, target_color = None):

    if data_frame is None:
        return False

    fig = plt.figure(figsize=(8, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlabel(input_name[0], fontsize=15)
    ax.set_ylabel(input_name[1], fontsize=15)
    ax.set_title(title, fontsize=20)
    targets = target_name
    colors = target_color
    for target, color in zip(targets, colors):
        indicesToKeep = data_frame[output_name] == target
        ax.scatter(data_frame.loc[indicesToKeep, input_name[0]], data_frame.loc[indicesToKeep, input_name[1]], c=color, s=50)
    ax.legend(targets)
    ax.grid()


def Figure_3D(data_frame, title='3 component', input_name = None, output_name = None, target_name = None, target_color = None):

    if data_frame is None:
        return False

    centers = [[1, 1], [-1, -1], [1, -1]]

    fig = plt.figure(1, figsize=(4, 3))
    plt.clf()
    ax = Axes3D(fig, rect=[0, 0, .95, 1], elev=48, azim=134)
    plt.cla()
    '''
    for name, label in [('Setosa', 0), ('Versicolour', 1), ('Virginica', 2)]:
        ax.text3D(X[y == label, 0].mean(), X[y == label, 1].mean() + 1.5, X[y == label, 2].mean(), name, horizontalalignment='center', bbox=dict(alpha=.5, edgecolor='w', facecolor='w'))
    # Reorder the labels to have colors matching the cluster results
    y = np.choose(y, [1, 2, 0]).astype(np.float)
    ax.scatter(X[:, 0], X[:, 1], X[:, 2], c=y, cmap=plt.cm.spectral, edgecolor='k')

    ax.w_xaxis.set_ticklabels([])
    ax.w_yaxis.set_ticklabels([])
    ax.w_zaxis.set_ticklabels([])
    '''

    plt.show()


#_*_ coding: utf-8 _*_

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import random
import platform
import matplotlib.font_manager as fm
import numpy as np

if platform.system() == 'Windows':
    font_path = 'C:/Windows/Boot/Fonts/malgun_boot.ttf'
    fontprop = fm.FontProperties(fname=font_path, size=18)

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


def Figure_2D_NoClass(data_frame, title='2 component', axis_name = None, color = None):

    if data_frame is None:
        return False

    fig = plt.figure(figsize=(8, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlabel(axis_name[0], fontsize=15)
    ax.set_ylabel(axis_name[1], fontsize=15)
    ax.set_title(title, fontsize=20)
    for data in data_frame.values:
        ax.scatter(data[0], data[1], c=color, s=50)
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

class Figure(object):

    def __init__(self):
        self.data = None
        self.figsize = None

    def draw_multi(self, data=None, title="", subplot_nm="", figsize=(10, 10), figshape=(1,1)):
        self.data = data
        if self.data is None:
            return False

        self.figsize = figsize

        fig, ax = plt.subplots(nrows=figshape[0], ncols=figshape[1])
        bx = np.ndarray(shape=(figshape[0],figshape[1]), dtype=object)
        for row_idx in range(figshape[0]):
            for column_idx in range(figshape[1]):

                idx = row_idx*figshape[0]+column_idx
                if idx >= len(data.columns)-1:
                    continue

                ax[row_idx][column_idx].set_title(data.columns[idx], fontproperties=fontprop)
                bx[row_idx][column_idx] = ax[row_idx][column_idx].twinx()

                self.data[data.columns[idx]].plot(ax=ax[row_idx][column_idx], figsize=self.figsize, color='k')
                self.data[subplot_nm].plot(ax=bx[row_idx][column_idx], figsize=self.figsize, kind='bar', position=0, width=1, color='r', alpha=0.3)


        plt.suptitle(subplot_nm, fontproperties=fontprop)
        fig.tight_layout()
        plt.show()

    def draw_multi2(self, data=None, check_data1=None, check_data2=None, title="", subplot_nm="", figsize=(10, 10), figshape=(1,1), img_save='n'):
        self.data = data
        if self.data is None:
            return False

        self.figsize = figsize

        fig, ax = plt.subplots(nrows=figshape[0], ncols=figshape[1])
        bx = np.ndarray(shape=(figshape[0],figshape[1]), dtype=object)
        cx = np.ndarray(shape=(figshape[0], figshape[1]), dtype=object)
        for row_idx in range(figshape[0]):
            for column_idx in range(figshape[1]):

                idx = row_idx*figshape[0]+column_idx
                if idx >= len(data.columns)-1:
                    continue

                ax[row_idx][column_idx].set_title(data.columns[idx], fontproperties=fontprop)
                bx[row_idx][column_idx] = ax[row_idx][column_idx].twinx()
                cx[row_idx][column_idx] = ax[row_idx][column_idx].twinx()

                self.data[data.columns[idx]].plot(ax=ax[row_idx][column_idx], figsize=self.figsize, color='k')
                check_data1[data.columns[idx]].plot(ax=bx[row_idx][column_idx], figsize=self.figsize, kind='bar', position=0, width=1, color='r', alpha=0.3)
                check_data2[data.columns[idx]].plot(ax=cx[row_idx][column_idx], figsize=self.figsize, kind='bar', position=0, width=1, color='b', alpha=0.3)

        plt.suptitle(subplot_nm, fontproperties=fontprop)
        fig.tight_layout()
        if img_save == 'y':
            plt.savefig('%s_momentum_triger.png' % (subplot_nm))
        else:
            plt.show()


    def draw(self, data=None, title="", subplots=None, figsize=(10,10)):
        self.data = data
        if self.data is None:
            return False

        self.figsize = figsize

        ax = {}
        fig, ax['base'] = plt.subplots(nrows=2, ncols=1)
        ax['base'].set_ylabel('base', fontproperties=fontprop)
        for idx, subplot_nm in enumerate(subplots):
            ax[subplot_nm] = ax['base'].twinx()
            ax[subplot_nm].set_ylabel(subplot_nm, fontproperties=fontprop)

            if idx > 0:
                rspine = ax[subplot_nm].spines['right']
                rspine.set_position(('axes', 1.05 * idx))
                ax[subplot_nm].set_frame_on(True)
                ax[subplot_nm].patch.set_visible(False)
                fig.subplots_adjust(right=0.75 * idx)

        #self.data.plot(figsize=(20, 10))
        for cd in self.data:
            if cd not in subplots:
                self.data[cd].plot(ax=ax['base'], figsize=self.figsize, kind='bar')
            else:
                for subplot_nm in subplots:
                    if cd == subplot_nm:
                        r = lambda: random.randint(0, 255)
                        self.data[cd].plot(ax=ax[cd], figsize=self.figsize, style=('#%02X%02X%02X' % (r(), r(), r())))

        plt.title(title, fontproperties=fontprop)
        plt.show()

#_*_ coding: utf-8 _*_

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mpl_toolkits.mplot3d import Axes3D
import random
import platform
import matplotlib.font_manager as fm
import numpy as np

if platform.system() == 'Windows':
    font_path = 'C:/Windows/Boot/Fonts/malgun_boot.ttf'
    fontprop = fm.FontProperties(fname=font_path, size=15)
else:
    path = '/Library/Fonts/Arial Unicode.ttf'
    fontprop = fm.FontProperties(fname=path, size=15)

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
        print("### Make Panel ###")

    def draw_multi_graph_with_matching_analysis(self, data=None, analysis=None, anal_value=None, title="", figsize=(10, 10), figshape=(1,1), img_save='n'):

        color_list = ('g', 'r', 'b', 'y')

        if data is None or analysis is None:
            return False

        panel, data_subs = plt.subplots(nrows=figshape[0], ncols=figshape[1], figsize=figsize, squeeze=False, constrained_layout=True)
        #panel.subplots_adjust(top=0.9, wspace=0.3)
        #panel.tight_layout()

        plt.suptitle(title, fontproperties=fontprop)

        analysis_subs = [np.ndarray(shape=(figshape[0],figshape[1]), dtype=object) for anal in analysis]
        for row_idx in range(figshape[0]):
            for column_idx in range(figshape[1]):

                idx = row_idx*figshape[1]+column_idx
                if idx >= len(data.columns):
                    continue

                data_subs[row_idx][column_idx].set_title(data.columns[idx], fontproperties=fontprop)
                #data_subs[row_idx][column_idx].axes.get_xaxis().set_visible(False)
                if anal_value is not None:
                    data_subs[row_idx][column_idx].set_title(data.columns[idx]+'('+str(round(anal_value[idx]*100, 2))+'%)', fontproperties=fontprop)

                for analysis_sub in analysis_subs:
                    analysis_sub[row_idx][column_idx] = data_subs[row_idx][column_idx].twinx()
                    #analysis_sub[row_idx][column_idx].axes.get_xaxis().set_visible(False)

                data[data.columns[idx]].plot(ax=data_subs[row_idx][column_idx], color='k')
                for color_idx, (anal, alaysis_sub) in enumerate(zip(analysis, analysis_subs)):
                    anal[data.columns[idx]].plot(ax=alaysis_sub[row_idx][column_idx], kind='bar', position=1, width=1, color=color_list[color_idx%len(color_list)], alpha=0.3, ylim=(0,1))

        if img_save == 'y':
            plt.savefig('%s_momentum_triger.png' % (title))
        else:
            plt.show()


    def draw(self, data=None, title="", subplots=None, figsize=(10,10)):
        if data is None:
            return False

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
        for cd in data:
            if cd not in subplots:
                data[cd].plot(ax=ax['base'], figsize=figsize, kind='bar')
            else:
                for subplot_nm in subplots:
                    if cd == subplot_nm:
                        r = lambda: random.randint(0, 255)
                        data[cd].plot(ax=ax[cd], figsize=figsize, style=('#%02X%02X%02X' % (r(), r(), r())))

        plt.title(title, fontproperties=fontprop)
        plt.show()

#
# dashboard_plot_utils
#
# Utility functions for plotting
#
import os, sys, subprocess, json, random
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import uuid
import dash
from dash import dash_table
from dash import dcc
from dash import html

############################################################
## CONSTANTS USED BY PLOTS
############################################################
PLOT_HEIGHT_DEFAULT = 800

############################################################
## PLOT UTILITY FUNCTIONS
############################################################
def addGraph( fig_object, fig_id = 'fig-'+str(uuid.uuid4())[0:6], options = {} ):
    if 'style' in options:
        return dcc.Graph(id=fig_id, figure=fig_object, style=options['style'])
    else:
        return dcc.Graph(id=fig_id, figure=fig_object)


############################################################
## Plot class for creating Plot objects.
##
## Plot objects are useful if you want to re-use the same graph properties on multiple plots, or if you want to have multiple plots overlayed on the same figure.
############################################################
class Plot:
    """ Class definition for all plots. Provides an easy structured way to define all options for plots.
    """
    def __init__(self, plot_type, x, y, xlabel = 'x', ylabel = 'y', title = ''):
        """ x and y must be lists or numpy arrays
        """
        self.plot_type = plot_type
        self.x = x
        self.y = y
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.title = title
        self.xscale = 'linear'
        self.yscale = 'linear'
        self.xrange = (min(x), max(x))
        self.yrange = (min(y), max(y)) if y != [] else (0, 0)
        self.plot_df = pd.DataFrame()
        self.options = {}
        self.figure_object = None

    def createDataFrame(self):
        """ Creates a dataframe out of x and y, using xlabel and ylabel as column labels
        """
        self.plot_df = pd.DataFrame(list(zip(self.x, self.y)), columns=[self.xlabel, self.ylabel])
        return self.plot_df

    def addOptions(self, options_json):
        """ Given a dictionary of options (JSON), adds these to self.options property.
        This can also be used to overwrite previous options.

        options_json: DICT - e.g., {'colorscale': 'greys', 'showlegend': 'false'}
        """
        for k, v in options_json.items():
            self.options[k] = v

    def setXScale(self, scale_type):
        """ Sets scale type for x-axis.

        scale_type: STRING - common types are 'linear', 'log'
        """
        self.xscale = str(scale_type).lower()

    def setYScale(self, scale_type):
        """ Sets scale type for y-axis.

        scale_type: STRING - common types are 'linear', 'log'
        """
        self.yscale = str(scale_type).lower()

    def setXRange(self, _xmin, _xmax = 0):
        """ Sets min and max x-range.

        _xmin: FLOAT
        _xmax: FLOAT
        """
        if type(_xmin) == type((1,2)):
            self.xrange = _xmin
        elif type(_xmin) == type([1,2]):
            self.xrange = list(_xmin)
        else:
            self.xrange = (_xmin, _xmax)

    def setYRange(self, _ymin, _ymax = 0):
        """ Sets min and max y-range.

        _ymin: FLOAT
        _ymax: FLOAT
        """
        if type(_ymin) == type((1,2)):
            self.yrange = _ymin
        elif type(_ymin) == type([1,2]):
            self.yrange = list(_ymin)
        else:
            self.yrange = (_ymin, _ymax)

    def setTitle(self, _title):
        """ Sets plot title
        """
        self.title = _title

    def setXLabel(self, _xlabel):
        """ Sets plot x-label
        """
        self.xlabel = _xlabel

    def setYLabel(self, _ylabel):
        """ Sets plot y-label
        """
        self.ylabel = _ylabel

    def setPlotType(self, _pt):
        """ Sets plot type
        """
        self.plot_type = _pt

    def setFigureObject(self, _fo):
        """ Sets plot (figure) object
        """
        self.figure_object = _fo

    def setFigureLayout_default(self):
        """ Sets figure object to set a default layout.
        """
        return updateFigureLayout_default()

    # getters
    def getDataFrame(self):
        return self.plot_df

    def getOptions(self):
        return self.options

    def getX(self):
        return self.x

    def getY(self):
        return self.y

    def getXScale(self):
        return self.xscale

    def getYScale(self):
        return self.yscale

    def getXRange(self):
        return self.xrange

    def getYRange(self):
        return self.yrange

    def getTitle(self):
        return self.title

    def getXLabel(self):
        return self.xlabel

    def getYLabel(self):
        return self.ylabel

    def getPlotType(self):
        return self.plot_type

    def getFigureObject(self):
        """ Sets graphical figure object
        """
        return self.figure_object

    # update functions
    def updateFigureLayout_default(self):
        """ Updates figure object to set a default layout.
        """
        if self.figure_object != None:
            self.figure_object.update_layout(transition_duration=250, xaxis_tickangle=-90,uniformtext_minsize=8, uniformtext_mode='hide', title_x=0.5)
        return self.figure_object



def createPlotObject( plot_type, x, y, xlabel = 'x', ylabel = 'y', title = ''):
    """ Creates a Plot class object
    """
    return Plot(plot_type, x, y, xlabel, ylabel, title)


############################################################
## Plot functions
############################################################
def plotScatter( x, y, xlabel = 'x', ylabel = 'y', title = '', options = {}):
    """ Creates a scatter plot from raw graph properties.
    """
    p = createPlotObject('scatter', x, y, xlabel, ylabel, title)
    return plotScatterObject( p, options )

def plotScatterObject( p, options ):
    """ Creates a scatter plot from a Plot object.
    """
    p.addOptions(options)
    p.setFigureObject(px.scatter(x=p.getX(), y=p.getY(), title=p.getTitle(), height=PLOT_HEIGHT_DEFAULT))
    p.updateFigureLayout_default()
    return p


def plotBar( x, y, xlabel = 'x', ylabel = 'y', title = '', options = {}):
    """ Creates a bar plot from raw graph properties.
    """
    p = createPlotObject('bar', x, y, xlabel, ylabel, title)
    return plotBarObject( p, options )

def plotBarObject( p, options ):
    """ Creates a bar plot from a Plot object.
    """
    p.addOptions(options)
    p.createDataFrame()
    p.setFigureObject(px.bar(p.getDataFrame(), x=p.getXLabel(), y=p.getYLabel(), title=p.getTitle(), height=PLOT_HEIGHT_DEFAULT))
    p.updateFigureLayout_default()
    return p


def plotHist( x, xlabel = 'x', ylabel = 'y', title = '', options = {}):
    """ Alias for plotHistogram()
    """
    return plotHistogram( x, xlabel, ylabel, title, options)

def plotHistogram( x, xlabel = 'x', ylabel = 'y', title = '', options = {}):
    """ Creates a histogram plot from raw graph properties.
    """
    p = createPlotObject('hist', x, [], xlabel, ylabel, title)
    return plotHistogramObject( p, options )

def plotHistogramObject( p, options ):
    """ Creates a histogram plot from a Plot object.
    """
    p.addOptions(options)
    # if p.getYScale() == 'log'
    # px.histogram(p.getX(), nbins=100, title=p.getTitle(), log_y=True)
    p_plot = go.Figure(go.Histogram(x=p.getX()))
    p_plot.update_layout( title_text=p.getTitle(), title_x=0.5, xaxis_title_text=p.getXLabel(), yaxis_title_text=p.getYLabel())
    p.setFigureObject(p_plot)
    return p


def plotViolin( x, y, xlabel = 'x', ylabel = 'y', title = '', options = {}):
    """ Creates a violin plot from raw graph properties.
    """
    p = createPlotObject('violin', x, y, xlabel, ylabel, title)
    return plotViolinObject( p, options )

def plotViolinObject( p, options ):
    """ Creates a violin plot from a Plot object.
    """
    p.addOptions(options)
    p.createDataFrame()
    p.setFigureObject(px.violin(x=p.getX(), y=p.getY(), title=p.getTitle(), box=True, height=PLOT_HEIGHT_DEFAULT))
    p.updateFigureLayout_default()
    return p


# empty placeholder plots
def plotScatterEmpty():
    return px.scatter()

def plotBarEmpty():
    return px.bar()

def plotHistogramEmpty():
    return px.histogram()

def plotViolinEmpty():
    return px.violin()

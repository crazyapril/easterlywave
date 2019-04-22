import numpy as np

from model.registry import register
from tools.metplot.plotplus import Plot

scope = __name__


@register(model='ecmwf', params=('500:h', '850:t', '850:u', '850:v'), code='GPT',
    category='upper air', name='850hPa Temp & Wind', regions=['asia', 'china'],
    scope=scope)
def plot_gpt(session):
    geopo = session.get('500:h')
    temp = session.get('850:t') - 273.15
    u = session.get('850:u')
    v = session.get('850:v')
    if session.region.kwargs.get('proj', None) is None:
        # PlateCarree projection
        aspect = 'cos'
    else:
        aspect = None
    p = Plot(aspect=aspect)
    p.usemapset(session.get_mapset())
    p.setxy(session.georange, session.resolution)
    p.draw('coastline country parameri')
    p.contour(geopo, levels=np.arange(4950, 5910, 30), color='#300060', lw=0.3,
        vline=5880, vlinedict={'color':'r', 'lw':1})
    p.contour(temp, levels=np.arange(-45,45,5), color='#882205', lw=0.1, clabel=False)
    p.contourf(temp, gpfcmap='temp2', cbar=True, vline=0, vlinedict={'linewidths':1})
    p.quiver(u, v, color='w')
    p.title('ECMWF 850mb Temperature & 500mb Geopotential')
    p.timestamp(session.basetime, session.fcsthour)
    p.save(session.target_path)
    p.clear()

import numpy as np

from model.registry import register
from tools.metplot.plotplus import Plot

scope = __name__


@register(model='ecmwf', params=('500:h', '850:t', '850:u', '850:v'), code='GPT',
    category='upper air', name='850hPa Temp & 500hPa Height',
    regions=['Asia', 'Japan & Korea', '*china'], scope=scope)
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
    if 'China' in session.region.key:
        p.draw('province')
    p.contour(geopo, levels=np.arange(4950, 5910, 30), color='#300060', lw=0.3,
        vline=5880, vlinedict={'color':'r', 'lw':1})
    p.contour(temp, levels=np.arange(-45,45,5), color='#882205', lw=0.1, clabel=False)
    p.contourf(temp, gpfcmap='temp2', cbar=True, vline=0, vlinedict={'linewidths':1})
    p.quiver(u, v, color='w')
    p.title('ECMWF 850mb Temperature (shaded), Wind (vector) & '
        '500mb Geopotential Height (contour)')
    p.timestamp(session.basetime, session.fcsthour)
    p.save(session.target_path)
    p.clear()

@register(model='ecmwf', params=('500:h', 'msl:p'), code='GHP',
    category='upprt air', name='500hPa Height & MSLP',
    regions=['*tropics'], scope=scope)
def plot_ghw(session):
    geopo = session.get('500:h')
    mslp = session.get('msl:p') / 100.
    p = Plot(aspect='cos')
    p.usemapset(session.get_mapset())
    p.setxy(session.georange, session.resolution)
    p.draw('coastline country parameri')
    p.contour(mslp, levels=np.arange(940,1060,2), lw=0.3,
        clabeldict={'fontsize':4}, ip=2)
    p.contourf(geopo, gpfcmap='geopo', cbar=True)
    p.maxminfilter(mslp, type='max', stroke=True, marktext=True,
        marktextdict={'mark':'H'}, color='b', vmin=1015, window=30, zorder=3)
    p.maxminfilter(mslp, type='min', stroke=True, marktext=True,
        marktextdict={'mark':'L'}, color='r', vmax=1008, window=30, zorder=3)
    p.maxminnote(mslp, type='min', fmt='{:.1f}', unit='hPa', name='MSLP')
    p.title('ECMWF 500mb Geopotential Height (shaded) & MSLP (contour, extrema)')
    p.timestamp(session.basetime, session.fcsthour)
    p.save(session.target_path)
    p.clear()

@register(model='ecmwf', params=('850:u', '850:v', 'msl:p'), code='WNP',
    category='upprt air', name='850 hPa Wind & MSLP',
    regions=['*tropics', '*china'], scope=scope)
def plot_wnp(session):
    u = session.get('850:u') * 1.94
    v = session.get('850:v') * 1.94
    mslp = session.get('msl:p') / 100
    wind = np.hypot(u, v)
    p = Plot(aspect='cos')
    p.usemapset(session.get_mapset())
    p.setxy(session.georange, session.resolution)
    p.draw('coastline country parameri')
    if 'China' in session.region.key:
        p.draw('province')
    p.contourf(wind, gpfcmap='wind', cbar=True)
    p.contour(mslp, levels=np.arange(940,1060,2), color='k', lw=0.2, ip=2)
    p.maxminfilter(mslp, type='min', marktext=True, vmax=1008, window=30,
        marktextdict=dict(mark='L', color='r'), stroke=True, zorder=3)
    p.maxminfilter(mslp, type='max', marktext=True, vmin=1015, window=30,
        marktextdict=dict(mark='H', color='b'), stroke=True, zorder=3)
    p.barbs(u, v, num=20, lw=0.2, color='w')
    p.maxminnote(mslp, type='min', fmt='{:.1f}', unit='hPa', name='MSLP')
    p.maxminnote(wind, type='max', fmt='{:.1f}', unit='kt', name='Wind')
    p.title('ECMWF 850mb Wind (shaded, barbs) & MSLP (contour, extrema)')
    p.timestamp(session.basetime, session.fcsthour)
    p.save(session.target_path)
    p.clear()

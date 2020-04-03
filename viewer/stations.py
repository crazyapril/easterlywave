from viewer.models import Station, StationRecord, StationClimate


RECORD_ITEMS = {
    'tmax': '最高气温 (℃)',
    'tmin': '最低气温 (℃)',
    'precip08': '日降水量08 (mm)',
    'precip20': '日降水量20 (mm)',
    'hightavg': '最高日均温 (℃)',
    'lowtavg': '最低日均温 (℃)',
    'hightmin': '最高日低温 (℃)',
    'lowtmax': '最低日高温 (℃)'
}

CLIMATE_ITEMS = {
    'tmax': '月均高温 (℃)',
    'tmin': '月均低温 (℃)',
    'precip': '月降水量 (mm)'
}


def get_station_by_name_or_code(name_or_code):
    if name_or_code[0].isdigit():
        try:
            station = Station.objects.get(code=name_or_code)
        except Station.DoesNotExist:
            station = None
    else:
        try:
            station = Station.objects.get(name=name_or_code)
        except Station.DoesNotExist:
            station = None
    return station


def get_station_record_as_json(station):
    json = {
        'valid': True,
        'data': []
    }
    if station is None:
        json['valid'] = False
        return json
    records = StationRecord.objects.filter(code=station.code)
    if records.count() == 0:
        json['valid'] = False
        return json
    for item in RECORD_ITEMS:
        itemdata = {
            'item': RECORD_ITEMS[item],
            'data': []
        }
        item_records = records.filter(item=item, valid=True).order_by('rank')
        for record in item_records:
            recorddata = {
                'rank': record.rank,
                'value': '{:.1f}'.format(record.value),
                'date': record.date.strftime('%Y/%m/%d')
            }
            itemdata['data'].append(recorddata)
        json['data'].append(itemdata)
    return json

def get_station_climate_as_json(station):
    json = {
        'valid': True,
        'data': []
    }
    if station is None:
        json['valid'] = False
        return json
    records = StationClimate.objects.filter(code=station.code).order_by('month')
    if records.count() == 0:
        json['valid'] = False
        return json
    for item in CLIMATE_ITEMS:
        json['data'].append({
            'item': CLIMATE_ITEMS[item],
            'values': ['{:.1f}'.format(v) for v in records.values_list(item, flat=True)]
        })
    return json

import datetime
import logging
from typing import Dict, List, Optional, Type, Union

from tools.typhoon import Storm

logger = logging.getLogger(__name__)


class DiagnosisSource:

    name = None
    full_name = None
    values = {} # type: Dict[str, Type[DiagnosisSource]]

    def __init__(self, code, storm: Storm):
        self.code = code
        self.storm = storm
        self.last_updated = None
        self.data_time = None
        self.loaded = False

    @classmethod
    def prefetch(cls) -> None:
        pass

    @classmethod
    def get(cls, storm: Storm) -> Optional[Type['DiagnosisSource']]:
        logging.info('Getting %s source for %s', cls.name, storm.code)
        if storm.code in cls.values:
            return cls.values[storm.code]
        source = cls(storm.code, storm)
        try:
            source.fetch()
            if source.loaded:
                return source
            else:
                logging.info('No %s source found for %s', cls.name, storm.code)
                return None
        except:
            logging.exception('Fail to get %s source for %s', cls.name, storm.code)
            return None

    @classmethod
    def should_update(cls, nowtime: datetime.datetime) -> bool:
        return True

    def fetch(self):
        pass

    def represent(self) -> Union[str, List[str], None]:
        return None

    def _export_mpl(self, position) -> None:
        pass


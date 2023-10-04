from .aloe import AloeParser
from .analitfarm import AnalitFarmParser
from .artes import ArtesParser
from .apteka_mos import AptekamosParser
from .cva import CvaParser
from .farmeconom import FarmeconomParser
from .garmoniya import GarmoniyaParser
from .okapteka import OkaptekaParser
from .planetazd import PlanetazdParser
from .vapteke import VaptekeParser
from .zdravservis import ZdravserviceParser

from .apteka36_6 import Apteka366Parser
from .aptekaforte import AptekaforteParser
from .asnaru import AsnaruParser
from .eapteka import EaptekaParser
from .farmiya import FarmiyaParser
from .mailru import MailruParser
from .nevis import NevisParser
from .sozvezdie import SozvezdieParser
from .uteka import UtekaParser

from .ozonrfbs import OzonParser
from .sbermm import SbermmParser
from .yandexdbs import YandexdbsParser


marketplaces_map = {
    # standard integrations
    'aloe': AloeParser,
    'analitfarm': AnalitFarmParser,
    'artes': ArtesParser,
    'apteka_mos': AptekamosParser,
    'cva': CvaParser,
    'farmeconom': FarmeconomParser,
    'garmoniya': GarmoniyaParser,
    'okapteka': OkaptekaParser,
    'planetazd': PlanetazdParser,
    'vapteke': VaptekeParser,
    'zdravservis': ZdravserviceParser,

    # semi standard integrations
    'apteka36_6': Apteka366Parser,
    'aptekaforte': AptekaforteParser,
    'asnaru': AsnaruParser,
    'eapteka': EaptekaParser,
    'farmiya': FarmiyaParser,
    'mailru': MailruParser,
    'nevis': NevisParser,
    'sozvezdie': SozvezdieParser,
    'uteka': UtekaParser,

    # non standard integrations
    'ozonrfbs': OzonParser,
    'sbermm': SbermmParser,
    'yandexdbs': YandexdbsParser,
}


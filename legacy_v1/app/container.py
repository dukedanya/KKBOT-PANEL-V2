from dataclasses import dataclass

from config import Config
from db import Database
from services.health import HealthAlertState
from services.itpay import ItpayAPI
from services.panel import PanelAPI
from services.yookassa import YooKassaAPI
from services.telegram_stars import TelegramStarsAPI


@dataclass(slots=True)
class AppContainer:
    db: Database
    panel: PanelAPI
    payment_gateway: object
    health_alert_state: HealthAlertState



def build_container() -> AppContainer:
    if Config.PAYMENT_PROVIDER == "yookassa":
        payment_gateway = YooKassaAPI()
    elif Config.PAYMENT_PROVIDER == "telegram_stars":
        payment_gateway = TelegramStarsAPI()
    else:
        payment_gateway = ItpayAPI()
    return AppContainer(
        db=Database(Config.DATA_FILE),
        panel=PanelAPI(),
        payment_gateway=payment_gateway,
        health_alert_state=HealthAlertState(),
    )

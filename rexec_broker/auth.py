import logging
from typing import Optional

import requests


def validate_token(auth_api_url: str, token: str) -> Optional[str]:
    try:
        response = requests.post(
            auth_api_url,
            json={"token": token},
            timeout=10,
        )
    except requests.exceptions.RequestException as exc:
        logging.error("Auth request failed: %s", exc)
        return None

    if response.status_code != 200:
        logging.warning("Auth rejected token: status=%s", response.status_code)
        return None

    data = response.json()
    user_id = str(data.get("sub") or "").strip()
    if not user_id:
        logging.warning("Auth response missing user id")
        return None
    return user_id

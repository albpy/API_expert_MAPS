import bleach
from dateutil import parser
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


def cleanse_payload(payload):
    """
    Recursively cleanse the JSON payload.
    """

    # A list of keywords that are considered malicious for command injection
    CMD_BLACKLIST = ["&", ";", "|", "$", "`", ">", "<", "(", ")", "[", "]", "{", "}"]

    if isinstance(payload, dict):
        # If data type is a dictionary, cleanse each key-value pair
        return {k: cleanse_payload(v) for k, v in payload.items()}
    elif isinstance(payload, list):
        # If data type is a list, cleanse each item in the list
        return [cleanse_payload(item) for item in payload]
    elif isinstance(payload, str):
        # Cleanse for Command Injection
        for char in CMD_BLACKLIST:
            if char in payload:
                payload = payload.replace(char, "")

        # Cleanse for XSS using bleach
        payload = bleach.clean(payload)

        # Safely parse date strings (if you expect some fields to contain date strings)
        try:
            parsed_date = parser.parse(payload)
            return parsed_date.isoformat()
        except:
            # If it's not a date string, return the cleansed payload
            return payload

    else:
        return payload


class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # If request method has a payload, cleanse it
        if request.method in ("POST", "PUT", "PATCH"):
            try:
                body = await request.json()
                cleaned_body = cleanse_payload(body)
                request._body = cleaned_body  # Overwrite body with cleaned version
            except Exception as e:
                # If there's an error (like malformed JSON), respond with an error message
                return JSONResponse({"detail": "Invalid payload."}, status_code=400)

        response = await call_next(request)
        return response


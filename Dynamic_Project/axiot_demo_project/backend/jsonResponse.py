from flask import jsonify
from backend.common.commonUtility import get_sys_args, open_read_file_box


class ResponseCode:
    _cache = {}

    @classmethod
    def _load_codes(cls):
        if not cls._cache:
            filename = get_sys_args()
            file = open_read_file_box(filename + '_response')
            cls._cache = file
            for key, value in cls._cache.items():
                setattr(cls, key, cls(key, value['code'], value['message']))

    def __init__(self, name, code, message):
        self._name = name
        self._code = code
        self._message = message

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        return self._message

    @classmethod
    def create_response(cls, response_code_name, extra_data=None, extra_message=None):
        response_code = getattr(cls, response_code_name, None)
        if response_code is None:
            return jsonify({
                "code": 9999,
                "message": "Unknown error",
                "hasError": True
            }), 400

        # If extra_message is provided, use it to customize the message
        message = response_code.message
        if extra_message:
            message = extra_message

        response = {
            "code": response_code.code,
            "message": message,
            "hasError": response_code.code >= 2000
        }
        if extra_data:
            response.update(extra_data)
        return jsonify(response), 200


# Load response codes at class definition time
ResponseCode._load_codes()
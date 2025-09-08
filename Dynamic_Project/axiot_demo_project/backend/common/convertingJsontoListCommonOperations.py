"""
search_tenant_parent.py
==============

Description: Module to handle Tenant Common Opertaions

"""
"""
Function Name: check_type
Inputs:
- value

Output: returns type

Description:
    check_type
"""


def check_type(value):
    if isinstance(value, (list, tuple)):  # Checks if it's a list or tuple
        return "list"
    elif isinstance(value, str):
        return "string"
    else:
        return "other"


"""
Function Name: convert_into_in_compatible_string
Inputs:
- value

Output: String

Description:
    convert_into_in_compatible_string
"""


def convert_into_in_compatible_string(value):
    value_type = check_type(value)
    if value_type == "list":
        str_value = ", ".join("'{}'".format(name) for name in value) if value else ""
    # If it's a string that contains commas, split it into a list and then process
    elif isinstance(value, str) and "," in value:
        value_list = [x.strip() for x in value.split(",") if x.strip()]
        str_value = ", ".join("'{}'".format(name) for name in value_list) if value_list else ""
    else:
        str_value = "'{}'".format(value) if value else ""

    return str_value


"""
Function Name: convert_into_in_compatible_string_no_quotes
Inputs:
- value

Output: String

Description:
    convert_into_in_compatible_string_no_quotes
"""


def convert_into_in_compatible_string_no_quotes(value):
    value_type = check_type(value)
    if value_type == "list":
        str_value = ",".join("{}".format(name) for name in value) if value else ""
    else:
        str_value = "{}".format(value) if value else ""

    return str_value

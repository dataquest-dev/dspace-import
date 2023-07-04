import logging

from utils import read_json, convert_response_to_json, do_api_post


def import_user_registration(email2epersonId_dict,
                             eperson_id_dict,
                             userRegistration_id_dict,
                             statistics_dict):
    """
    Import data into database.
    Mapped tables: user_registration
    """
    user_reg_json_name = "user_registration.json"
    user_reg_url = 'clarin/import/userregistration'
    imported_user_reg = 0
    # read user_registration
    user_reg_json_a = read_json(user_reg_json_name)
    if not user_reg_json_a:
        logging.info("User_registration JSON is empty.")
        return
    for user_reg_json in user_reg_json_a:
        user_reg_json_p = {
            'email': user_reg_json['email'],
            'organization': user_reg_json['organization'],
            'confirmation': user_reg_json['confirmation']
        }
        if user_reg_json['email'] in email2epersonId_dict:
            user_reg_json_p['ePersonID'] = \
                eperson_id_dict[email2epersonId_dict[user_reg_json['email']]]
        else:
            user_reg_json_p['ePersonID'] = None
        try:
            response = do_api_post(user_reg_url, {}, user_reg_json_p)
            userRegistration_id_dict[user_reg_json['eperson_id']] = \
                convert_response_to_json(response)['id']
            imported_user_reg += 1
        except Exception as e:
            logging.error('POST request ' + user_reg_url + ' for id: ' +
                          str(user_reg_json['eperson_id']) +
                          ' failed. Exception: ' + str(e))

    statistics_val = (len(user_reg_json_a), imported_user_reg)
    statistics_dict['user_registration'] = statistics_val
    logging.info("User registration was successfully imported!")
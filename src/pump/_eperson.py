import logging
from ._utils import read_json, time_method, serialize, deserialize, progress_bar, log_before_import, log_after_import

_logger = logging.getLogger("pump.eperson")


def _emails(email):
    """
        The eperson email could consist of more email, return all of them in the array.
        If the email doesn't contain `;` that means there is only one email without `;` separator.
    """
    if email is None:
        return []

    if ';' not in email:
        return [email]

    # email value contains of two email, take just the first one.
    # e.g., test@msn.com;name@gmail.com
    return email.split(';')


class epersons:
    """
        Import data into database.
        Mapped tables: epersongroup2eperson
        SQL:
            delete from epersongroup2eperson ; delete from eperson where email NOT IN (SELECT email FROM eperson LIMIT 1) ;
            delete from group2groupcache ; delete from group2group ; delete from resourcepolicy ; delete from community2community ; delete from community ; delete from epersongroup where permanent=false;
    """
    TYPE = 7

    def __init__(self, eperson_file_str: str):
        self._epersons = read_json(eperson_file_str)
        self._imported = {
            "p": 0,
        }

        self._email2id = {}
        self._id2uuid = {}

        if len(self._epersons) == 0:
            _logger.info(f"Empty input: [{eperson_file_str}].")
            return

        # fill mapping email -> eperson_id
        for e in self._epersons:
            # eperson email could consist of more emails, add eperson_id into everyone
            for email in _emails(e['email']):
                self._email2id[email] = e['eperson_id']

    def __len__(self):
        return len(self._epersons)

    def by_email(self, email: str):
        return self._email2id.get(email, None)

    def uuid(self, eid: int):
        assert isinstance(list(self._id2uuid.keys())[0], str)
        return self._id2uuid.get(str(eid), None)

    @property
    def imported(self):
        return self._imported['p']

    @time_method
    def import_to(self, env, dspace, metadatas):
        expected = len(self)
        log_key = "eperson"
        log_before_import(log_key, expected)

        ignore_eids = env.get("ignore", {}).get("epersons", [])
        ignored = 0

        for e in progress_bar(self._epersons):
            e_id = e['eperson_id']

            if e_id in ignore_eids:
                _logger.debug(f"Skipping eperson [{e_id}]")
                ignored += 1
                continue

            data = {
                'selfRegistered': e['self_registered'],
                'requireCertificate': e['require_certificate'],
                'netid': e['netid'],
                'canLogIn': e['can_log_in'],
                'lastActive': e['last_active'],
                'email': e['email'],
                'password': e['password'],
                'welcomeInfo': e['welcome_info'],
                'canEditSubmissionMetadata': e['can_edit_submission_metadata']
            }

            e_meta = metadatas.value(epersons.TYPE, e_id)
            if e_meta:
                data['metadata'] = e_meta

            params = {
                'selfRegistered': e['self_registered'],
                'lastActive': e['last_active']
            }
            try:
                resp = dspace.put_eperson(params, data)
                self._id2uuid[str(e_id)] = resp['id']
                self._imported["p"] += 1
            except Exception as e:
                _logger.error(f'put_eperson: [{e_id}] failed [{str(e)}]')

        log_after_import(f"{log_key} ignored:[{ignored}]",
                         expected, self.imported + ignored)

    # =============

    def serialize(self, file_str: str):
        data = {
            "epersons": self._epersons,
            "id2uuid": self._id2uuid,
            "email2id": self._email2id,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        self._epersons = data["epersons"]
        self._id2uuid = data["id2uuid"]
        self._email2id = data["email2id"]
        self._imported = data["imported"]


# =============

class groups:
    """
        Mapped tables: epersongroup2eperson
    """

    def __init__(self, egroups_file_str: str):
        self._groups = read_json(egroups_file_str)
        self._imported = {
            "group": 0,
        }

        self._id2uuid = {}

        if len(self._groups) == 0:
            _logger.info(f"Empty input: [{egroups_file_str}].")
            return

    def __len__(self):
        return len(self._groups)

    @property
    def imported(self):
        return self._imported['group']

    @time_method
    def import_to(self, dspace, groups, epersons):
        _logger.info(f"Importing epersongroup2eperson [{len(self._groups)}]")

        for g in progress_bar(self._groups):
            g_id = g['eperson_group_id']
            e_id = g['eperson_id']
            try:
                g_uuid = groups.uuid(g_id)
                e_uuid = epersons.uuid(e_id)
                if g_uuid is None:
                    _logger.critical(f"Group UUID for [{g_id}] is None!")
                    continue
                if e_uuid is None:
                    _logger.critical(f"Eperson UUID for [{e_id}] is None!")
                    continue
                dspace.put_egroup(g_uuid, e_uuid)
                self._imported["group"] += 1
            except Exception as e:
                _logger.error(f'put_egroup: [{g_id}] failed [{str(e)}]')

        _logger.info(f"Epersongroup2eperson [{self.imported}] imported!")

    # =============

    def serialize(self, file_str: str):
        data = {
            "groups": self._groups,
            "id2uuid": self._id2uuid,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        self._groups = data["groups"]
        self._id2uuid = data["id2uuid"]
        self._imported = data["imported"]

    # =============

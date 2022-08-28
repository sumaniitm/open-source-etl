import datetime as dt
from include.scripts.utils.config import config


class Partitioner:

    def generate_s3_partition_suffix(self):
        suffix = ''
        for i in range(int(config.file_partition_levels)):
            param = config.get_file_partition_level(str(i+1))
            if param == 'month':
                suffix = "/".join([suffix, dt.datetime.now().strftime("%B").lower()])
            else:
                suffix = "/".join([suffix, str(getattr(dt.datetime.now(), param))])

        suffix = "".join([suffix, '/'])
        return suffix


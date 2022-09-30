import datetime as dt
from include.scripts.utils.config import config


class Partitioner:

    def generate_s3_partition_suffix(self, date_param=None):
        suffix = ''
        for i in range(int(config.file_partition_levels)):
            param = config.get_file_partition_level(str(i+1))
            print("date_param is : ",date_param)
            if date_param is not None:
                date_val = dt.datetime.strptime(date_param, '%Y-%m-%d')
                if param == 'month':
                    suffix = "/".join([suffix, date_val.strftime("%B").lower()])
                else:
                    suffix = "/".join([suffix, str(getattr(date_val, param))])
            else:
                if param == 'month':
                    suffix = "/".join([suffix, dt.datetime.now().strftime("%B").lower()])
                else:
                    suffix = "/".join([suffix, str(getattr(dt.datetime.now(), param))])

        suffix = "".join([suffix, '/'])
        return suffix
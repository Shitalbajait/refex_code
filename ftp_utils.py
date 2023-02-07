from ftplib import FTP
from datetime import datetime, timedelta
import json
import time


class FtpUtils:
    """
    Utils Class to save json and csv to FTP
    """

    def __init__(self, logger):
        """
        Initialize Utils Class
        """
        self.url = 'ftp.prescinto.com'
        self.uiserid = 'Refex_Test'
        self.passwd = 'Welcome@123'
        '''self.url1 = 'ftp.prescinto.com'
        self.uiserid1 = 'Refex_Test'
        self.passwd1 = 'Welcome@123'''
        self.logger = logger

    def save_json_to_ftp(self, json_data):
        """Save Json to FTP

        Args:
            json_data (json data): Json data received from the API

        Returns:
            boolean: True if file is saved to FTP, else False
        """
        try:
            network_id = json_data['network_id']
            packet_timestamp = json_data['packet_timestamp']
            json_file_name = f'json_data/{network_id}_{packet_timestamp}.json'
            outfile = open(json_file_name, "w")
            json.dump(json_data, outfile)
            outfile.close()
            packet_dt = str(
                (datetime.utcfromtimestamp(json_data['packet_timestamp']) + timedelta(minutes=330)).strftime(
                    '%Y-%m-%d %H:%M'))
            cnt = 0
            while cnt < 3:
                if self.save_file_to_ftp(json_file_name, "json_data"):
                    cnt = 3
                else:
                    cnt += 1
                    self.logger.info('Retrying after 1 sec')
                    time.sleep(1)
        except Exception as e:
            print(e)
            self.logger.error(f'ftp_util.save_json_to_ftp - {e}')
        finally:
            try:
                outfile.close()
                return True
            except Exception as e:
                print(e)
                return False
        return True

    '''def get_file_ftp(self, file_name, dir=""):
        ftp = FTP(self.url1)
        try:
            ftp.login(user=self.uiserid1, passwd=self.passwd1)
            if dir != "":
                ftp.cwd(dir)
            temp_file = str(datetime.now().timestamp()) + '.csv'
            with open(temp_file, 'wb') as fp:
                ftp.retrbinary('RETR ' + str(file_name), fp.write)
            return temp_file, True
        except Exception as e:
            self.logger.error(e)
        finally:
            ftp.quit()
        return None, False'''

    def save_file_to_ftp(self, file_name, dir=""):
        """Save File to FTP (Generic)

        Args:
            file_name (String): File Name

        Returns:
            Boolean: True if file is successfully saved to FTP
        """
        ftp = FTP(self.url)
        try:
            ftp.login(user=self.uiserid, passwd=self.passwd)
            if dir != "":
                try:
                    ftp.mkd(dir)
                except Exception as e:
                    pass
                ftp.cwd(dir)

            file_name_only = file_name.split('/')[-1]
            fp = open(file_name, 'rb')
            ftp.storbinary('STOR ' + str(file_name_only), fp)
            return True
        except Exception as e:
            self.logger.error(f'ftp_util.save_file_to_ftp - {file_name} - {e}')
        return False

    def save_csv_dir(self, file_name, destination_dir):
        """Save CSV to FTP

        Args:
            file_name (String): File to save to FTP
            :param file_name:
            :param destination_dir:
        """
        ftp = FTP(self.url1)
        ftp.login(user=self.uiserid, passwd=self.passwd)
        try:
            ftp.cwd(destination_dir)
            with open(file_name, 'rb') as fp:
                try:
                    ftp.storbinary('STOR ' + str(file_name), fp)
                except Exception as e:
                    self.logger.error(f'Error storing file - {file_name}')
                    self.logger.error(e)
        except Exception as e:
            self.logger.error(f'Error saving file to FTP - {file_name}')
            self.logger.error(e)
        ftp.quit()

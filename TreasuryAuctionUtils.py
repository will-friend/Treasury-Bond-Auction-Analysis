import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime
import urllib
import time

class AuctionDownload:

    _URL = 'https://www.treasurydirect.gov/xml/'

    def __init__(self, download_dir, start_date, end_date, security_type):

        self.download_dir = download_dir
        self.security_type = security_type
        self._start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self._end_date = datetime.strptime(end_date, "%Y-%m-%d")

        os.makedirs(self.download_dir, exist_ok=True)

    @property
    def url(self):
        return AuctionDownload._URL
    
    @property
    def start_date(self):
        return self._start_date
    
    @start_date.setter
    def start_date(self, start_date):
        self._start_date = datetime.strptime(start_date, "%Y-%m-%d")

    @property
    def end_date(self):
        return self._end_date
    
    @end_date.setter
    def end_date(self, end_date):
        self._end_date = datetime.strptime(end_date, "%Y-%m-%d")

    def _parse_date(self):
        self.start_year = datetime.strftime(self.start_date, "%Y")
        self.start_month = datetime.strftime(self.start_date, "%m")
        self.start_day = datetime.strftime(self.start_date, "%d")

        self.end_year = datetime.strftime(self.end_date, "%Y")
        self.end_month = datetime.strftime(self.end_date, "%m")
        self.end_day = datetime.strftime(self.end_date, "%d")

    def download(self):

        session = requests.Session()

        files = BeautifulSoup(
            session.get(AuctionDownload._URL).content, 
            features='html.parser'
        )

        link_lst = []
    
        # Iterate through the HTML code to find the link names, and append them to list if they are results for the year 2022
        for link in files.find('pre').find_all('a'):
            file_comp = link.attrs['href'].split('_')
            if len(file_comp) == 1:
                continue
            elif file_comp[0] != 'R':
                continue
            else:
                date = datetime.strptime(link.attrs['href'][2:10], '%Y%m%d')
                if date >= self._start_date and date <= self._end_date:
                    link_lst.append(link.attrs['href'])

        session.close()

        for file in link_lst:

            session = requests.Session()

            xml_link = self.url + file
            split = xml_link.split('/')[-1]
            
            link_table = BeautifulSoup(
                session.get(xml_link).content, 
                features='xml'
            )

            sec = str(link_table.find_all('SecurityType'))

            if self.security_type.lower() == "bond" and "BOND" in sec:
                os.makedirs(os.path.join(self.download_dir, 'Bonds'), exist_ok=True)
                urllib.request.urlretrieve(xml_link, os.path.join(self.download_dir, 'Bonds', 'Bond_'+split))

            if self.security_type.lower() == "bill" and "BILL" in sec:
                os.makedirs(os.path.join(self.download_dir, 'Bills'), exist_ok=True)
                urllib.request.urlretrieve(xml_link, os.path.join(self.download_dir, 'Bills', 'Bill_'+split))

            if self.security_type.lower() == "note" and "NOTE" in sec:
                os.makedirs(os.path.join(self.download_dir, 'Notes'), exist_ok=True)
                urllib.request.urlretrieve(xml_link, os.path.join(self.download_dir, 'Notes', 'Note_'+split))

            session.close()


import xml.etree.ElementTree as ET
import pandas as pd

class AuctionData:

    def __init__(self, data_dir, security_type):

        self.data_dir = data_dir
        self.security_type = security_type

    def auction_to_dataframe(self):

        if 'bond' in self.security_type.lower():
            file_path = os.path.join(self.data_dir, 'Bonds')
        
        if 'note' in self.security_type.lower():
            file_path = os.path.join(self.data_dir, 'Notes')
        
        if 'bill' in self.security_type.lower():
            file_path = os.path.join(self.data_dir, 'Bills')
        
        # List the dwnloaded files in given directory (either note or bill folder)
        files = os.listdir(file_path)

        # Iterate through files to parse and save off to a DataFrame
        for file in files:
            if self.security_type in file:
                # Get file as path
                xml_file = os.path.join(file_path, file)
                # Parse the xml data
                tree = ET.parse(xml_file)
                # Get the root of the xml data file
                root = tree.getroot()
                # Empty list to store data in
                data = []
                # Iterate through the elements of the data
                for element in root:
                    # Create empty Dictionary to store the data from the element in
                    row = {}
                    # Iterate through the subelements of the element and save data to the row dict
                    for subelement in element:
                        row[subelement.tag] = subelement.text
                    # Add the element's data to the data list
                    data.append(row)
                # Get the auction announcement data
                data_1 = [data[0]]
                # Get the auction results data
                data_2 = [data[-1]] 
                # Create base dataframe to be added to for both announcement and results data
                if file == files[0]:
                    df1 = pd.DataFrame(data_1)
                    df2 = pd.DataFrame(data_2)
                else:
                    # Create temporary dataframes to store the new file data in
                    df_temp1 = pd.DataFrame(data_1)
                    df_temp2 = pd.DataFrame(data_2)
                    # Add the announcement and result data from the new file into a new row in the base dataframe
                    df1 = pd.concat([df1, df_temp1], axis=0, ignore_index=True)
                    df2 = pd.concat([df2, df_temp2], axis=0, ignore_index=True)

        df2['AuctionDate'] = df1['AuctionDate']
        df2['SecurityTermWeekYear'] = df1['SecurityTermWeekYear']
        df2['SecurityTermDayMonth'] = df1['SecurityTermDayMonth']

        df = df2.dropna(axis=1)

        df = df.drop(['ResultsPDFName'], axis=1)

        df = df.sort_values(by=['AuctionDate'])

        # Remove but save the auction date
        auction_date = df.pop('AuctionDate')
        # Remove but save the auction release time
        term_week = df.pop('SecurityTermWeekYear')
        term_month = df.pop('SecurityTermDayMonth')

        # Add the auction date as the first column to the dataframe
        df.insert(0, 'AuctionDate', auction_date)
        # Add the auction release time as the second column of the dataframe
        df.insert(1, 'SecurityTermWeekYear', term_week)
        df.insert(2, 'SecurityTermDayMonth', term_month)

        df.reset_index(inplace=True)

        df = df.drop(["index"], axis=1)

        return df

downloader = AuctionDownload('./Data', "2023-01-01", "2023-12-31", "Bond")
downloader.download()

parser = AuctionData(os.path.join(os.getcwd(),'Data'), "Bond")
df = parser.auction_to_dataframe()

x = None









    
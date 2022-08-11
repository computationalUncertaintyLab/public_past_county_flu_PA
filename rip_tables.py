#mcandrew

import sys
import numpy as np
import pandas as pd

from tabula.io import read_pdf
from glob import glob

import re
import requests
from bs4 import BeautifulSoup

import camelot

if __name__ == "__main__":

    all_archive_data = pd.DataFrame()
    for n,f in enumerate(glob("*.pdf")):

        if n==0:
            counties = []
            tables = camelot.read_pdf(f, pages='1,2', flavor="stream")
            for table in tables:

                ind=0
                for county in table.df[0]:
                    if ind:
                        if county=="":
                            pass
                        else:
                            counties.append(county)
                    elif county=="County":
                        ind=1
            counties.append("total")
        tables = camelot.read_pdf(f, pages='1,2')

        if tables.n==0:
            continue
        
        d = pd.DataFrame()
        for table in tables:
            d = d.append(table.df)
        d = d[d[0]!=""]
            
        yrs = re.findall('\d+',f)
        season = "-".join(yrs)

        d["season"] = season
        d["county"] = counties
            
        all_archive_data = all_archive_data.append(d)

    all_archive_data = all_archive_data.loc[~all_archive_data.county.isna()]
    all_archive_data["county"] = all_archive_data.county.str.capitalize()

    all_archive_data = all_archive_data.rename(columns = {0:'A',1:"B",2:"U",3:"ttl"})
    
    all_archive_data = all_archive_data[ ['season','county','A',"B","U","ttl"] ]

    
    #---collect population estimates
    html = requests.get("https://www.pacodeandbulletin.gov/Display/pabull?file=/secure/pabulletin/data/vol38/38-35/1574.html")

    soup = BeautifulSoup(html.content)
    table = soup.find("table")

    # The first tr contains the field names.
    headings = [th.get_text() for th in table.find("tr").find_all("th")]

    pop_data = {"county":[], "pop":[]}
    for row in table.find_all("tr")[3:]:
        row_data = [td.get_text() for td in row.find_all("td")]
        if len(row_data) == 1:
            continue

        location = row_data[0]
        _2020    = row_data[3]

        pop_data["county"].append(location)
        pop_data["pop"].append(_2020)
    pop_data = pd.DataFrame(pop_data)

    pop_data["county"] = pop_data['county'].str.strip()
    pop_data["pop"] = pop_data['pop'].str.replace(",","").astype(int)

    all_archive_data = all_archive_data.merge(pop_data, on = ["county"], how = "left")

    all_archive_data.to_csv("all_archive_data.csv",index=False)
